/**
 * BatchWorkpool: a client-side class for batch execution mode.
 *
 * Instead of 1 action per task, a long-lived "executor" action runs many
 * handlers concurrently via Promise.all inside a single action. Each handler
 * is a plain TypeScript async function that calls ctx.runQuery/ctx.runMutation
 * for durable reads/writes and fetch() for external API calls.
 *
 * Usage:
 * ```ts
 * // convex/batch.ts
 * import { BatchWorkpool } from "@convex-dev/workpool";
 * import { components, internal } from "./_generated/api";
 *
 * export const batch = new BatchWorkpool(components.workpool, {
 *   maxWorkers: 10,
 *   maxConcurrencyPerWorker: 200,
 * });
 *
 * export const executor = batch.executor();
 * batch.setExecutorRef(internal.batch.executor);
 * ```
 *
 * ```ts
 * // convex/llm.ts
 * import { batch } from "./batch";
 *
 * export const generateBio = batch.action("generateBio", {
 *   args: { userId: v.id("users") },
 *   handler: async (ctx, args) => {
 *     // ... same handler code as before
 *   },
 * });
 * ```
 */
import {
  createFunctionHandle,
  type DefaultFunctionArgs,
  type FunctionReference,
  type FunctionVisibility,
  type GenericActionCtx,
  type GenericDataModel,
  internalActionGeneric,
  type RegisteredAction,
} from "convex/server";
import type { Validator } from "convex/values";
import type { ComponentApi } from "../component/_generated/component.js";
import type { RetryBehavior } from "../component/shared.js";
import type { RunMutationCtx, RunQueryCtx } from "./utils.js";
import type { OnCompleteArgs } from "./index.js";

// ─── Types ──────────────────────────────────────────────────────────────────

export type BatchTaskId = string & { __isBatchTaskId: true };

export type BatchWorkpoolOptions = {
  /**
   * How many executor actions to run concurrently. Each executor is one
   * Convex action slot. Default: 10.
   */
  maxWorkers?: number;
  /**
   * How many task handlers to run concurrently inside each executor.
   * Default: 1000. Memory-based back-pressure (maxHeapMB) provides
   * a safety net if tasks hold large data in memory.
   */
  maxConcurrencyPerWorker?: number;
  /**
   * Stop claiming new work after this many ms from executor start.
   * In-flight tasks continue running after this point.
   * Default: 300000 (5 min).
   */
  claimDeadlineMs?: number;
  /**
   * Hard shutdown deadline: release all remaining claims and exit.
   * Expressed as a buffer before the 10-min action timeout.
   * Default: 30000 (30s), meaning shutdown at 9.5 min.
   */
  softDeadlineMs?: number;
  /**
   * Sweep claims older than this back to pending. Handles executor crashes.
   * Default: 120000 (2 min).
   */
  claimTimeoutMs?: number;
  /**
   * Default retry behavior for failed tasks.
   */
  defaultRetryBehavior?: RetryBehavior;
  /**
   * Whether to retry actions by default. Default: false.
   */
  retryActionsByDefault?: boolean;
  /**
   * Polling interval in ms when the executor has no work but pending
   * tasks exist (possibly with future readyAt). Default: 500.
   */
  pollIntervalMs?: number;
  /**
   * Stop claiming new tasks when heap usage exceeds this many MB.
   * In-flight tasks continue running; claiming resumes when memory drops.
   * Default: 448 MB in Node.js (512 MB limit), 48 MB in Convex runtime (64 MB limit).
   * Set to 0 to disable memory-based back-pressure.
   */
  maxHeapMB?: number;
};

export type BatchTaskStatus =
  | { state: "pending"; attempt: number }
  | { state: "running"; attempt: number }
  | { state: "finished" };

export type BatchEnqueueOptions = {
  onComplete?: FunctionReference<
    "mutation",
    FunctionVisibility,
    OnCompleteArgs
  > | null;
  context?: unknown;
  retry?: boolean | RetryBehavior;
};

type HandlerFn = (ctx: GenericActionCtx<any>, args: any) => Promise<any>;

// ─── Class ──────────────────────────────────────────────────────────────────

export class BatchWorkpool {
  public component: ComponentApi;
  private options: BatchWorkpoolOptions;
  private registry = new Map<string, HandlerFn>();
  private executorFnRef: FunctionReference<
    "action",
    "internal"
  > | null = null;

  constructor(component: ComponentApi, options?: BatchWorkpoolOptions) {
    this.component = component;
    this.options = options ?? {};
  }

  /**
   * Register a handler AND return a real internalAction so the
   * function reference resolves for workflow's step.runAction().
   *
   * ```ts
   * export const generateBio = batch.action("generateBio", {
   *   args: { userId: v.id("users") },
   *   handler: async (ctx, args) => {
   *     // handler code ...
   *   },
   * });
   * ```
   */
  action<
    DataModel extends GenericDataModel,
    Args extends DefaultFunctionArgs = any,
    Returns = any,
  >(
    name: string,
    opts: {
      args: Record<string, Validator<any, any, any>>;
      handler: (
        ctx: GenericActionCtx<DataModel>,
        args: Args,
      ) => Promise<Returns>;
    },
  ): RegisteredAction<"internal", Args, Returns> {
    this.registry.set(name, opts.handler as HandlerFn);
    return internalActionGeneric({
      args: opts.args as any,
      handler: opts.handler as any,
    });
  }

  /**
   * Generate the executor action. The user must export this from their
   * convex/ directory:
   *
   * ```ts
   * export const executor = batch.executor();
   * batch.setExecutorRef(internal.batch.executor);
   * ```
   */
  executor(): RegisteredAction<"internal", Record<string, never>, void> {
    const component = this.component;
    const registry = this.registry;
    const options = this.options;
    return internalActionGeneric({
      args: {},
      handler: async (ctx: GenericActionCtx<any>) => {
        const startTime = Date.now();
        const actionTimeout = 10 * 60 * 1000; // Convex action timeout: 10 min
        const softDeadline =
          startTime +
          actionTimeout -
          (options.softDeadlineMs ?? 30_000);
        const claimDeadline =
          startTime + (options.claimDeadlineMs ?? 5 * 60 * 1000);
        const maxConcurrency =
          options.maxConcurrencyPerWorker ?? 1000;
        // Cap how many tasks we claim per mutation to avoid oversized transactions.
        const maxClaimBatch = 200;
        const pollInterval = options.pollIntervalMs ?? 500;
        // Detect runtime: Node.js has process.memoryUsage, Convex runtime doesn't
        const isNode =
          typeof process !== "undefined" &&
          typeof process.memoryUsage === "function";
        const defaultHeapMB = isNode ? 448 : 48;
        const maxHeapMB = options.maxHeapMB ?? defaultHeapMB;
        const maxHeapBytes = maxHeapMB * 1024 * 1024;
        const inFlight = new Map<string, Promise<void>>();

        try {
          while (Date.now() < softDeadline) {
            const canClaim = Date.now() < claimDeadline;

            // Check memory pressure — skip claiming if heap is too large
            let memoryOk = true;
            if (maxHeapMB > 0 && isNode) {
              const heapUsed = process.memoryUsage().heapUsed;
              memoryOk = heapUsed < maxHeapBytes;
              if (!memoryOk && canClaim && inFlight.size > 0) {
                const heapMB = (heapUsed / (1024 * 1024)).toFixed(1);
                console.log(
                  `[batch] memory pressure: ${heapMB} MB heap, ` +
                    `${inFlight.size} in flight — waiting for tasks to complete`,
                );
              }
            }

            // Fill concurrency slots (only before claim deadline and under memory limit)
            if (canClaim && memoryOk) {
              const available = Math.min(
                maxConcurrency - inFlight.size,
                maxClaimBatch,
              );
              if (available > 0) {
                const batch: Array<{
                  _id: string;
                  name: string;
                  args: any;
                  attempt: number;
                }> = await ctx.runMutation(component.batch.claimBatch, {
                  limit: available,
                });

                for (const task of batch) {
                  const handler = registry.get(task.name);
                  if (!handler) {
                    await ctx.runMutation(component.batch.fail, {
                      taskId: task._id,
                      error: `Unknown handler: ${task.name}`,
                    });
                    continue;
                  }

                  const p = handler(ctx, task.args)
                    .then(async (result) => {
                      await ctx.runMutation(component.batch.complete, {
                        taskId: task._id,
                        result: result ?? null,
                      });
                    })
                    .catch(async (err: unknown) => {
                      try {
                        await ctx.runMutation(component.batch.fail, {
                          taskId: task._id,
                          error: String(err),
                        });
                      } catch (failErr) {
                        console.error(
                          `[batch] failed to report failure for ${task._id}:`,
                          failErr,
                        );
                      }
                    })
                    .finally(() => inFlight.delete(task._id));

                  inFlight.set(task._id, p);
                }
              }
            }

            // Nothing in flight — check if we should exit
            if (inFlight.size === 0) {
              // Past claim deadline with nothing in flight: we're done draining
              if (!canClaim) break;
              const pending: number = await ctx.runQuery(
                component.batch.countPending,
                {},
              );
              if (pending === 0) break;
              await new Promise((r) => setTimeout(r, pollInterval));
              continue;
            }

            // Wait for at least one task to finish or soft deadline, whichever comes first
            const timeToDeadline = softDeadline - Date.now();
            if (timeToDeadline <= 0) break;
            const deadlineTimer = new Promise<void>((r) =>
              setTimeout(r, timeToDeadline),
            );
            await Promise.race([...inFlight.values(), deadlineTimer]);
          }
        } finally {
          // Guarantee claim release even if the executor crashes.
          // Released tasks return to "pending" for another executor to pick up.
          const unfinished = [...inFlight.keys()];
          if (unfinished.length > 0) {
            await ctx.runMutation(component.batch.releaseClaims, {
              taskIds: unfinished,
            });
          }
        }

        // Check if there's remaining work
        const remaining: number = await ctx.runQuery(
          component.batch.countPending,
          {},
        );

        // Notify component that this executor is done.
        // If there's remaining work, executorDone calls ensureExecutors
        // to schedule a replacement — no self-scheduling needed.
        await ctx.runMutation(component.batch.executorDone, {
          startMore: remaining > 0,
        });
      },
    });
  }

  /**
   * Set the executor function reference for self-scheduling.
   * Must be called after `executor()`:
   *
   * ```ts
   * export const executor = batch.executor();
   * batch.setExecutorRef(internal.batch.executor);
   * ```
   */
  setExecutorRef(
    ref: FunctionReference<"action", "internal">,
  ): void {
    this.executorFnRef = ref;
  }

  /**
   * Enqueue a task to be picked up by an executor.
   * Call from mutations or actions.
   */
  async enqueue(
    ctx: RunMutationCtx,
    name: string,
    args: DefaultFunctionArgs,
    options?: BatchEnqueueOptions,
  ): Promise<BatchTaskId> {
    const batchConfig = await this.getBatchConfig();
    const onComplete = options?.onComplete
      ? {
          fnHandle: await createFunctionHandle(options.onComplete),
          context: options.context,
        }
      : undefined;
    const retryBehavior = this.getRetryBehavior(options?.retry);
    const id = await ctx.runMutation(this.component.batch.enqueue, {
      name,
      args,
      onComplete,
      retryBehavior,
      batchConfig,
    });
    return id as unknown as BatchTaskId;
  }

  /**
   * Enqueue multiple tasks in a single mutation.
   */
  async enqueueBatch(
    ctx: RunMutationCtx,
    tasks: Array<{
      name: string;
      args: DefaultFunctionArgs;
      options?: BatchEnqueueOptions;
    }>,
  ): Promise<BatchTaskId[]> {
    const batchConfig = await this.getBatchConfig();
    const resolvedTasks = await Promise.all(
      tasks.map(async (task) => ({
        name: task.name,
        args: task.args,
        onComplete: task.options?.onComplete
          ? {
              fnHandle: await createFunctionHandle(task.options.onComplete),
              context: task.options.context,
            }
          : undefined,
        retryBehavior: this.getRetryBehavior(task.options?.retry),
      })),
    );
    const ids = await ctx.runMutation(this.component.batch.enqueueBatch, {
      tasks: resolvedTasks,
      batchConfig,
    });
    return ids as unknown as BatchTaskId[];
  }

  /**
   * Get the status of a batch task.
   */
  async status(ctx: RunQueryCtx, id: BatchTaskId): Promise<BatchTaskStatus> {
    return ctx.runQuery(this.component.batch.status, {
      taskId: id as unknown as string,
    });
  }

  /**
   * Cancel a batch task. If it's currently being executed, the executor
   * will find it gone when it tries to report the result.
   */
  async cancel(ctx: RunMutationCtx, id: BatchTaskId): Promise<void> {
    await ctx.runMutation(this.component.batch.cancel, {
      taskId: id as unknown as string,
    });
  }

  /**
   * Check if a function name (from `safeFunctionName()`) matches a
   * registered batch handler. Tries direct match first, then suffix
   * match after `:` or `/`.
   */
  isRegistered(fnName: string): boolean {
    return this.resolveHandlerName(fnName) !== undefined;
  }

  /**
   * Same logic as `isRegistered` but returns the handler name, or
   * `undefined` if no match.
   */
  resolveHandlerName(fnName: string): string | undefined {
    if (this.registry.has(fnName)) return fnName;
    // Try suffix after last `:` or `/`
    const colonIdx = fnName.lastIndexOf(":");
    if (colonIdx !== -1) {
      const suffix = fnName.slice(colonIdx + 1);
      if (this.registry.has(suffix)) return suffix;
    }
    const slashIdx = fnName.lastIndexOf("/");
    if (slashIdx !== -1) {
      const suffix = fnName.slice(slashIdx + 1);
      if (this.registry.has(suffix)) return suffix;
    }
    return undefined;
  }

  /**
   * Like `enqueue()` but takes a pre-computed onComplete handle string
   * instead of a FunctionReference. Used by workflow integration where
   * the component returns the onComplete as a function handle string.
   */
  async enqueueByHandle(
    ctx: RunMutationCtx,
    name: string,
    args: DefaultFunctionArgs,
    options?: {
      onComplete?: { fnHandle: string; context?: unknown };
      retry?: boolean | RetryBehavior;
    },
  ): Promise<BatchTaskId> {
    const batchConfig = await this.getBatchConfig();
    const retryBehavior = this.getRetryBehavior(options?.retry);
    const id = await ctx.runMutation(this.component.batch.enqueue, {
      name,
      args,
      onComplete: options?.onComplete,
      retryBehavior,
      batchConfig,
    });
    return id as unknown as BatchTaskId;
  }

  // ─── Private helpers ────────────────────────────────────────────────────

  private async getBatchConfig() {
    // If executorFnRef isn't set, return undefined. This is valid for
    // subsequent enqueues (e.g. from onComplete callbacks in pipeline modules)
    // where the component already has the batchConfig in the DB from the
    // initial enqueue. The component's ensureExecutors reads from the DB.
    if (!this.executorFnRef) return undefined;
    const executorHandle = await createFunctionHandle(this.executorFnRef);
    return {
      executorHandle,
      maxWorkers: this.options.maxWorkers ?? 10,
      claimTimeoutMs: this.options.claimTimeoutMs ?? 120_000,
    };
  }

  private getRetryBehavior(
    retry: boolean | RetryBehavior | undefined,
  ): RetryBehavior | undefined {
    const defaultBehavior: RetryBehavior = this.options.defaultRetryBehavior ?? {
      maxAttempts: 5,
      initialBackoffMs: 250,
      base: 2,
    };
    if (retry === true) return defaultBehavior;
    if (retry === false) return undefined;
    if (typeof retry === "object") return retry;
    return this.options.retryActionsByDefault ? defaultBehavior : undefined;
  }
}
