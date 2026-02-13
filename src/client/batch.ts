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
import { v, type Validator } from "convex/values";
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

// ─── Executor loop (extracted for testability) ───────────────────────────────

type ClaimedTask = { _id: string; name: string; args: any; attempt: number };

type OnCompleteItem = {
  fnHandle: string;
  workId: string;
  context?: unknown;
  result:
    | { kind: "success"; returnValue: unknown }
    | { kind: "failed"; error: string }
    | { kind: "canceled" };
};

/** @internal Dependencies injected into the executor loop. Exported for testing. */
export interface _ExecutorDeps {
  listPending(limit: number): Promise<string[]>;
  claimByIds(taskIds: string[]): Promise<ClaimedTask[]>;
  completeBatch(
    items: { taskId: string; result: unknown }[],
  ): Promise<OnCompleteItem[]>;
  failBatch(
    items: { taskId: string; error: string }[],
  ): Promise<OnCompleteItem[]>;
  /** Dispatch multiple onComplete handlers in a single mutation transaction. */
  dispatchOnCompleteBatch(items: OnCompleteItem[]): Promise<void>;
  countPending(): Promise<number>;
  releaseClaims(taskIds: string[]): Promise<void>;
  executorDone(startMore: boolean): Promise<void>;
  getHandler(name: string): ((args: any) => Promise<any>) | undefined;
  getHeapUsedBytes(): number;
  isNode: boolean;
}

const FLUSH_BATCH_SIZE = 100;

function isTransientError(err: unknown): boolean {
  const msg = String(err);
  return (
    msg.includes("changed while this mutation was being run") ||
    msg.includes("Too many concurrent commits") ||
    msg.includes("no available workers") ||
    msg.includes("couldn't be completed") ||
    msg.includes("timed out")
  );
}

/** @internal Core executor loop. Exported for testing. */
export async function _runExecutorLoop(
  deps: _ExecutorDeps,
  options: BatchWorkpoolOptions,
): Promise<void> {
  const startTime = Date.now();
  const actionTimeout = 10 * 60 * 1000; // Convex action timeout: 10 min
  const softDeadline =
    startTime + actionTimeout - (options.softDeadlineMs ?? 30_000);
  const claimDeadline =
    startTime + (options.claimDeadlineMs ?? 5 * 60 * 1000);
  const maxConcurrency = options.maxConcurrencyPerWorker ?? 1000;
  const maxClaimBatch = 200;
  const pollInterval = options.pollIntervalMs ?? 500;
  const defaultHeapMB = deps.isNode ? 448 : 48;
  const maxHeapMB = options.maxHeapMB ?? defaultHeapMB;
  const maxHeapBytes = maxHeapMB * 1024 * 1024;
  const inFlight = new Map<string, Promise<void>>();

  // Completion/failure buffers — flushed as batch mutations to reduce
  // total mutation count from O(N) to O(N/batchSize).
  const completionBuffer: { taskId: string; result: unknown }[] = [];
  const failureBuffer: { taskId: string; error: string }[] = [];
  // OnComplete items returned from completeBatch/failBatch, dispatched
  // directly from the action for much higher throughput than scheduling.
  const onCompleteBuffer: OnCompleteItem[] = [];
  let onCompleteInflight = 0;
  let onCompleteDrainerRunning = false;

  // Dispatch onComplete items in batches of this size (single mutation each).
  // Each handler does ~2-3 DB ops, so 50 items = ~125 ops (well under 16K limit).
  // Kept moderate to avoid tying up Convex mutation workers too long.
  const ONCOMPLETE_BATCH_SIZE = 50;
  // Max concurrent batch mutations per executor: 5 × 20 workers = ~100 total
  const ONCOMPLETE_CONCURRENCY = 5;

  async function drainOnComplete() {
    if (onCompleteDrainerRunning) return;
    onCompleteDrainerRunning = true;
    let backoff = 50;
    try {
      while (onCompleteBuffer.length > 0) {
        const available = ONCOMPLETE_CONCURRENCY - onCompleteInflight;
        if (available <= 0) {
          await new Promise((r) => setTimeout(r, 20));
          continue;
        }
        // Launch up to `available` batch mutations in parallel
        const promises: Promise<void>[] = [];
        for (let i = 0; i < available && onCompleteBuffer.length > 0; i++) {
          const chunk = onCompleteBuffer.splice(0, ONCOMPLETE_BATCH_SIZE);
          onCompleteInflight++;
          promises.push(
            deps
              .dispatchOnCompleteBatch(chunk)
              .then(() => {
                backoff = 50;
              })
              .catch((err: unknown) => {
                if (isTransientError(err)) {
                  onCompleteBuffer.push(...chunk);
                } else {
                  console.error(`[batch] onComplete dispatch failed:`, err);
                }
              })
              .finally(() => {
                onCompleteInflight--;
              }),
          );
        }
        await Promise.race([
          Promise.all(promises),
          new Promise((r) => setTimeout(r, 100)),
        ]);
        if (onCompleteBuffer.length > 0 && backoff > 50) {
          await new Promise((r) =>
            setTimeout(r, backoff + Math.random() * backoff),
          );
        }
        if (onCompleteBuffer.length > 0 && promises.length === 0) {
          backoff = Math.min(backoff * 1.5, 2000);
        }
      }
    } finally {
      onCompleteDrainerRunning = false;
    }
  }

  /** Wait until all onComplete items have been dispatched. */
  async function awaitOnCompleteDrain() {
    while (onCompleteBuffer.length > 0 || onCompleteInflight > 0) {
      if (!onCompleteDrainerRunning && onCompleteBuffer.length > 0) {
        void drainOnComplete();
      }
      await new Promise((r) => setTimeout(r, 50));
    }
  }

  async function flushBuffers() {
    while (completionBuffer.length > 0) {
      const batch = completionBuffer.splice(0, FLUSH_BATCH_SIZE);
      try {
        const onComplete = await deps.completeBatch(batch);
        onCompleteBuffer.push(...onComplete);
      } catch (err: unknown) {
        if (isTransientError(err)) {
          // Put items back and bail — will retry next flush cycle
          completionBuffer.unshift(...batch);
          await new Promise((r) => setTimeout(r, 100 + Math.random() * 200));
          return;
        }
        console.error(`[batch] completeBatch failed for ${batch.length} items:`, err);
      }
    }
    while (failureBuffer.length > 0) {
      const batch = failureBuffer.splice(0, FLUSH_BATCH_SIZE);
      try {
        const onComplete = await deps.failBatch(batch);
        onCompleteBuffer.push(...onComplete);
      } catch (err: unknown) {
        if (isTransientError(err)) {
          failureBuffer.unshift(...batch);
          await new Promise((r) => setTimeout(r, 100 + Math.random() * 200));
          return;
        }
        console.error(`[batch] failBatch failed for ${batch.length} items:`, err);
      }
    }
    // Kick off background drain (non-blocking)
    void drainOnComplete();
  }

  try {
    while (Date.now() < softDeadline) {
      const canClaim = Date.now() < claimDeadline;

      // Flush buffered completions/failures before claiming more work
      await flushBuffers();

      // Check memory pressure — skip claiming if heap is too large
      let memoryOk = true;
      if (maxHeapMB > 0 && deps.isNode) {
        const heapUsed = deps.getHeapUsedBytes();
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
          let batch: ClaimedTask[];
          try {
            // Two-step claim: query for IDs (no OCC), then claim by point reads
            const ids = await deps.listPending(available);
            batch = ids.length > 0 ? await deps.claimByIds(ids) : [];
          } catch (err: unknown) {
            if (isTransientError(err)) {
              await new Promise((r) => setTimeout(r, 100 + Math.random() * 400));
              continue;
            }
            throw err;
          }

          for (const task of batch) {
            const handler = deps.getHandler(task.name);
            if (!handler) {
              failureBuffer.push({ taskId: task._id, error: `Unknown handler: ${task.name}` });
              continue;
            }

            const p = handler(task.args)
              .then((result) => {
                completionBuffer.push({ taskId: task._id, result: result ?? null });
              })
              .catch((err: unknown) => {
                failureBuffer.push({ taskId: task._id, error: String(err) });
              })
              .finally(() => inFlight.delete(task._id));

            inFlight.set(task._id, p);
          }
        }
      }

      // Nothing in flight and all buffers empty — check if we should exit
      if (
        inFlight.size === 0 &&
        completionBuffer.length === 0 &&
        failureBuffer.length === 0 &&
        onCompleteBuffer.length === 0 &&
        onCompleteInflight === 0
      ) {
        // Past claim deadline with nothing in flight: we're done draining
        if (!canClaim) break;
        let pending: number;
        try {
          pending = await deps.countPending();
        } catch {
          // If countPending fails (timeout, etc.), assume work remains
          pending = 1;
        }
        if (pending === 0) break;
        await new Promise((r) => setTimeout(r, pollInterval));
        continue;
      }

      // Wait for at least one task to finish, a flush interval, or soft deadline
      const timeToDeadline = softDeadline - Date.now();
      if (timeToDeadline <= 0) break;
      const deadlineTimer = new Promise<void>((r) =>
        setTimeout(r, Math.min(timeToDeadline, 200)),
      );
      if (inFlight.size > 0) {
        await Promise.race([...inFlight.values(), deadlineTimer]);
      } else {
        await deadlineTimer;
      }
    }
  } finally {
    // Final flush of any remaining buffered results
    await flushBuffers();
    // Wait for all onComplete dispatches to finish
    await awaitOnCompleteDrain();
    // Release any tasks still in "claimed" state
    const unfinished = [...inFlight.keys()];
    if (unfinished.length > 0) {
      try {
        await deps.releaseClaims(unfinished);
      } catch (err: unknown) {
        console.error(`[batch] failed to release ${unfinished.length} claims:`, err);
      }
    }
  }

  // Check if there's remaining work — treat failures as "yes" to be safe
  let remaining: number;
  try {
    remaining = await deps.countPending();
  } catch {
    remaining = 1;
  }

  // Notify component that this executor is done — retry on transient errors
  // since all executors write to the same batchConfig singleton.
  for (let attempt = 0; attempt < 5; attempt++) {
    try {
      await deps.executorDone(remaining > 0);
      break;
    } catch (err: unknown) {
      if (attempt === 4 || !isTransientError(err)) throw err;
      await new Promise((r) => setTimeout(r, 100 + Math.random() * 400));
    }
  }
}

// ─── Class ──────────────────────────────────────────────────────────────────

export class BatchWorkpool {
  public component: ComponentApi;
  private options: BatchWorkpoolOptions;
  private registry = new Map<string, HandlerFn>();
  private executorFnRef: FunctionReference<
    "action",
    "internal"
  > | null = null;
  private cachedBatchConfig:
    | { executorHandle: string; maxWorkers: number; claimTimeoutMs: number }
    | undefined;
  private configSentThisTx = false;

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
  executor(): RegisteredAction<"internal", { slot: number }, void> {
    const component = this.component;
    const registry = this.registry;
    const options = this.options;
    return internalActionGeneric({
      args: { slot: v.number() },
      handler: async (ctx: GenericActionCtx<any>, { slot }: { slot: number }) => {
        const isNode =
          typeof process !== "undefined" &&
          typeof process.memoryUsage === "function";
        const deps: _ExecutorDeps = {
          listPending: (limit) =>
            ctx.runQuery(component.batch.listPending, { slot, limit }),
          claimByIds: (taskIds) =>
            ctx.runMutation(component.batch.claimByIds, { taskIds }),
          completeBatch: (items) =>
            ctx.runMutation(component.batch.completeBatch, { items }),
          failBatch: (items) =>
            ctx.runMutation(component.batch.failBatch, { items }),
          dispatchOnCompleteBatch: (items) =>
            ctx.runMutation(component.batch.dispatchOnCompleteBatch, {
              items,
            }),
          countPending: () =>
            ctx.runQuery(component.batch.countPending, {}),
          releaseClaims: (taskIds) =>
            ctx.runMutation(component.batch.releaseClaims, { taskIds }),
          executorDone: (startMore) =>
            ctx.runMutation(component.batch.executorDone, { startMore, slot }),
          getHandler: (name) => {
            const h = registry.get(name);
            return h ? (args) => h(ctx, args) : undefined;
          },
          getHeapUsedBytes: () =>
            isNode ? process.memoryUsage().heapUsed : 0,
          isNode,
        };
        await _runExecutorLoop(deps, options);
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
    // Only pass batchConfig on first enqueue per mutation to avoid
    // OCC contention on the batchConfig singleton.
    const batchConfig = this.configSentThisTx
      ? undefined
      : await this.getBatchConfig();
    const onComplete = options?.onComplete
      ? {
          fnHandle: await createFunctionHandle(options.onComplete),
          context: options.context,
        }
      : undefined;
    const retryBehavior = this.getRetryBehavior(options?.retry);
    const maxWorkers = this.options.maxWorkers ?? 10;
    const slot = Math.floor(Math.random() * maxWorkers);
    const id = await ctx.runMutation(this.component.batch.enqueue, {
      name,
      args,
      slot,
      onComplete,
      retryBehavior,
      batchConfig,
    });
    if (batchConfig) this.configSentThisTx = true;
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
    const batchConfig = this.configSentThisTx
      ? undefined
      : await this.getBatchConfig();
    const maxWorkers = this.options.maxWorkers ?? 10;
    const resolvedTasks = await Promise.all(
      tasks.map(async (task) => ({
        name: task.name,
        args: task.args,
        slot: Math.floor(Math.random() * maxWorkers),
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
    if (batchConfig) this.configSentThisTx = true;
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
    const maxWorkers = this.options.maxWorkers ?? 10;
    const slot = Math.floor(Math.random() * maxWorkers);
    const id = await ctx.runMutation(this.component.batch.enqueue, {
      name,
      args,
      slot,
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
    if (!this.cachedBatchConfig) {
      const executorHandle = await createFunctionHandle(this.executorFnRef);
      this.cachedBatchConfig = {
        executorHandle,
        maxWorkers: this.options.maxWorkers ?? 10,
        claimTimeoutMs: this.options.claimTimeoutMs ?? 120_000,
      };
    }
    return this.cachedBatchConfig;
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
