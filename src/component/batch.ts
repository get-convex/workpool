/**
 * Batch module: component-side mutations and queries for the batch execution mode.
 *
 * The batch module maintains a `batchTasks` table of pending/claimed/completed tasks.
 * Executor actions (running in user space) call these mutations across the
 * component boundary to claim work, report results, and manage the queue.
 */
import type { FunctionHandle } from "convex/server";
import { v } from "convex/values";
import type { Id } from "./_generated/dataModel.js";
import { mutation, type MutationCtx, query } from "./_generated/server.js";
import {
  type OnCompleteArgs,
  retryBehavior,
  type RunResult,
  vOnCompleteFnContext,
} from "./shared.js";
import { withJitter } from "./loop.js";

// ─── Validators ─────────────────────────────────────────────────────────────

const batchConfigArgs = v.object({
  executorHandle: v.string(),
  maxWorkers: v.number(),
  claimTimeoutMs: v.number(),
});

// ─── Configure ──────────────────────────────────────────────────────────────

/**
 * Upsert batch configuration. Called lazily on first enqueue.
 */
export const configure = mutation({
  args: batchConfigArgs,
  handler: async (ctx, args) => {
    await upsertBatchConfig(ctx, args);
  },
});

// ─── Enqueue ────────────────────────────────────────────────────────────────

export const enqueue = mutation({
  args: {
    name: v.string(),
    args: v.any(),
    onComplete: v.optional(vOnCompleteFnContext),
    retryBehavior: v.optional(retryBehavior),
    // Batch config passed on every enqueue for lazy init
    batchConfig: v.optional(batchConfigArgs),
  },
  returns: v.id("batchTasks"),
  handler: async (ctx, args) => {
    // Lazy-init batch config
    await upsertBatchConfig(ctx, args.batchConfig);

    const taskId = await ctx.db.insert("batchTasks", {
      name: args.name,
      args: args.args,
      status: "pending",
      readyAt: Date.now(),
      attempt: 0,
      onComplete: args.onComplete,
      retryBehavior: args.retryBehavior,
    });

    // Only start executors when batchConfig is provided (initial enqueue).
    // Chained enqueues from onComplete callbacks don't pass batchConfig and
    // don't need this — the executor is already running and will pick up
    // new tasks. Skipping avoids OCC conflicts with claimBatch/executorDone.
    if (args.batchConfig) {
      await ensureExecutors(ctx);
    }
    return taskId;
  },
});

export const enqueueBatch = mutation({
  args: {
    tasks: v.array(
      v.object({
        name: v.string(),
        args: v.any(),
        onComplete: v.optional(vOnCompleteFnContext),
        retryBehavior: v.optional(retryBehavior),
      }),
    ),
    batchConfig: v.optional(batchConfigArgs),
  },
  returns: v.array(v.id("batchTasks")),
  handler: async (ctx, args) => {
    await upsertBatchConfig(ctx, args.batchConfig);

    const ids = await Promise.all(
      args.tasks.map((task) =>
        ctx.db.insert("batchTasks", {
          name: task.name,
          args: task.args,
          status: "pending",
          readyAt: Date.now(),
          attempt: 0,
          onComplete: task.onComplete,
          retryBehavior: task.retryBehavior,
        }),
      ),
    );

    if (args.batchConfig) {
      await ensureExecutors(ctx);
    }
    return ids;
  },
});

// ─── Claim ──────────────────────────────────────────────────────────────────

export const claimBatch = mutation({
  args: { limit: v.number() },
  returns: v.array(
    v.object({
      _id: v.id("batchTasks"),
      name: v.string(),
      args: v.any(),
      attempt: v.number(),
    }),
  ),
  handler: async (ctx, { limit }) => {
    const now = Date.now();
    const pending = await ctx.db
      .query("batchTasks")
      .withIndex("by_status_readyAt", (q) =>
        q.eq("status", "pending").lte("readyAt", now),
      )
      .take(limit);

    const claimed = [];
    for (const task of pending) {
      await ctx.db.patch(task._id, {
        status: "claimed",
        claimedAt: now,
      });
      claimed.push({
        _id: task._id,
        name: task.name,
        args: task.args,
        attempt: task.attempt,
      });
    }
    return claimed;
  },
});

// ─── Complete ───────────────────────────────────────────────────────────────

export const complete = mutation({
  args: {
    taskId: v.id("batchTasks"),
    result: v.any(),
  },
  handler: async (ctx, { taskId, result }) => {
    const task = await ctx.db.get(taskId);
    if (!task) {
      // Task was canceled or already completed
      return;
    }
    if (task.status !== "claimed") {
      // Task is not in expected state (e.g. was canceled)
      return;
    }

    // Call onComplete with success result
    if (task.onComplete) {
      const runResult: RunResult = { kind: "success", returnValue: result };
      await callOnComplete(ctx, taskId, task.onComplete, runResult);
    }

    // Delete the task (terminal state)
    await ctx.db.delete(taskId);
  },
});

// ─── Fail ───────────────────────────────────────────────────────────────────

export const fail = mutation({
  args: {
    taskId: v.id("batchTasks"),
    error: v.string(),
  },
  handler: async (ctx, { taskId, error }) => {
    const task = await ctx.db.get(taskId);
    if (!task) {
      // Task was canceled or already completed
      return;
    }
    if (task.status !== "claimed") {
      // Task is not in expected state
      return;
    }

    const maxAttempts = task.retryBehavior?.maxAttempts;
    const nextAttempt = task.attempt + 1;
    const shouldRetry = !!maxAttempts && nextAttempt < maxAttempts;

    if (shouldRetry) {
      // Retry with exponential backoff + jitter
      const backoffMs =
        task.retryBehavior!.initialBackoffMs *
        Math.pow(task.retryBehavior!.base, nextAttempt - 1);
      const delayMs = withJitter(backoffMs);

      await ctx.db.patch(taskId, {
        status: "pending",
        attempt: nextAttempt,
        claimedAt: undefined,
        readyAt: Date.now() + delayMs,
        error,
      });
    } else {
      // Retries exhausted (or no retry config). Terminal failure.
      if (task.onComplete) {
        const runResult: RunResult = { kind: "failed", error };
        await callOnComplete(ctx, taskId, task.onComplete, runResult);
      }
      await ctx.db.delete(taskId);
    }
  },
});

// ─── Count pending ──────────────────────────────────────────────────────────

export const countPending = query({
  args: {},
  returns: v.number(),
  handler: async (ctx) => {
    const pending = await ctx.db
      .query("batchTasks")
      .withIndex("by_status_readyAt", (q) => q.eq("status", "pending"))
      .collect();
    return pending.length;
  },
});

// ─── Status ─────────────────────────────────────────────────────────────────

const batchTaskStatus = v.union(
  v.object({
    state: v.literal("pending"),
    attempt: v.number(),
  }),
  v.object({
    state: v.literal("running"),
    attempt: v.number(),
  }),
  v.object({
    state: v.literal("finished"),
  }),
);

export const status = query({
  args: { taskId: v.id("batchTasks") },
  returns: batchTaskStatus,
  handler: async (ctx, { taskId }) => {
    const task = await ctx.db.get(taskId);
    if (!task) {
      return { state: "finished" as const };
    }
    switch (task.status) {
      case "pending":
        return { state: "pending" as const, attempt: task.attempt };
      case "claimed":
        return { state: "running" as const, attempt: task.attempt };
      case "completed":
      case "failed":
      case "canceled":
        return { state: "finished" as const };
    }
  },
});

// ─── Cancel ─────────────────────────────────────────────────────────────────

export const cancel = mutation({
  args: { taskId: v.id("batchTasks") },
  handler: async (ctx, { taskId }) => {
    const task = await ctx.db.get(taskId);
    if (!task) {
      return;
    }

    // Call onComplete with canceled result
    if (task.onComplete) {
      const runResult: RunResult = { kind: "canceled" };
      await callOnComplete(ctx, taskId, task.onComplete, runResult);
    }

    await ctx.db.delete(taskId);
  },
});

// ─── Release claims ──────────────────────────────────────────────────────────

/**
 * Release claims on specific tasks, returning them to "pending".
 * Called by the executor at its soft deadline for any in-flight tasks
 * that haven't completed yet, so they can be picked up immediately
 * by another executor instead of waiting for the stale claim sweep.
 */
export const releaseClaims = mutation({
  args: { taskIds: v.array(v.id("batchTasks")) },
  handler: async (ctx, { taskIds }) => {
    for (const taskId of taskIds) {
      const task = await ctx.db.get(taskId);
      // Only release if it's still claimed — it may have completed
      // between the executor deciding to release and this mutation running.
      if (task && task.status === "claimed") {
        await ctx.db.patch(taskId, {
          status: "pending",
          claimedAt: undefined,
          readyAt: Date.now(),
        });
      }
    }
  },
});

// ─── Sweep stale claims ─────────────────────────────────────────────────────

export const sweepStaleClaims = mutation({
  args: {},
  returns: v.number(),
  handler: async (ctx) => {
    const config = await ctx.db.query("batchConfig").unique();
    const claimTimeoutMs = config?.claimTimeoutMs ?? 120_000;
    const cutoff = Date.now() - claimTimeoutMs;

    const stale = await ctx.db
      .query("batchTasks")
      .withIndex("by_status_claimedAt", (q) =>
        q.eq("status", "claimed").lt("claimedAt", cutoff),
      )
      .take(100);

    for (const task of stale) {
      await ctx.db.patch(task._id, {
        status: "pending",
        claimedAt: undefined,
        readyAt: Date.now(),
      });
    }

    // Schedule next sweep if there are claimed tasks
    if (stale.length > 0) {
      await ensureExecutors(ctx);
    }

    return stale.length;
  },
});

// ─── Executor lifecycle ─────────────────────────────────────────────────────

/**
 * Called by the executor action when it exits, so the component can
 * decrement active executor count and potentially schedule replacements.
 */
export const executorDone = mutation({
  args: { startMore: v.boolean() },
  handler: async (ctx, { startMore }) => {
    const config = await ctx.db.query("batchConfig").unique();
    if (!config) return;

    const newActive = Math.max(0, config.activeExecutors - 1);
    await ctx.db.patch(config._id, { activeExecutors: newActive });

    if (startMore) {
      await ensureExecutors(ctx);
    }
  },
});

// ─── Helpers ────────────────────────────────────────────────────────────────

async function upsertBatchConfig(
  ctx: MutationCtx,
  batchConfig:
    | {
        executorHandle: string;
        maxWorkers: number;
        claimTimeoutMs: number;
      }
    | undefined,
) {
  if (!batchConfig) return;
  const existing = await ctx.db.query("batchConfig").unique();
  if (existing) {
    await ctx.db.patch(existing._id, {
      executorHandle: batchConfig.executorHandle,
      maxWorkers: batchConfig.maxWorkers,
      claimTimeoutMs: batchConfig.claimTimeoutMs,
    });
  } else {
    await ctx.db.insert("batchConfig", {
      ...batchConfig,
      activeExecutors: 0,
    });
  }
}

async function ensureExecutors(ctx: MutationCtx) {
  const config = await ctx.db.query("batchConfig").unique();
  if (!config) return;

  // Start executors up to maxWorkers.
  // No need to check batchTasks for pending work here — this is only called
  // right after inserting a task, so we know there's work. Avoiding the
  // batchTasks query prevents OCC conflicts with concurrent claimBatch calls.
  if (config.activeExecutors < config.maxWorkers) {
    const handle = config.executorHandle as FunctionHandle<"action">;
    await ctx.db.patch(config._id, {
      activeExecutors: config.activeExecutors + 1,
    });
    await ctx.scheduler.runAfter(0, handle, {});
  }
}

async function callOnComplete(
  ctx: MutationCtx,
  taskId: Id<"batchTasks">,
  onComplete: { fnHandle: string; context?: unknown },
  runResult: RunResult,
) {
  try {
    const handle = onComplete.fnHandle as FunctionHandle<
      "mutation",
      OnCompleteArgs,
      void
    >;
    await ctx.runMutation(handle, {
      workId: taskId as unknown as string,
      context: onComplete.context,
      result: runResult,
    });
  } catch (e) {
    // Log but don't propagate onComplete errors, matching workpool behavior
    console.error(`[batch.complete] error running onComplete for ${taskId}`, e);
  }
}
