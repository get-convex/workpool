/**
 * Batch module: component-side mutations and queries for the batch execution mode.
 *
 * The batch module maintains a `batchTasks` table of pending/claimed/completed tasks.
 * Executor actions (running in user space) call these mutations across the
 * component boundary to claim work, report results, and manage the queue.
 */
import type { FunctionHandle } from "convex/server";
import { v } from "convex/values";
import { internal } from "./_generated/api.js";
import type { Id } from "./_generated/dataModel.js";
import {
  internalMutation,
  mutation,
  type MutationCtx,
  query,
} from "./_generated/server.js";
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
    slot: v.number(),
    onComplete: v.optional(vOnCompleteFnContext),
    retryBehavior: v.optional(retryBehavior),
    // Batch config passed on first enqueue for lazy init
    batchConfig: v.optional(batchConfigArgs),
  },
  returns: v.id("batchTasks"),
  handler: async (ctx, args) => {
    const taskId = await ctx.db.insert("batchTasks", {
      name: args.name,
      args: args.args,
      slot: args.slot,
      status: "pending",
      readyAt: Date.now(),
      attempt: 0,
      onComplete: args.onComplete,
      retryBehavior: args.retryBehavior,
    });

    // Schedule executor start in a separate transaction to avoid OCC
    // conflicts on the batchConfig singleton.
    if (args.batchConfig) {
      await ctx.scheduler.runAfter(
        0,
        internal.batch._maybeStartExecutors,
        args.batchConfig,
      );
    } else {
      // No batchConfig means the client's configSentThisTx optimization
      // skipped it. Ensure executors are running from the DB config.
      await ctx.scheduler.runAfter(0, internal.batch._ensureExecutors, {});
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
        slot: v.number(),
        onComplete: v.optional(vOnCompleteFnContext),
        retryBehavior: v.optional(retryBehavior),
      }),
    ),
    batchConfig: v.optional(batchConfigArgs),
  },
  returns: v.array(v.id("batchTasks")),
  handler: async (ctx, args) => {
    const ids = await Promise.all(
      args.tasks.map((task) =>
        ctx.db.insert("batchTasks", {
          name: task.name,
          args: task.args,
          slot: task.slot,
          status: "pending",
          readyAt: Date.now(),
          attempt: 0,
          onComplete: task.onComplete,
          retryBehavior: task.retryBehavior,
        }),
      ),
    );

    if (args.batchConfig) {
      await ctx.scheduler.runAfter(
        0,
        internal.batch._maybeStartExecutors,
        args.batchConfig,
      );
    } else {
      await ctx.scheduler.runAfter(0, internal.batch._ensureExecutors, {});
    }
    return ids;
  },
});

// ─── Claim ──────────────────────────────────────────────────────────────────

/**
 * Read-only query to list pending task IDs for a slot.
 * Because this is a query (not a mutation), it has no OCC implications.
 * The executor calls this first, then calls claimByIds to claim the tasks
 * via point reads — avoiding index range conflicts with concurrent inserts
 * from onComplete handlers.
 */
export const listPending = query({
  args: { slot: v.number(), limit: v.number() },
  returns: v.array(v.id("batchTasks")),
  handler: async (ctx, { slot, limit }) => {
    const now = Date.now();
    const pending = await ctx.db
      .query("batchTasks")
      .withIndex("by_slot_status_readyAt", (q) =>
        q.eq("slot", slot).eq("status", "pending").lte("readyAt", now),
      )
      .take(limit);
    return pending.map((t) => t._id);
  },
});

/**
 * Claim specific tasks by ID using point reads only.
 * The read set is just the individual documents — no index range scan.
 * This eliminates OCC conflicts from concurrent inserts (onComplete enqueues).
 */
export const claimByIds = mutation({
  args: { taskIds: v.array(v.id("batchTasks")) },
  returns: v.array(
    v.object({
      _id: v.id("batchTasks"),
      name: v.string(),
      args: v.any(),
      attempt: v.number(),
      claimedAt: v.number(),
    }),
  ),
  handler: async (ctx, { taskIds }) => {
    const now = Date.now();
    const claimed = [];
    for (const taskId of taskIds) {
      const task = await ctx.db.get(taskId);
      // Only claim if still pending and ready — it may have been
      // claimed by a sweep or canceled between the query and this mutation.
      if (task && task.status === "pending" && task.readyAt <= now) {
        await ctx.db.patch(task._id, {
          status: "claimed",
          claimedAt: now,
        });
        claimed.push({
          _id: task._id,
          name: task.name,
          args: task.args,
          attempt: task.attempt,
          claimedAt: now,
        });
      }
    }
    return claimed;
  },
});


// ─── OnComplete return type ──────────────────────────────────────────────────

const vOnCompleteItem = v.object({
  fnHandle: v.string(),
  workId: v.string(),
  context: v.optional(v.any()),
  result: v.union(
    v.object({ kind: v.literal("success"), returnValue: v.any() }),
    v.object({ kind: v.literal("failed"), error: v.string() }),
    v.object({ kind: v.literal("canceled") }),
  ),
});

// ─── Complete ───────────────────────────────────────────────────────────────

export const complete = mutation({
  args: {
    taskId: v.id("batchTasks"),
    result: v.any(),
  },
  handler: async (ctx, { taskId, result }) => {
    const item = await completeOne(ctx, taskId, result);
    // Legacy single-complete still schedules onComplete for backward compat
    if (item) {
      await scheduleOnComplete(ctx, item);
    }
  },
});

/**
 * Batch-complete multiple tasks in a single mutation.
 * Returns onComplete data for the executor to dispatch directly,
 * bypassing the scheduler queue for much higher throughput.
 */
export const completeBatch = mutation({
  args: {
    items: v.array(
      v.object({
        taskId: v.id("batchTasks"),
        result: v.any(),
        claimedAt: v.optional(v.number()),
      }),
    ),
  },
  returns: v.array(vOnCompleteItem),
  handler: async (ctx, { items }) => {
    const onCompleteItems: OnCompleteItem[] = [];
    for (const { taskId, result, claimedAt } of items) {
      const item = await completeOne(ctx, taskId, result, claimedAt);
      if (item) onCompleteItems.push(item);
    }
    return onCompleteItems;
  },
});

type OnCompleteItem = {
  fnHandle: string;
  workId: string;
  context?: unknown;
  result: RunResult;
};

async function completeOne(
  ctx: MutationCtx,
  taskId: Id<"batchTasks">,
  result: unknown,
  claimedAt?: number,
): Promise<OnCompleteItem | null> {
  const task = await ctx.db.get(taskId);
  if (!task) return null;
  if (task.status !== "claimed") return null;
  // Verify claim ownership: if claimedAt is provided, it must match.
  // This prevents a stale executor (whose claim was swept and re-claimed
  // by another executor) from completing someone else's claim.
  if (claimedAt !== undefined && task.claimedAt !== claimedAt) return null;

  await ctx.db.delete(taskId);

  if (task.onComplete) {
    return {
      fnHandle: task.onComplete.fnHandle,
      workId: taskId as unknown as string,
      context: task.onComplete.context,
      result: { kind: "success", returnValue: result },
    };
  }
  return null;
}

// ─── Fail ───────────────────────────────────────────────────────────────────

export const fail = mutation({
  args: {
    taskId: v.id("batchTasks"),
    error: v.string(),
  },
  handler: async (ctx, { taskId, error }) => {
    const item = await failOne(ctx, taskId, error);
    if (item) {
      await scheduleOnComplete(ctx, item);
    }
  },
});

/**
 * Batch-fail multiple tasks in a single mutation.
 * Returns onComplete data for permanently-failed tasks (no more retries).
 */
export const failBatch = mutation({
  args: {
    items: v.array(
      v.object({
        taskId: v.id("batchTasks"),
        error: v.string(),
        claimedAt: v.optional(v.number()),
      }),
    ),
  },
  returns: v.array(vOnCompleteItem),
  handler: async (ctx, { items }) => {
    const onCompleteItems: OnCompleteItem[] = [];
    for (const { taskId, error, claimedAt } of items) {
      const item = await failOne(ctx, taskId, error, claimedAt);
      if (item) onCompleteItems.push(item);
    }
    return onCompleteItems;
  },
});

async function failOne(
  ctx: MutationCtx,
  taskId: Id<"batchTasks">,
  error: string,
  claimedAt?: number,
): Promise<OnCompleteItem | null> {
  const task = await ctx.db.get(taskId);
  if (!task) return null;
  if (task.status !== "claimed") return null;
  // Verify claim ownership (same as completeOne)
  if (claimedAt !== undefined && task.claimedAt !== claimedAt) return null;

  const maxAttempts = task.retryBehavior?.maxAttempts;
  const nextAttempt = task.attempt + 1;
  const shouldRetry = !!maxAttempts && nextAttempt < maxAttempts;

  if (shouldRetry) {
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
    return null;
  }

  await ctx.db.delete(taskId);

  if (task.onComplete) {
    return {
      fnHandle: task.onComplete.fnHandle,
      workId: taskId as unknown as string,
      context: task.onComplete.context,
      result: { kind: "failed", error },
    };
  }
  return null;
}

// ─── Batch onComplete dispatch ───────────────────────────────────────────────

/**
 * Dispatch multiple onComplete handlers in a single mutation transaction.
 * This is dramatically more efficient than calling each handler individually
 * from the action, since it reduces mutation count from O(N) to O(N/batchSize).
 *
 * Each onComplete handler runs in the same transaction, so all their writes
 * (including enqueue calls for the next pipeline stage) happen atomically.
 */
export const dispatchOnCompleteBatch = mutation({
  args: {
    items: v.array(vOnCompleteItem),
  },
  returns: v.number(),
  handler: async (ctx, { items }) => {
    let failures = 0;
    for (const item of items) {
      try {
        await ctx.runMutation(
          item.fnHandle as FunctionHandle<"mutation">,
          {
            workId: item.workId,
            context: item.context,
            result: item.result,
          },
        );
      } catch (err: unknown) {
        failures++;
        // Log but continue — don't let one bad handler block the rest.
        // The failed handler's writes are not applied, but the rest are.
        console.error(
          `[batch] onComplete handler failed for workId=${item.workId}:`,
          String(err),
        );
      }
    }
    return failures;
  },
});

// ─── Count pending ──────────────────────────────────────────────────────────

export const countPending = query({
  args: { slot: v.optional(v.number()) },
  returns: v.number(),
  handler: async (ctx, { slot }) => {
    // Just check if there's at least one pending task (avoids 32K read limit)
    if (slot !== undefined) {
      const one = await ctx.db
        .query("batchTasks")
        .withIndex("by_slot_status_readyAt", (q) =>
          q.eq("slot", slot).eq("status", "pending"),
        )
        .first();
      return one ? 1 : 0;
    }
    // Global check: single query using status index (1 query instead of 20 per-slot)
    const pending = await ctx.db
      .query("batchTasks")
      .withIndex("by_status_claimedAt", (q) => q.eq("status", "pending"))
      .first();
    if (pending) return 1;
    // Also check for any claimed tasks (might be stale)
    const claimed = await ctx.db
      .query("batchTasks")
      .withIndex("by_status_claimedAt", (q) => q.eq("status", "claimed"))
      .first();
    if (claimed) return 1;
    return 0;
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

    // Call onComplete with canceled result (cancel is user-initiated, uses scheduler)
    if (task.onComplete) {
      await scheduleOnComplete(ctx, {
        fnHandle: task.onComplete.fnHandle,
        workId: taskId as unknown as string,
        context: task.onComplete.context,
        result: { kind: "canceled" },
      });
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
  args: {
    // Accept either plain IDs (backward compat) or {taskId, claimedAt} pairs
    taskIds: v.optional(v.array(v.id("batchTasks"))),
    items: v.optional(
      v.array(v.object({ taskId: v.id("batchTasks"), claimedAt: v.number() })),
    ),
  },
  handler: async (ctx, { taskIds, items }) => {
    // Support both formats: items with claimedAt (preferred) or plain taskIds
    const entries: { taskId: Id<"batchTasks">; claimedAt?: number }[] =
      items
        ? items
        : (taskIds ?? []).map((id) => ({ taskId: id }));

    for (const { taskId, claimedAt } of entries) {
      const task = await ctx.db.get(taskId);
      // Only release if it's still claimed — it may have completed
      // between the executor deciding to release and this mutation running.
      // If claimedAt is provided, verify ownership to avoid releasing
      // a task that was re-claimed by another executor.
      if (task && task.status === "claimed") {
        if (claimedAt !== undefined && task.claimedAt !== claimedAt) continue;
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

    return stale.length;
  },
});

// ─── Executor lifecycle ─────────────────────────────────────────────────────

/**
 * Called by the executor action when it exits, so the component can
 * decrement active executor count and potentially schedule replacements.
 */
export const executorDone = mutation({
  args: { startMore: v.boolean(), slot: v.number() },
  handler: async (ctx, { startMore, slot }) => {
    const config = await ctx.db.query("batchConfig").unique();
    if (!config) return;

    // Remove this slot from activeSlots
    const newSlots = config.activeSlots.filter((s) => s !== slot);

    if (startMore) {
      // Start executors for ALL missing slots, not just this one.
      // In pipeline workloads, stage 2+ tasks land in random slots —
      // if those executors already exited, they need to be restarted now
      // (the 30s watchdog is too slow). Duplicate scheduling is safe:
      // executors exit quickly when they find no work.
      const activeSet = new Set(newSlots);
      const handle = config.executorHandle as FunctionHandle<"action">;
      for (let s = 0; s < config.maxWorkers; s++) {
        if (!activeSet.has(s)) {
          newSlots.push(s);
          await ctx.scheduler.runAfter(0, handle, { slot: s });
        }
      }
    }

    await ctx.db.patch(config._id, { activeSlots: newSlots });
  },
});

// ─── Internal: executor startup (separate tx to avoid OCC) ──────────────

/**
 * Scheduled from enqueue/enqueueBatch to set up config and start executors
 * in a separate transaction. This avoids OCC conflicts between concurrent
 * enqueue mutations that would otherwise all read/write the batchConfig doc.
 */
export const _maybeStartExecutors = internalMutation({
  args: batchConfigArgs,
  handler: async (ctx, args) => {
    await upsertBatchConfig(ctx, args);

    // Start executors for any missing slots 0..maxWorkers-1.
    const config = await ctx.db.query("batchConfig").unique();
    if (!config) return;

    const activeSet = new Set(config.activeSlots);
    const handle = config.executorHandle as FunctionHandle<"action">;
    const newSlots = [...config.activeSlots];

    for (let slot = 0; slot < config.maxWorkers; slot++) {
      if (!activeSet.has(slot)) {
        await ctx.scheduler.runAfter(0, handle, { slot });
        newSlots.push(slot);
      }
    }

    const patch: Record<string, unknown> = {};
    if (newSlots.length !== config.activeSlots.length) {
      patch.activeSlots = newSlots;
    }

    // Schedule watchdog if not recently scheduled (dedup window: 20s)
    const now = Date.now();
    if (
      !config.watchdogScheduledAt ||
      now - config.watchdogScheduledAt > 20_000
    ) {
      await ctx.scheduler.runAfter(30_000, internal.batch._watchdog);
      patch.watchdogScheduledAt = now;
    }

    if (Object.keys(patch).length > 0) {
      await ctx.db.patch(config._id, patch);
    }
  },
});

/**
 * Lightweight executor check — reads config from DB and starts missing
 * executors + watchdog. Scheduled from enqueue when batchConfig is not
 * provided (client-side configSentThisTx optimization).
 */
export const _ensureExecutors = internalMutation({
  args: {},
  handler: async (ctx) => {
    const config = await ctx.db.query("batchConfig").unique();
    if (!config) return;

    const activeSet = new Set(config.activeSlots);
    const handle = config.executorHandle as FunctionHandle<"action">;
    const newSlots = [...config.activeSlots];

    for (let slot = 0; slot < config.maxWorkers; slot++) {
      if (!activeSet.has(slot)) {
        await ctx.scheduler.runAfter(0, handle, { slot });
        newSlots.push(slot);
      }
    }

    const patch: Record<string, unknown> = {};
    if (newSlots.length !== config.activeSlots.length) {
      patch.activeSlots = newSlots;
    }

    // Restart watchdog if not recently scheduled
    const now = Date.now();
    if (
      !config.watchdogScheduledAt ||
      now - config.watchdogScheduledAt > 20_000
    ) {
      await ctx.scheduler.runAfter(30_000, internal.batch._watchdog);
      patch.watchdogScheduledAt = now;
    }

    if (Object.keys(patch).length > 0) {
      await ctx.db.patch(config._id, patch);
    }
  },
});

// ─── Watchdog: self-healing after redeploys / executor crashes ────────────

/**
 * Periodic watchdog that self-schedules while work remains.
 * Handles three failure modes:
 * 1. Stale claims: tasks claimed by dead executors (crash/redeploy)
 * 2. Dead executors: slots marked active but not processing work
 * 3. Missing executors: slots not in activeSlots with pending work
 *
 * After a redeploy, scheduled functions survive so the watchdog fires
 * even though all executor actions were killed.
 */
export const _watchdog = internalMutation({
  args: {},
  handler: async (ctx) => {
    const config = await ctx.db.query("batchConfig").unique();
    if (!config) return;

    const now = Date.now();

    // 1. Sweep stale claims (older than claimTimeoutMs)
    const claimCutoff = now - config.claimTimeoutMs;
    const staleClaims = await ctx.db
      .query("batchTasks")
      .withIndex("by_status_claimedAt", (q) =>
        q.eq("status", "claimed").lt("claimedAt", claimCutoff),
      )
      .take(500);

    const deadSlots = new Set<number>();
    for (const task of staleClaims) {
      deadSlots.add(task.slot);
      await ctx.db.patch(task._id, {
        status: "pending",
        claimedAt: undefined,
        readyAt: now,
      });
    }

    // 2. Detect stuck executors: sample pending tasks via the status
    //    index (1 query) instead of per-slot scans (was 20 queries).
    //    Pending tasks ordered by _id (oldest first), so old stuck
    //    tasks appear at the front of the sample.
    const stuckThreshold = now - 30_000;
    const activeSet = new Set(config.activeSlots);
    const pendingSample = await ctx.db
      .query("batchTasks")
      .withIndex("by_status_claimedAt", (q) => q.eq("status", "pending"))
      .take(100);
    for (const task of pendingSample) {
      if (task.readyAt <= stuckThreshold && activeSet.has(task.slot)) {
        deadSlots.add(task.slot);
      }
    }

    // 3. Remove dead slots from activeSlots
    const newSlots =
      deadSlots.size > 0
        ? config.activeSlots.filter((s) => !deadSlots.has(s))
        : [...config.activeSlots];

    // 4. Check if any work remains (1 query via status index instead of 20 per-slot)
    let hasWork = staleClaims.length > 0;
    if (!hasWork) {
      hasWork = pendingSample.length > 0;
    }
    if (!hasWork) {
      const claimed = await ctx.db
        .query("batchTasks")
        .withIndex("by_status_claimedAt", (q) => q.eq("status", "claimed"))
        .first();
      if (claimed) hasWork = true;
    }

    if (!hasWork) {
      // No work left — clean up and stop the watchdog
      if (config.activeSlots.length > 0) {
        await ctx.db.patch(config._id, { activeSlots: [] });
      }
      return;
    }

    // 5. Start executors for missing slots
    const activeSetNew = new Set(newSlots);
    const handle = config.executorHandle as FunctionHandle<"action">;
    for (let s = 0; s < config.maxWorkers; s++) {
      if (!activeSetNew.has(s)) {
        newSlots.push(s);
        await ctx.scheduler.runAfter(0, handle, { slot: s });
      }
    }

    // 6. Update config and reschedule watchdog
    await ctx.db.patch(config._id, {
      activeSlots: newSlots,
      watchdogScheduledAt: now,
    });
    await ctx.scheduler.runAfter(30_000, internal.batch._watchdog);
  },
});

// ─── Reset (for testing) ─────────────────────────────────────────────────────

export const resetConfig = mutation({
  args: {},
  handler: async (ctx) => {
    const config = await ctx.db.query("batchConfig").unique();
    if (config) {
      await ctx.db.delete(config._id);
    }
  },
});

export const resetTasks = mutation({
  args: {},
  returns: v.object({ deleted: v.number(), more: v.boolean() }),
  handler: async (ctx) => {
    const tasks = await ctx.db.query("batchTasks").take(1000);
    for (const task of tasks) {
      await ctx.db.delete(task._id);
    }
    return { deleted: tasks.length, more: tasks.length === 1000 };
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
    // Skip write if config already matches — avoids OCC contention
    if (
      existing.executorHandle === batchConfig.executorHandle &&
      existing.maxWorkers === batchConfig.maxWorkers &&
      existing.claimTimeoutMs === batchConfig.claimTimeoutMs
    ) {
      return;
    }
    await ctx.db.patch(existing._id, {
      executorHandle: batchConfig.executorHandle,
      maxWorkers: batchConfig.maxWorkers,
      claimTimeoutMs: batchConfig.claimTimeoutMs,
    });
  } else {
    await ctx.db.insert("batchConfig", {
      ...batchConfig,
      activeSlots: [],
    });
  }
}

/**
 * Schedule onComplete via the scheduler. Used by legacy single-item
 * complete/fail mutations. The batch variants return onComplete data
 * for the executor to dispatch directly (much higher throughput).
 */
async function scheduleOnComplete(
  ctx: MutationCtx,
  item: OnCompleteItem,
) {
  const handle = item.fnHandle as FunctionHandle<
    "mutation",
    OnCompleteArgs,
    void
  >;
  await ctx.scheduler.runAfter(0, handle, {
    workId: item.workId,
    context: item.context,
    result: item.result,
  });
}
