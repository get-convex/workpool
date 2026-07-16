import type { WithoutSystemFields } from "convex/server";
import { type Infer, v } from "convex/values";
import { type BatchResult, vBatchResult } from "@convex-dev/batch-worker";
import { internal } from "./_generated/api.js";
import { kickMainLoop } from "./kick.js";
import type { Doc, Id } from "./_generated/dataModel.js";
import {
  internalMutation,
  internalQuery,
  type MutationCtx,
  type QueryCtx,
} from "./_generated/server.js";
import type { CompleteJob } from "./complete.js";
import {
  createLogger,
  DEFAULT_LOG_LEVEL,
  type Logger,
  type LogLevel,
} from "./logging.js";
import {
  type Config,
  DEFAULT_MAX_PARALLELISM,
  fromSegment,
  getCurrentSegment,
  MINUTE,
  SECOND,
  type RunResult,
  toSegment,
} from "./shared.js";
import { vResultInternal } from "./schema.js";
import { generateReport, recordCompleted, recordStarted } from "./stats.js";

const CANCELLATION_BATCH_SIZE = 64; // the only queue that can get unbounded.
const RECOVERY_BATCH_SIZE = 32;
// Cap per-iteration completions + starts. Larger batches push per-iteration
// latency up without buying throughput: the loop re-fires immediately while
// it's draining, so smaller cheaper iterations carry the same work in aggregate.
const MAIN_BATCH_SIZE = 64;
const RECOVERY_THRESHOLD_MS = 5 * MINUTE; // attempt to recover jobs this old.
export const RECOVERY_PERIOD_SEGMENTS = toSegment(1 * MINUTE); // how often to check.
// While the queue is idle we keep the loop warm for this long (measured from
// when it last saw work) so a trickle of new work doesn't thrash the run
// status, re-polling this often during that window — preserving the old loop's
// cooldown behavior, now expressed via batch-worker's idle hints.
export const STATUS_COOLDOWN = 2 * SECOND;
export const COOLDOWN_CHECK_INTERVAL = 200;
// Buffer applied when querying with cursors. Transactions that started
// before ours may still be running and commit inserts at segments behind
// a previously advanced cursor — the buffer lets us pick those up. Most
// commits land within milliseconds, so a small buffer covers nearly all
// of them. The minute-cadence recovery iteration scans from segment 0
// (the start of each pending table) to catch any very-late commit that
// fell behind even this buffer.
const CURSOR_BUFFER_SEGMENTS = toSegment(15 * SECOND);

export const INITIAL_STATE: WithoutSystemFields<Doc<"internalState">> = {
  generation: 0n,
  segmentCursors: { incoming: 0n, completion: 0n, cancelation: 0n },
  lastRecovery: 0n,
  report: {
    completed: 0,
    succeeded: 0,
    failed: 0,
    retries: 0,
    canceled: 0,
    conflicted: 0,
    lastReportTs: 0,
  },
  running: [],
};

// ── The work query / worker mutation contract with batch-worker ────────────
// `getBatch` (the work query) decides whether there's work to do and, if so,
// hands a `batch` to `run` (the worker mutation). batch-worker drives the
// loop: it runs `getBatch`, runs `run` with the batch, re-runs to drain, and
// sleeps/idles per the hints `getBatch` returns. batch-worker also owns the
// generation guard (one loop chain at a time) and the liveness monitor that
// restarts the loop if it dies — so this module no longer schedules or
// recovers itself.

const vCompletion = v.object({
  _id: v.id("pendingCompletion"),
  workId: v.id("work"),
  runResult: vResultInternal,
  retry: v.boolean(),
  segment: v.int64(),
});
type Completion = Infer<typeof vCompletion>;

const vCancelation = v.object({
  _id: v.id("pendingCancelation"),
  workId: v.id("work"),
  segment: v.int64(),
});
type Cancelation = Infer<typeof vCancelation>;

const vStart = v.object({
  _id: v.id("pendingStart"),
  workId: v.id("work"),
  segment: v.int64(),
});
type Start = Infer<typeof vStart>;

/** The shape `getBatch` hands to `run`. */
const batchFields = {
  // The segment at query time — what "now" was when the batch was built.
  segment: v.int64(),
  // Whether this iteration should run the periodic work-recovery scan.
  recovery: v.boolean(),
  completions: v.array(vCompletion),
  cancelations: v.array(vCancelation),
  starts: v.array(vStart),
};
type Batch = Infer<ReturnType<typeof v.object<typeof batchFields>>>;

/**
 * The work query (batch-worker contract). Decides whether there's work to do,
 * and hands `run` a batch when there is. When there's nothing to do, returns
 * `idle` with hints for when to look again (next future start / next recovery
 * scan), plus a short cooldown so a trickle of work doesn't thrash the loop.
 *
 * batch-worker runs this as a snapshot read while draining and re-reads it with
 * a real dependency before going idle, so we just read the tables directly.
 */
export const getBatch = internalQuery({
  args: { name: v.string() },
  returns: vBatchResult(v.object(batchFields)),
  handler: async (ctx): Promise<BatchResult<Batch>> => {
    const globals = await getGlobals(ctx);
    const state = await ctx.db.query("internalState").unique();
    const running = state?.running ?? INITIAL_STATE.running;
    const cursors = state?.segmentCursors ?? INITIAL_STATE.segmentCursors;
    const lastRecovery = state?.lastRecovery ?? INITIAL_STATE.lastRecovery;
    const segment = getCurrentSegment();

    // Once per recovery period (≈1min), scan from segment 0 to catch any
    // very-late commit that fell behind the cursor buffer, and to recover any
    // stuck running jobs. Otherwise scan from the cursors (minus a buffer for
    // out-of-order inserts that landed behind the cursor since the last scan).
    const isRecoveryIter =
      running.length > 0 && segment - lastRecovery >= RECOVERY_PERIOD_SEGMENTS;
    const queryArgs = isRecoveryIter
      ? {
          completionCursor: 0n,
          cancelationCursor: 0n,
          incomingCursor: 0n,
          maxParallelism: globals.maxParallelism,
          runningCount: running.length,
        }
      : {
          completionCursor: cursors.completion - CURSOR_BUFFER_SEGMENTS,
          cancelationCursor: cursors.cancelation - CURSOR_BUFFER_SEGMENTS,
          incomingCursor: cursors.incoming - CURSOR_BUFFER_SEGMENTS,
          maxParallelism: globals.maxParallelism,
          runningCount: running.length,
        };

    const { allStarts, cancelations, completions } = await queryPending(
      ctx,
      queryArgs,
    );
    const starts = allStarts.filter((s) => s.segment <= segment);

    const hasWork =
      completions.length > 0 ||
      cancelations.length > 0 ||
      starts.length > 0 ||
      isRecoveryIter;

    if (hasWork) {
      const batch: Batch = {
        segment,
        recovery: isRecoveryIter,
        completions: completions.map((c) => ({
          _id: c._id,
          workId: c.workId,
          runResult: c.runResult,
          retry: c.retry,
          segment: c.segment,
        })),
        cancelations: cancelations.map((c) => ({
          _id: c._id,
          workId: c.workId,
          segment: c.segment,
        })),
        starts: starts.map((s) => ({
          _id: s._id,
          workId: s.workId,
          segment: s.segment,
        })),
      };
      return { kind: "work" as const, batch };
    }

    // Nothing to do now. Figure out when to wake up next: the sooner of the
    // earliest future-scheduled start and (if jobs are running) the next
    // recovery scan. A ping still wakes us sooner.
    const futureStart = allStarts.find((s) => s.segment > segment);
    const waits: number[] = [];
    if (futureStart) {
      waits.push(fromSegment(futureStart.segment) - Date.now());
    }
    if (running.length > 0) {
      const nextRecovery = lastRecovery + RECOVERY_PERIOD_SEGMENTS;
      waits.push(fromSegment(nextRecovery) - Date.now());
    }
    const timeoutMs =
      waits.length > 0 ? Math.max(0, Math.min(...waits)) : undefined;
    // Go (interruptibly) idle after the short cooldown. batch-worker confirms
    // with a real read before going idle, and every enqueue/complete/cancel
    // pings us to wake a waiting loop promptly. `timeoutMs` is a backstop for
    // future-scheduled work and the periodic recovery scan.
    return {
      kind: "idle" as const,
      cooldownMs: STATUS_COOLDOWN,
      pollIntervalMs: COOLDOWN_CHECK_INTERVAL,
      ...(timeoutMs !== undefined ? { timeoutMs } : {}),
    };
  },
});

/**
 * The worker mutation (batch-worker contract). Processes one batch from
 * `getBatch`: applies completions, cancelations, the periodic recovery scan,
 * and starts new work — then advances the cursors and persists state.
 * Returning `null` tells batch-worker to re-run immediately to keep draining.
 */
export const run = internalMutation({
  args: batchFields,
  returns: v.null(),
  handler: async (ctx, batch) => {
    const state = await getOrCreateState(ctx);
    const globals = await getGlobals(ctx);
    const console = createLogger(globals.logLevel);
    const segment = getCurrentSegment();

    const compLabel = `[main] pendingCompletion(${batch.completions.length})`;
    console.time(compLabel);
    const toCancel = await handleCompletions(
      ctx,
      state,
      batch.completions,
      console,
    );
    console.timeEnd(compLabel);

    const cancLabel = `[main] pendingCancelation(${batch.cancelations.length})`;
    console.time(cancLabel);
    await handleCancelation(ctx, state, batch.cancelations, console, toCancel);
    console.timeEnd(cancLabel);

    if (state.running.length === 0) {
      // If there's nothing active, reset lastRecovery.
      state.lastRecovery = segment;
    } else if (batch.recovery) {
      // Otherwise schedule recovery for any old jobs.
      const recoveryLabel = `[main] recovery(${state.running.length})`;
      console.time(recoveryLabel);
      await handleRecovery(ctx, state, console);
      console.timeEnd(recoveryLabel);
      state.lastRecovery = segment;
    }

    // ── Start new work ──
    // Slice to actual available capacity (completions may have freed slots).
    // Guard against negative numbers in case running.length > maxParallelism.
    const actualCapacity = globals.maxParallelism - state.running.length;
    const pending =
      actualCapacity > 0 ? batch.starts.slice(0, actualCapacity) : [];
    const startLabel = `[main] pendingStart(${pending.length})`;
    console.time(startLabel);
    await handleStart(ctx, state, pending, console, globals);
    console.timeEnd(startLabel);

    if (Date.now() - state.report.lastReportTs >= MINUTE) {
      // If minute rollover since last report, log report.
      // Try to avoid clock skew by shifting by a minute.
      let lastReportTs = state.report.lastReportTs + MINUTE;
      if (Date.now() > lastReportTs + MINUTE / 2) {
        // It's been a while, let's start fresh.
        lastReportTs = Date.now();
      }
      const reportLabel = "[main] report";
      console.time(reportLabel);
      await generateReport(ctx, console, state, globals);
      console.timeEnd(reportLabel);
      state.report = {
        completed: 0,
        succeeded: 0,
        failed: 0,
        retries: 0,
        canceled: 0,
        conflicted: 0,
        lastReportTs,
      };
    }

    // Advance cursors to skip tombstones on next scan, but only for the
    // queues we actually drained this iteration.
    if (batch.completions.length > 0) {
      state.segmentCursors.completion = batch.completions.at(-1)!.segment;
    }
    if (batch.cancelations.length > 0) {
      state.segmentCursors.cancelation = batch.cancelations.at(-1)!.segment;
    }
    if (pending.length > 0) {
      state.segmentCursors.incoming = pending.at(-1)!.segment;
    } else if (actualCapacity > 0 && batch.starts.length === 0) {
      // No more pending work to start and we had capacity — advance to now.
      state.segmentCursors.incoming = segment;
    }

    await ctx.db.replace("internalState", state._id, state);
    // Return null: batch-worker re-runs `getBatch` immediately to drain, and
    // idles (per getBatch's hints) once there's nothing left.
    return null;
  },
});

/** Read the three pending tables the loop processes. */
async function queryPending(
  ctx: QueryCtx,
  {
    completionCursor,
    cancelationCursor,
    incomingCursor,
    maxParallelism,
    runningCount,
  }: {
    completionCursor: bigint;
    cancelationCursor: bigint;
    incomingCursor: bigint;
    maxParallelism: number;
    runningCount: number;
  },
) {
  const completions = await ctx.db
    .query("pendingCompletion")
    .withIndex("segment", (q) => q.gte("segment", completionCursor))
    .take(Math.min(maxParallelism, MAIN_BATCH_SIZE));
  const cancelations = await ctx.db
    .query("pendingCancelation")
    .withIndex("segment", (q) => q.gte("segment", cancelationCursor))
    .take(CANCELLATION_BATCH_SIZE);
  // Available slots after we process this batch's completions, plus 1
  // for the +1 trick (detect overflow vs. a future-scheduled retry).
  // Cap at MAIN_BATCH_SIZE so a single iteration's per-item writes
  // (delete pendingStart + scheduler.runAfter) don't grow unbounded.
  const startLimit = Math.min(
    MAIN_BATCH_SIZE,
    Math.max(0, maxParallelism - runningCount + completions.length),
  );
  const excludedIds = [
    ...completions.map((c) => c.workId),
    ...cancelations.map((c) => c.workId),
  ];
  const allStarts =
    startLimit === 0
      ? []
      : await ctx.db
          .query("pendingStart")
          .withIndex("segment", (q) => q.gte("segment", incomingCursor))
          // eslint-disable-next-line @convex-dev/no-filter-in-query
          .filter((q) =>
            q.and(...excludedIds.map((id) => q.neq(q.field("workId"), id))),
          )
          .take(startLimit + 1);
  return { completions, cancelations, allStarts };
}

/**
 * Handles the completion of pending completions.
 * This only processes work that succeeded or failed, not canceled.
 */
async function handleCompletions(
  ctx: MutationCtx,
  state: Doc<"internalState">,
  completed: Completion[],
  console: Logger,
) {
  // Completions that were going to be retried but have since been canceled.
  const toCancel: CompleteJob[] = [];
  await Promise.all(
    completed.map(async (c) => {
      await ctx.db.delete("pendingCompletion", c._id);

      const running = state.running.find((r) => r.workId === c.workId);
      if (!running) {
        console.error(
          `[main] completing ${c.workId} but it's not in "running"`,
        );
        return;
      }
      if (c.retry) {
        // Only check for work if it's going to be retried.
        const work = await ctx.db.get("work", c.workId);
        if (!work) {
          console.warn(`[main] ${c.workId} is gone, but trying to complete`);
          return;
        }
        const wasStuckInScheduler = c.runResult.kind === "stuckInScheduler";
        const retried = await rescheduleJob(
          ctx,
          work,
          console,
          wasStuckInScheduler,
        );
        if (retried) {
          if (wasStuckInScheduler) {
            state.report.conflicted = (state.report.conflicted ?? 0) + 1;
            recordCompleted(console, work, "retrying conflicted", undefined);
          } else {
            state.report.retries++;
            recordCompleted(console, work, "retrying", undefined);
          }
        } else {
          // We don't retry if it's been canceled in the mean time.
          state.report.canceled++;
          toCancel.push({
            workId: c.workId,
            runResult: { kind: "canceled" },
            attempt: work.attempts,
          });
        }
      } else {
        if (c.runResult.kind === "success") {
          state.report.succeeded++;
        } else if (c.runResult.kind === "failed") {
          state.report.failed++;
        }
      }
    }),
  );
  // We do this after so the stats above know if it was in progress.
  const before = state.running.length;
  state.running = state.running.filter(
    (r) => !completed.some((c) => c.workId === r.workId),
  );
  const numCompleted = before - state.running.length;
  state.report.completed += numCompleted;
  console.debug(`[main] completed ${numCompleted} work`);
  return toCancel;
}

/**
 * Handles cancelation.
 */
async function handleCancelation(
  ctx: MutationCtx,
  state: Doc<"internalState">,
  canceled: Cancelation[],
  console: Logger,
  toCancel: CompleteJob[],
) {
  if (canceled.length) {
    console.debug(`[main] attempting to cancel ${canceled.length}`);
  }
  const canceledWork: Set<Id<"work">> = new Set();
  const runResult: RunResult = { kind: "canceled" };
  const jobs = toCancel.concat(
    ...(
      await Promise.all(
        canceled.map(async ({ _id, workId }) => {
          if (!(await ctx.db.get("pendingCancelation", _id))) {
            return null;
          }
          await ctx.db.delete("pendingCancelation", _id);
          if (canceledWork.has(workId)) {
            // We shouldn't have multiple pending cancelations for the same work.
            console.error(`[main] ${workId} already canceled`);
            return null;
          }
          const work = await ctx.db.get("work", workId);
          if (!work) {
            console.warn(`[main] ${workId} is gone, but trying to cancel`);
            return null;
          }
          // Ensure it doesn't retry.
          await ctx.db.patch("work", workId, { canceled: true });
          // Ensure it doesn't start.
          const pendingStart = await ctx.db
            .query("pendingStart")
            .withIndex("workId", (q) => q.eq("workId", workId))
            .unique();
          if (pendingStart && !canceledWork.has(workId)) {
            state.report.canceled++;
            await ctx.db.delete("pendingStart", pendingStart._id);
            canceledWork.add(workId);
            return { workId, runResult, attempt: work.attempts };
          }
          return null;
        }),
      )
    ).flatMap((r) => (r ? [r] : [])),
  );
  if (jobs.length) {
    await ctx.scheduler.runAfter(0, internal.complete.complete, { jobs });
  }
}

async function handleRecovery(
  ctx: MutationCtx,
  state: Doc<"internalState">,
  console: Logger,
) {
  const missing = new Set<Id<"work">>();
  const oldEnoughToConsider = Date.now() - RECOVERY_THRESHOLD_MS;
  const jobs = (
    await Promise.all(
      state.running.map(async (r) => {
        if (r.started >= oldEnoughToConsider) {
          return null;
        }
        const work = await ctx.db.get("work", r.workId);
        if (!work) {
          const pendingCompletion = await ctx.db
            .query("pendingCompletion")
            .withIndex("workId", (q) => q.eq("workId", r.workId))
            .first();
          if (!pendingCompletion) {
            missing.add(r.workId);
            console.error(
              `[main] ${r.workId} already gone (skipping recovery)`,
            );
          } else {
            console.debug(
              `[main] ${r.workId} already gone but has pendingCompletion`,
            );
          }
          return null;
        }
        return { ...r, attempt: work.attempts };
      }),
    )
  ).flatMap((r) => (r ? [r] : []));
  state.running = state.running.filter((r) => !missing.has(r.workId));
  // Pass scheduledAt so the recovery handler can measure scheduler lag
  // (`Date.now() - scheduledAt` when it actually runs) and judge stuck
  // mutations relative to current backlog.
  const scheduledAt = Date.now();
  for (let i = 0; i < jobs.length; i += RECOVERY_BATCH_SIZE) {
    const batch = jobs.slice(i, i + RECOVERY_BATCH_SIZE);
    await ctx.scheduler.runAfter(0, internal.recovery.recover, {
      jobs: batch,
      scheduledAt,
    });
  }
}

/**
 * Starts pending work.
 */
async function handleStart(
  ctx: MutationCtx,
  state: Doc<"internalState">,
  pending: Start[],
  console: Logger,
  { logLevel }: Config,
) {
  console.debug(`[main] scheduling ${pending.length} pending work`);
  // Start new work.
  state.running.push(
    ...(
      await Promise.all(
        pending.map(async ({ _id, workId, segment }) => {
          if (state.running.some((r) => r.workId === workId)) {
            console.error(`[main] ${workId} already running (skipping start)`);
            return null;
          }
          // Guard against a pendingStart a concurrent cancelation removed.
          if (!(await ctx.db.get("pendingStart", _id))) {
            return null;
          }
          const lagMs = Date.now() - fromSegment(segment);
          const scheduledId = await beginWork(ctx, workId, logLevel, lagMs);
          await ctx.db.delete("pendingStart", _id);
          if (!scheduledId) return null;
          return { scheduledId, workId, started: Date.now() };
        }),
      )
    ).flatMap((r) => (r ? [r] : [])),
  );
}

async function beginWork(
  ctx: MutationCtx,
  workId: Id<"work">,
  logLevel: LogLevel,
  lagMs: number,
): Promise<Id<"_scheduled_functions"> | null> {
  const console = createLogger(logLevel);
  const work = await ctx.db.get("work", workId);
  if (!work) {
    console.error(`Trying to start, but work not found: ${workId}`);
    return null;
  }
  const { attempts: attempt, fnHandle, fnArgs, payloadId } = work;
  const args = { workId, fnHandle, fnArgs, payloadId, logLevel, attempt };
  let scheduleId;
  if (work.fnType === "action") {
    scheduleId = await ctx.scheduler.runAfter(
      0,
      internal.worker.runActionWrapper,
      args,
    );
  } else if (work.fnType === "mutation" || work.fnType === "query") {
    scheduleId = await ctx.scheduler.runAfter(
      0,
      internal.worker.runMutationWrapper,
      {
        ...args,
        fnType: work.fnType,
      },
    );
  } else {
    throw new Error(`Unexpected fnType ${work.fnType}`);
  }
  recordStarted(console, work, lagMs, scheduleId);
  return scheduleId;
}

/**
 * Reschedules a job for retry.
 * If it's been canceled in the mean time, don't retry.
 * @returns true if the job was rescheduled, false if it was not.
 */
async function rescheduleJob(
  ctx: MutationCtx,
  work: Doc<"work">,
  console: Logger,
  wasStuckInScheduler: boolean,
): Promise<boolean> {
  const pendingCancelation = await ctx.db
    .query("pendingCancelation")
    .withIndex("workId", (q) => q.eq("workId", work._id))
    .unique();
  if (pendingCancelation) {
    // If there's an un-processed cancelation request, don't retry.
    console.warn(`[main] ${work._id} in pendingCancelation so not retrying`);
    return false;
  }
  if (work.canceled) {
    return false;
  }
  // stuckInScheduler retries immediately and doesn't need retryBehavior —
  // the function never ran, so user-configured backoff doesn't apply.
  let backoffMs: number;
  if (wasStuckInScheduler) {
    backoffMs = 0;
  } else if (work.retryBehavior) {
    backoffMs =
      work.retryBehavior.initialBackoffMs *
      Math.pow(work.retryBehavior.base, work.attempts - 1);
  } else {
    console.warn(`[main] ${work._id} has no retryBehavior so not retrying`);
    return false;
  }
  const existing = await ctx.db
    .query("pendingStart")
    .withIndex("workId", (q) => q.eq("workId", work._id))
    .first();
  if (existing) {
    // Not sure why this would ever happen, but ensure uniqueness explicitly.
    console.error(`[main] ${work._id} already in pendingStart so not retrying`);
    return false;
  }
  const nextAttempt = wasStuckInScheduler ? 0 : withJitter(backoffMs);
  const startTime = Date.now() + nextAttempt;
  const segment = toSegment(startTime);
  await ctx.db.insert("pendingStart", {
    workId: work._id,
    segment,
  });
  return true;
}

export function withJitter(delay: number) {
  return delay * (0.5 + Math.random());
}

async function getGlobals(ctx: QueryCtx) {
  const globals = await ctx.db.query("globals").unique();
  if (!globals) {
    return {
      maxParallelism: DEFAULT_MAX_PARALLELISM,
      logLevel: DEFAULT_LOG_LEVEL,
    };
  }
  return globals;
}

async function getOrCreateState(ctx: MutationCtx) {
  const state = await ctx.db.query("internalState").unique();
  if (state) return state;
  const globals = await getGlobals(ctx);
  const console = createLogger(globals.logLevel);
  console.debug("Creating initial internalState for main loop");
  return (await ctx.db.get(
    "internalState",
    await ctx.db.insert("internalState", INITIAL_STATE),
  ))!;
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
const console = "THIS IS A REMINDER TO USE createLogger";

/**
 * @deprecated Forwarder for in-flight scheduled `internal.loop.main` calls from
 * before the batch-worker migration. The real worker mutation is `run`.
 */
export const main = internalMutation({
  args: { generation: v.optional(v.int64()), segment: v.optional(v.int64()) },
  handler: async (ctx) => {
    await kickMainLoop(ctx, "kick");
  },
});

/**
 * @deprecated Forwarder for in-flight scheduled `internal.loop.updateRunStatus`
 * calls from before the batch-worker migration.
 */
export const updateRunStatus = internalMutation({
  args: { generation: v.optional(v.int64()), segment: v.optional(v.int64()) },
  handler: async (ctx) => {
    await kickMainLoop(ctx, "kick");
  },
});
