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
  endOfMs,
  fromSegment,
  fromTimestamp,
  getCurrentSegment,
  MINUTE,
  SECOND,
  type RunResult,
  toSegment,
  toTimestamp,
  vResult,
} from "./shared.js";
import { generateReport, recordCompleted, recordStarted } from "./stats.js";

const CANCELLATION_BATCH_SIZE = 64; // the only queue that can get unbounded.
const RECOVERY_BATCH_SIZE = 32;
const START_BATCH_SIZE = 32;
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
  runResult: vResult,
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
  runAt: v.optional(v.number()),
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
    const eligibleBefore = endOfMs(Date.now());

    // Once per recovery period (≈1min), check for stuck running jobs. The
    // pending queues need no periodic rescan: they're ordered by commit
    // timestamp, so nothing can appear behind a cursor we've read past.
    const isRecoveryIter =
      running.length > 0 && segment - lastRecovery >= RECOVERY_PERIOD_SEGMENTS;

    const { starts, cancelations, completions } = await queryPending(ctx, {
      completionCursor: cursors.completion,
      cancelationCursor: cursors.cancelation,
      incomingCursor: cursors.incoming,
      maxParallelism: globals.maxParallelism,
      runningCount: running.length,
      eligibleBefore,
    });

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
          segment: c.segment as bigint,
        })),
        cancelations: cancelations.map((c) => ({
          _id: c._id,
          workId: c.workId,
          segment: c.segment as bigint,
        })),
        starts,
      };
      return { kind: "work" as const, batch };
    }

    // Nothing to do now. Figure out when to wake up next: the sooner of the
    // earliest future-scheduled start and (if jobs are running) the next
    // recovery scan. A ping still wakes us sooner.
    const futureStart = await ctx.db
      .query("pendingStart")
      .withIndex("segment", (q) => q.gte("segment", eligibleBefore))
      .first();
    const waits: number[] = [];
    if (futureStart) {
      waits.push(fromTimestamp(futureStart.segment as bigint) - Date.now());
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
    // Entries whose `runAt` hasn't arrived were only visible because we
    // couldn't safely write that time into `segment` at enqueue. We can here:
    // this transaction also writes the cursor, so a value past `now` is
    // guaranteed to land ahead of it. Move them and they stop coming back.
    const now = Date.now();
    const isDue = (s: Start) => s.runAt === undefined || s.runAt <= now;
    const notYet = batch.starts.filter((s) => !isDue(s));
    const eligible = batch.starts.filter(isDue);
    if (notYet.length > 0) {
      const promoteLabel = `[main] promote(${notYet.length})`;
      console.time(promoteLabel);
      await promoteScheduled(ctx, notYet);
      console.timeEnd(promoteLabel);
    }

    // Slice to actual available capacity (completions may have freed slots).
    // Guard against negative numbers in case running.length > maxParallelism.
    const actualCapacity = globals.maxParallelism - state.running.length;
    const pending = actualCapacity > 0 ? eligible.slice(0, actualCapacity) : [];
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
        lastReportTs,
      };
    }

    // Advance cursors to skip tombstones on next scan, but only for the
    // queues we actually drained this iteration. The batches came back in
    // commit order, so the last entry is the furthest we read.
    if (batch.completions.length > 0) {
      state.segmentCursors.completion = batch.completions.at(-1)!.segment;
    }
    if (batch.cancelations.length > 0) {
      state.segmentCursors.cancelation = batch.cancelations.at(-1)!.segment;
    }
    // Capacity can cut `starts` short, so only advance over the leading run we
    // finished with — started or moved forward. Stopping at the first entry we
    // left alone is what keeps it from being skipped.
    const handled = new Set([...pending, ...notYet].map((s) => s._id));
    for (const start of batch.starts) {
      if (!handled.has(start._id)) break;
      state.segmentCursors.incoming = start.segment;
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
    eligibleBefore,
  }: {
    completionCursor: bigint;
    cancelationCursor: bigint;
    incomingCursor: bigint;
    maxParallelism: number;
    runningCount: number;
    eligibleBefore: bigint;
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
  // Available slots after we process this batch's completions. Cap at
  // MAIN_BATCH_SIZE so a single iteration's per-item writes (delete
  // pendingStart + scheduler.runAfter) don't grow unbounded.
  const startLimit = Math.min(
    MAIN_BATCH_SIZE,
    Math.max(0, maxParallelism - runningCount + completions.length),
  );
  const excludedIds = [
    ...completions.map((c) => c.workId),
    ...cancelations.map((c) => c.workId),
  ];
  // Everything eligible, oldest first. Work scheduled for later sorts above the
  // bound, so it's left alone without pinning the cursor.
  const starts =
    startLimit === 0
      ? []
      : await ctx.db
          .query("pendingStart")
          .withIndex("segment", (q) =>
            q.gte("segment", incomingCursor).lt("segment", eligibleBefore),
          )
          // eslint-disable-next-line @convex-dev/no-filter-in-query
          .filter((q) =>
            q.and(...excludedIds.map((id) => q.neq(q.field("workId"), id))),
          )
          .take(startLimit);
  return {
    completions,
    cancelations,
    starts: starts.map((s) => ({
      _id: s._id,
      workId: s.workId,
      segment: s.segment as bigint,
      runAt: s.runAt,
    })) satisfies Start[],
  };
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
        const retried = await rescheduleJob(ctx, work, console);
        if (retried) {
          state.report.retries++;
          recordCompleted(console, work, "retrying", undefined);
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
            // Drop any pendingStart left pointing at it. Nothing rescans that
            // lane, so a row left behind here would never be read again.
            const orphan = await ctx.db
              .query("pendingStart")
              .withIndex("workId", (q) => q.eq("workId", workId))
              .unique();
            if (orphan) await ctx.db.delete("pendingStart", orphan._id);
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
  for (let i = 0; i < jobs.length; i += RECOVERY_BATCH_SIZE) {
    const batch = jobs.slice(i, i + RECOVERY_BATCH_SIZE);
    await ctx.scheduler.runAfter(0, internal.recovery.recover, { jobs: batch });
  }
}

/**
 * Moves entries that aren't due yet to sort at their `runAt` instead of at the
 * commit timestamp they were enqueued with, so the cursor can pass them and
 * they don't come back until they're actually due. Safe to compute from the
 * clock here, unlike at enqueue: this transaction writes the cursor too, so a
 * time past `now` can't end up behind it.
 */
async function promoteScheduled(ctx: MutationCtx, notYet: Start[]) {
  await Promise.all(
    notYet.map(async ({ _id, runAt }) => {
      // A concurrent cancelation may have removed it.
      if (!(await ctx.db.get("pendingStart", _id))) return;
      await ctx.db.patch("pendingStart", _id, {
        // Round up to the next whole millisecond: the cursor has already
        // reached `endOfMs(now)`, so a `runAt` part-way through the current
        // millisecond would truncate to a timestamp behind it.
        segment: toTimestamp(Math.ceil(runAt!)),
      });
    }),
  );
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
  const starts = (
    await Promise.all(
      pending.map(async ({ _id, workId, segment, runAt }) => {
        if (state.running.some((r) => r.workId === workId)) {
          console.error(`[main] ${workId} already running (skipping start)`);
          // The row is spurious, and nothing rescans the lane behind the
          // cursor, so drop it rather than leave it unreadable.
          if (await ctx.db.get("pendingStart", _id)) {
            await ctx.db.delete("pendingStart", _id);
          }
          return null;
        }
        // Guard against a pendingStart a concurrent cancelation removed.
        if (!(await ctx.db.get("pendingStart", _id))) {
          return null;
        }
        const work = await ctx.db.get("work", workId);
        await ctx.db.delete("pendingStart", _id);
        if (!work) {
          console.error(`Trying to start, but work not found: ${workId}`);
          return null;
        }
        return {
          work,
          // `segment` is when this became eligible: the time it was scheduled
          // for, or the commit timestamp of the enqueue if it was ready then.
          lagMs: Date.now() - (runAt ?? fromTimestamp(segment)),
        };
      }),
    )
  ).flatMap((r) => (r ? [r] : []));

  state.running.push(...(await beginWorkBatch(ctx, starts, console, logLevel)));
}

async function beginWorkBatch(
  ctx: MutationCtx,
  starts: Array<{
    work: Doc<"work">;
    lagMs: number;
  }>,
  console: Logger,
  logLevel: LogLevel,
): Promise<
  Array<{
    workId: Id<"work">;
    scheduledId: Id<"_scheduled_functions">;
    started: number;
  }>
> {
  const running: Array<{
    workId: Id<"work">;
    scheduledId: Id<"_scheduled_functions">;
    started: number;
  }> = [];
  const actionOrQuery = starts.filter(
    ({ work }) => work.fnType === "action" || work.fnType === "query",
  );
  for (let i = 0; i < actionOrQuery.length; i += START_BATCH_SIZE) {
    const batch = actionOrQuery.slice(i, i + START_BATCH_SIZE);
    const scheduledId = await ctx.scheduler.runAfter(
      0,
      internal.worker.runBatch,
      {
        logLevel,
        items: batch.map(({ work }) => ({
          workId: work._id,
          fnHandle: work.fnHandle,
          fnArgs: work.fnArgs,
          payloadId: work.payloadId,
          attempt: work.attempts,
          fnType: work.fnType as "action" | "query",
        })),
      },
    );
    const started = Date.now();
    for (const { work, lagMs } of batch) {
      recordStarted(console, work, lagMs, scheduledId);
      running.push({ workId: work._id, scheduledId, started });
    }
  }

  const mutationStarts = starts.filter(
    ({ work }) => work.fnType === "mutation",
  );
  for (const { work, lagMs } of mutationStarts) {
    const scheduledId = await ctx.scheduler.runAfter(
      0,
      internal.worker.runMutationWrapper,
      {
        workId: work._id,
        fnHandle: work.fnHandle,
        fnArgs: work.fnArgs,
        payloadId: work.payloadId,
        logLevel,
        attempt: work.attempts,
        fnType: "mutation",
      },
    );
    recordStarted(console, work, lagMs, scheduledId);
    running.push({
      workId: work._id,
      scheduledId,
      started: Date.now(),
    });
  }

  const unexpected = starts.find(
    ({ work }) =>
      work.fnType !== "action" &&
      work.fnType !== "query" &&
      work.fnType !== "mutation",
  );
  if (unexpected) {
    throw new Error(`Unexpected fnType ${unexpected.work.fnType}`);
  }
  return running;
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
  if (!work.retryBehavior) {
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
  const backoffMs =
    work.retryBehavior.initialBackoffMs *
    Math.pow(work.retryBehavior.base, work.attempts - 1);
  const nextAttempt = withJitter(backoffMs);
  // The backoff can go straight into `segment` however short it is: we're in
  // the transaction that writes the cursor, so a time past now is certain to
  // sort ahead of it. No `runAt` needed — `segment` already is the start time.
  await ctx.db.insert("pendingStart", {
    workId: work._id,
    segment: toTimestamp(Date.now() + nextAttempt),
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
