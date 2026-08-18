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
  legacyRunAt,
  MINUTE,
  SECOND,
  type RunResult,
  toSegment,
  dueTimestamp,
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
  segmentCursors: {
    incoming: 0n,
    completion: 0n,
    cancelation: 0n,
    scheduled: 0n,
  },
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
  // Not a stored field: the start time recovered from an entry an older
  // version wrote (its `segment` is a 100ms bucket, not a timestamp). Absent
  // on anything written by this version — such entries are due when visible.
  runAt: v.optional(v.number()),
});
type Start = Infer<typeof vStart>;

/**
 * One commit stamp's worth of the out-of-order sweep, in commit order. The
 * sweep cursor may advance to `scheduledAt` once every entry in `starts` has
 * been retired; entries that shared the stamp but weren't behind the incoming
 * cursor need nothing (the segment scan will reach them) and aren't carried.
 */
const vSweepStep = v.object({
  scheduledAt: v.int64(),
  starts: v.array(vStart),
});
type SweepStep = Infer<typeof vSweepStep>;

/** The shape `getBatch` hands to `run`. */
const batchFields = {
  // The segment at query time — what "now" was when the batch was built.
  segment: v.int64(),
  // Whether this iteration should run the periodic work-recovery scan.
  recovery: v.boolean(),
  completions: v.array(vCompletion),
  cancelations: v.array(vCancelation),
  starts: v.array(vStart),
  // @deprecated Held-lane entries from a batch an unreleased revision built.
  // Ignored: those rows stay put, and the segment scan re-reads them.
  scheduled: v.optional(v.array(vStart)),
  // The out-of-order sweep's findings. Optional so a batch built just before
  // this field existed still validates.
  sweep: v.optional(v.array(vSweepStep)),
  // The highest commit timestamp observed while building the batch — the
  // frontier the incoming cursor may advance to, but not beyond. Optional for
  // the same deploy-boundary reason; `run` falls back to its own last stamp.
  cursorCeiling: v.optional(v.int64()),
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

    const { starts, sweep, cancelations, completions, cursorCeiling } =
      await queryPending(ctx, {
        completionCursor: cursors.completion,
        cancelationCursor: cursors.cancelation,
        incomingCursor: cursors.incoming,
        sweepCursor: cursors.scheduled ?? 0n,
        lastCommitTs: (state?.lastCommitTs as bigint | undefined) ?? 0n,
        maxParallelism: globals.maxParallelism,
        runningCount: running.length,
        eligibleBefore,
      });

    // Every sweep step is progress — an entry to start, or a cursor advance —
    // so its presence counts as work.
    const hasWork =
      completions.length > 0 ||
      cancelations.length > 0 ||
      starts.length > 0 ||
      sweep.length > 0 ||
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
        sweep,
        cursorCeiling,
      };
      return { kind: "work" as const, batch };
    }

    // Nothing to do now. Figure out when to wake up next: the sooner of the
    // earliest future-scheduled start and (if jobs are running) the next
    // recovery scan. A ping still wakes us sooner. Work the sweep couldn't
    // retire (a due entry with no capacity) is covered too: capacity implies
    // jobs are running, so the recovery wait applies and completions ping us.
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
    const sweep = batch.sweep ?? [];
    // The bounds on this run's cursor movement, both plain values:
    // - `floor`: the cursor as it stands. Every `segment` this run writes is
    //   raised to at least the floor, so nothing lands where the loop already
    //   read past; the cursor never moves backwards past it.
    // - `ceiling`: the highest commit timestamp the batch's snapshot had
    //   observed (computed alongside the batch in `queryPending`). The cursor
    //   never advances beyond it: a wall-clock key can exceed every commit
    //   stamp that exists, and a commit racing this run would land behind a
    //   cursor set there. Nothing relates the two clocks; observed commit
    //   stamps are the only safe frontier.
    const floor = state.segmentCursors.incoming;
    const ceiling =
      batch.cursorCeiling ?? (state.lastCommitTs as bigint | undefined) ?? 0n;

    const compLabel = `[main] pendingCompletion(${batch.completions.length})`;
    console.time(compLabel);
    const { toCancel, lowestRetry } = await handleCompletions(
      ctx,
      state,
      batch.completions,
      console,
      floor,
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
    // The segment scan's entries are all due (scheduled ones only become
    // visible at their start time), except entries written by older versions,
    // which the loop re-keys to their start time. Re-keying is safe here where
    // it wasn't at enqueue: this transaction also writes the cursor, so the
    // key can be placed relative to it. The sweep's entries are the opposite —
    // committed *behind* the cursor, so the scan will never reach them; they
    // start from here.
    const now = Date.now();
    const isDue = (s: Start) => s.runAt === undefined || s.runAt <= now;
    const swept = sweep.flatMap((step) => step.starts);
    const all = [...batch.starts, ...swept];
    const notYet = all.filter((s) => !isDue(s));
    // Oldest first: `segment` is when an entry became eligible, and a swept
    // entry has been eligible since its (past) start time.
    const eligible = all
      .filter(isDue)
      .sort(
        (a, b) =>
          (a.runAt ?? fromTimestamp(a.segment)) -
          (b.runAt ?? fromTimestamp(b.segment)),
      );
    let lowestPromoted: bigint | undefined;
    if (notYet.length > 0) {
      const promoteLabel = `[main] promote(${notYet.length})`;
      console.time(promoteLabel);
      lowestPromoted = await promoteScheduled(ctx, notYet, floor);
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
    // Capacity can cut the starts short, so only advance each cursor over the
    // leading run we finished with — started or re-keyed. Stopping at the
    // first entry we left alone is what keeps it from being skipped. The
    // incoming cursor additionally never passes the ceiling, nor any segment
    // this run wrote — a retry or re-keyed entry lands at or above the floor,
    // but the cursor's final position isn't known when those writes happen.
    // Equality is fine throughout: the index is read with `gte`.
    const handled = new Set([...pending, ...notYet].map((s) => s._id));
    let incoming = floor;
    for (const start of batch.starts) {
      if (!handled.has(start._id)) break;
      incoming = start.segment;
    }
    for (const cap of [ceiling, lowestRetry, lowestPromoted]) {
      if (cap !== undefined && cap < incoming) incoming = cap;
    }
    state.segmentCursors.incoming = maxTimestamp(floor, incoming);
    // The sweep cursor moves a whole commit stamp at a time, and only past
    // stamps whose behind-the-cursor entries were all retired. Its keys are
    // observed commit stamps, so no ceiling is needed.
    let sweepCursor = state.segmentCursors.scheduled ?? 0n;
    for (const step of sweep) {
      if (step.starts.some((s) => !handled.has(s._id))) break;
      sweepCursor = step.scheduledAt;
    }
    state.segmentCursors.scheduled = maxTimestamp(
      state.segmentCursors.scheduled ?? 0n,
      sweepCursor,
    );
    // Record this run's own commit stamp: the next run reads this document,
    // so its snapshot is at least this recent, and the mark seeds its ceiling.
    state.lastCommitTs = ctx.db.vars.commitTs;

    await ctx.db.replace("internalState", state._id, state);
    // Return null: batch-worker re-runs `getBatch` immediately to drain, and
    // idles (per getBatch's hints) once there's nothing left.
    return null;
  },
});

// How many `scheduledAt` entries the sweep inspects per iteration. Inspection
// is read-only and each entry is inspected once ever, so this mainly bounds
// per-iteration reads. It must exceed the most entries one transaction can
// stamp identically (a maximal batch enqueue), or the cursor could never
// clear that stamp.
const SWEEP_BATCH = 1024;

/** Read the three pending tables the loop processes. */
async function queryPending(
  ctx: QueryCtx,
  {
    completionCursor,
    cancelationCursor,
    incomingCursor,
    sweepCursor,
    lastCommitTs,
    maxParallelism,
    runningCount,
    eligibleBefore,
  }: {
    completionCursor: bigint;
    cancelationCursor: bigint;
    incomingCursor: bigint;
    sweepCursor: bigint;
    lastCommitTs: bigint;
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

  // ── The out-of-order sweep ──
  // A scheduled enqueue writes its start time as its `segment`, but commits
  // later — so if the commit took longer than the delay, the entry can land
  // behind the incoming cursor, where the segment scan will never see it.
  // Every wall-clock-keyed entry also records its commit stamp in
  // `scheduledAt`, and commit order is the one order nothing can land behind:
  // walking it from a cursor inspects every entry exactly once. An entry
  // whose `segment` is still at or above the incoming cursor can be passed
  // forever — the scan can only get past it by reading it. One behind the
  // cursor is due (the cursor never passes the current time) and must start
  // from here; those claim start slots first. The cursor moves a whole commit
  // stamp at a time (`gt` — a batch enqueue shares one stamp, and a bare
  // stamp can't split the group), and only past stamps whose stragglers all
  // got retired.
  const scanned = await ctx.db
    .query("pendingStart")
    .withIndex("scheduledAt", (q) => q.gt("scheduledAt", sweepCursor))
    // eslint-disable-next-line @convex-dev/no-filter-in-query
    .filter((q) =>
      q.and(...excludedIds.map((id) => q.neq(q.field("workId"), id))),
    )
    .take(SWEEP_BATCH);
  const exhausted = scanned.length < SWEEP_BATCH;
  const sweep: SweepStep[] = [];
  let oooBudget = startLimit;
  for (let i = 0; i < scanned.length; ) {
    const stamp = scanned[i].scheduledAt as bigint;
    const group = [];
    while (i < scanned.length && (scanned[i].scheduledAt as bigint) === stamp) {
      group.push(scanned[i]);
      i++;
    }
    // If the scan was cut mid-stamp, rows beyond it may share this stamp, so
    // it can't be cleared yet. (Reachable only if one transaction stamped
    // more entries than SWEEP_BATCH.)
    if (i === scanned.length && !exhausted) break;
    const behind = group.filter((r) => (r.segment as bigint) < incomingCursor);
    if (behind.length > oooBudget) break;
    oooBudget -= behind.length;
    const starts = behind.map((r) => ({
      _id: r._id,
      workId: r.workId,
      segment: r.segment as bigint,
    }));
    // Fold stamp-only advances into the previous step to keep batches small;
    // a step is only worth carrying for its starts or as the furthest bound.
    const previous = sweep.at(-1);
    if (starts.length === 0 && previous && previous.starts.length === 0) {
      previous.scheduledAt = stamp;
    } else {
      sweep.push({ scheduledAt: stamp, starts });
    }
  }

  // Entries the sweep starts take slots first; only fetch ready work for the
  // slots left over. Everything eligible, oldest first; work scheduled for
  // later sorts above the bound, so it's left alone without pinning the
  // cursor.
  const sweptCount = sweep.reduce((n, s) => n + s.starts.length, 0);
  const readyLimit = Math.max(0, startLimit - sweptCount);
  const starts =
    readyLimit === 0
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
          .take(readyLimit);
  // The highest commit timestamp observed while building this batch. Every
  // source is the stamp of a transaction visible to this snapshot, so
  // anything committing later is stamped above it — the frontier the cursor
  // may advance to. Entries with `scheduledAt` are wall-clock-keyed and don't
  // count (their stamps do); entries without it carry an observed commit
  // stamp in `segment` (an older version's tiny buckets can't win a max).
  const cursorCeiling = [
    lastCommitTs,
    ...completions.map((c) => c.segment as bigint),
    ...cancelations.map((c) => c.segment as bigint),
    ...starts
      .filter((s) => s.scheduledAt === undefined)
      .map((s) => s.segment as bigint),
    ...scanned.map((r) => r.scheduledAt as bigint),
  ].reduce(maxTimestamp);

  return {
    completions,
    cancelations,
    starts: starts.map((s) => {
      const segment = s.segment as bigint;
      return {
        _id: s._id,
        workId: s.workId,
        segment,
        // An entry from before commit-timestamp ordering keeps its start time
        // in `segment`; recovering it here means `run` handles it like any
        // other not-yet-due entry and re-keys it as a timestamp.
        runAt: legacyRunAt(segment),
      };
    }) satisfies Start[],
    sweep,
    cursorCeiling,
  };
}

/**
 * Handles the completion of pending completions.
 * This only processes work that succeeded or failed, not canceled.
 * Returns the lowest `segment` written for a retry, which bounds how far the
 * incoming cursor may advance this iteration.
 */
async function handleCompletions(
  ctx: MutationCtx,
  state: Doc<"internalState">,
  completed: Completion[],
  console: Logger,
  floor: bigint,
) {
  // Completions that were going to be retried but have since been canceled.
  const toCancel: CompleteJob[] = [];
  let lowestRetry: bigint | undefined;
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
        const retriedAt = await rescheduleJob(ctx, work, console, floor);
        if (retriedAt !== undefined) {
          if (lowestRetry === undefined || retriedAt < lowestRetry) {
            lowestRetry = retriedAt;
          }
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
  return { toCancel, lowestRetry };
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
            // A pendingStart left pointing at it, if any, is dropped when the
            // segment scan reads it and finds the work gone.
            console.warn(`[main] ${workId} is gone, but trying to cancel`);
            return null;
          }
          // Ensure it doesn't retry — and doesn't start, if its pendingStart
          // is only reachable through the scan (the pointer can be missing on
          // entries older versions wrote; `handleStart` checks this flag).
          await ctx.db.patch("work", workId, { canceled: true });
          // Ensure it doesn't start. The pointer can be stale; check it.
          const pendingStart = work.pendingStartId
            ? await ctx.db.get("pendingStart", work.pendingStartId)
            : null;
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

function maxTimestamp(a: bigint, b: bigint) {
  return a > b ? a : b;
}

/**
 * Re-keys entries that are visible but not due to sort at their start time,
 * so the cursor can pass them and they don't come back until they're actually
 * due. New-format entries are keyed at their start time and only become
 * visible once due, so this only handles the 100ms buckets older versions
 * wrote. Safe to compute from the clock here, unlike at enqueue: this
 * transaction writes the cursor too, and raising the key to at least `floor`
 * keeps the entry readable. Returns the lowest key written, which bounds how
 * far the cursor may advance.
 */
async function promoteScheduled(
  ctx: MutationCtx,
  notYet: Start[],
  floor: bigint,
): Promise<bigint | undefined> {
  let lowest: bigint | undefined;
  await Promise.all(
    notYet.map(async ({ _id, runAt }) => {
      // A concurrent cancelation may have removed it.
      if (!(await ctx.db.get("pendingStart", _id))) return;
      const segment = maxTimestamp(dueTimestamp(runAt!), floor);
      if (lowest === undefined || segment < lowest) lowest = segment;
      await ctx.db.patch("pendingStart", _id, {
        segment,
        // The key is now a wall-clock time, and `scheduledAt` is what records
        // that (its absence marks a key as an observed commit stamp).
        scheduledAt: ctx.db.vars.commitTs,
      });
    }),
  );
  return lowest;
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
        if (work.canceled) {
          // Canceled while its entry was only reachable through this scan
          // (no `pendingStartId` pointer — written by an older version).
          // Finish the cancelation that couldn't find the entry then.
          console.debug(`[main] ${workId} was canceled (not starting)`);
          state.report.canceled++;
          await ctx.scheduler.runAfter(0, internal.complete.complete, {
            jobs: [
              {
                workId,
                runResult: { kind: "canceled" as const },
                attempt: work.attempts,
              },
            ],
          });
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
 * @returns the `segment` it was rescheduled at, or undefined if it was not.
 */
async function rescheduleJob(
  ctx: MutationCtx,
  work: Doc<"work">,
  console: Logger,
  floor: bigint,
): Promise<bigint | undefined> {
  const pendingCancelation = await ctx.db
    .query("pendingCancelation")
    .withIndex("workId", (q) => q.eq("workId", work._id))
    .unique();
  if (pendingCancelation) {
    // If there's an un-processed cancelation request, don't retry.
    console.warn(`[main] ${work._id} in pendingCancelation so not retrying`);
    return undefined;
  }
  if (work.canceled) {
    return undefined;
  }
  if (!work.retryBehavior) {
    console.warn(`[main] ${work._id} has no retryBehavior so not retrying`);
    return undefined;
  }
  // The pointer can be stale (its entry started); check before declaring a
  // duplicate.
  if (
    work.pendingStartId &&
    (await ctx.db.get("pendingStart", work.pendingStartId))
  ) {
    // Not sure why this would ever happen, but ensure uniqueness explicitly.
    console.error(`[main] ${work._id} already in pendingStart so not retrying`);
    return undefined;
  }
  const backoffMs =
    work.retryBehavior.initialBackoffMs *
    Math.pow(work.retryBehavior.base, work.attempts - 1);
  const nextAttempt = withJitter(backoffMs);
  // The backoff can go straight into `segment` however short it is: we're in
  // the transaction that writes the cursor, and raising the key to at least
  // `floor` keeps the entry readable even for a zero backoff. The key is a
  // wall-clock time, so it records its commit stamp in `scheduledAt` like any
  // scheduled enqueue (its absence marks a key as an observed commit stamp).
  const segment = maxTimestamp(dueTimestamp(Date.now() + nextAttempt), floor);
  const pendingStartId = await ctx.db.insert("pendingStart", {
    workId: work._id,
    segment,
    scheduledAt: ctx.db.vars.commitTs,
  });
  await ctx.db.patch("work", work._id, { pendingStartId });
  return segment;
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
