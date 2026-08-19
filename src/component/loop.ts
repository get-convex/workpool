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
  fromTimestamp,
  legacyRunAt,
  MINUTE,
  SECOND,
  type RunResult,
  toTimestamp,
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
export const RECOVERY_PERIOD_NS = toTimestamp(MINUTE); // how often to check.
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
    sweep: 0n,
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
  // The start time recovered from an entry a version ≤ 0.4.9 wrote (see
  // `legacyRunAt`). Not a stored field.
  legacyStartTime: v.optional(v.number()),
});
type Start = Infer<typeof vStart>;

// batch-worker runs `getBatch` and `run` in the same transaction, so a batch
// never crosses a deploy or a snapshot: `run` sees exactly the state the
// batch was built from.
/** The shape `getBatch` hands to `run`. */
const batchFields = {
  // What "now" was (in nanoseconds) when the batch was built.
  now: v.int64(),
  // Whether this iteration should run the periodic work-recovery scan.
  recovery: v.boolean(),
  completions: v.array(vCompletion),
  cancelations: v.array(vCancelation),
  starts: v.array(vStart),
  // Entries the out-of-order sweep found behind the incoming cursor (their
  // enqueues lost the race), and how far its inspection got — the position
  // its cursor may advance to once every found entry is retired. `sweepStop`
  // is absent when it inspected nothing.
  sweepStarts: v.array(vStart),
  sweepStop: v.optional(v.int64()),
  // The highest commit timestamp observed while building the batch — the
  // frontier the incoming cursor may advance to, but not beyond.
  cursorCeiling: v.int64(),
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
    const state = await ctx.db.query("internalState").order("desc").first();
    const running = state?.running ?? INITIAL_STATE.running;
    const cursors = state?.segmentCursors ?? INITIAL_STATE.segmentCursors;
    const lastRecovery = state?.lastRecovery ?? INITIAL_STATE.lastRecovery;
    const nowTs = toTimestamp(Date.now());
    const eligibleBefore = endOfMs(Date.now());

    // Once per recovery period (≈1min), check for stuck running jobs. The
    // pending queues need no periodic rescan: they're ordered by commit
    // timestamp, so nothing can appear behind a cursor we've read past.
    const isRecoveryIter =
      running.length > 0 && nowTs - lastRecovery >= RECOVERY_PERIOD_NS;

    const {
      starts,
      sweepStarts,
      sweepStop,
      cancelations,
      completions,
      cursorCeiling,
    } = await queryPending(ctx, {
      completionCursor: cursors.completion,
      cancelationCursor: cursors.cancelation,
      incomingCursor: cursors.incoming,
      sweepCursor: cursors.sweep ?? 0n,
      lastCommitTs: (state?.lastCommitTs as bigint | undefined) ?? 0n,
      maxParallelism: globals.maxParallelism,
      runningCount: running.length,
      eligibleBefore,
    });

    // The sweep counts as work when it found entries to start or moved past
    // new documents; re-verifying the live documents at its inclusive
    // boundary is not progress and must not keep the loop awake.
    const hasWork =
      completions.length > 0 ||
      cancelations.length > 0 ||
      starts.length > 0 ||
      sweepStarts.length > 0 ||
      (sweepStop !== undefined && sweepStop > (cursors.sweep ?? 0n)) ||
      isRecoveryIter;

    if (hasWork) {
      const batch: Batch = {
        now: nowTs,
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
        sweepStarts,
        sweepStop,
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
      const nextRecovery = lastRecovery + RECOVERY_PERIOD_NS;
      waits.push(fromTimestamp(nextRecovery) - Date.now());
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
    const nowTs = toTimestamp(Date.now());

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
      state.lastRecovery = nowTs;
    } else if (batch.recovery) {
      // Otherwise schedule recovery for any old jobs.
      const recoveryLabel = `[main] recovery(${state.running.length})`;
      console.time(recoveryLabel);
      await handleRecovery(ctx, state, console);
      console.timeEnd(recoveryLabel);
      state.lastRecovery = nowTs;
    }

    // ── Start new work ──
    // The segment scan's entries are all due (scheduled ones only become
    // visible at their start time), except entries written by older versions,
    // which the loop re-keys to their start time. The sweep's entries are the
    // opposite — committed *behind* the cursor, so the scan will never reach
    // them; they start from here.
    const now = Date.now();
    const isDue = (s: Start) =>
      s.legacyStartTime === undefined || s.legacyStartTime <= now;
    const all = [...batch.starts, ...batch.sweepStarts];
    const notYet = all.filter((s) => !isDue(s));
    // Oldest first: `segment` is when an entry became eligible, and a swept
    // entry has been eligible since its (past) start time.
    const eligible = all
      .filter(isDue)
      .sort(
        (a, b) =>
          (a.legacyStartTime ?? fromTimestamp(a.segment)) -
          (b.legacyStartTime ?? fromTimestamp(b.segment)),
      );
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
    // Capacity can cut the starts short, so only advance each cursor over the
    // leading run we finished with — started or re-keyed. Stopping at the
    // first entry we left alone is what keeps it from being skipped. The
    // incoming cursor additionally never passes `cursorCeiling`, the highest
    // commit timestamp observed while building the batch: a wall-clock key
    // can exceed every commit stamp that exists, and a commit racing this run
    // would land behind a cursor set there. (The keys this run itself writes
    // need no accounting — they're all at or above the end of the current
    // millisecond, which no cursor reaches: the scan's bound is exclusive and
    // the ceiling only lowers.) Equality is fine throughout: the index is
    // read with `gte`.
    const handled = new Set([...pending, ...notYet].map((s) => s._id));
    let incoming = state.segmentCursors.incoming;
    for (const start of batch.starts) {
      if (!handled.has(start._id)) break;
      incoming = start.segment;
    }
    // Never backwards: a batch can carry a ceiling below the cursor when it
    // observed no commit stamps at all (e.g. a recovery-only iteration).
    state.segmentCursors.incoming = maxBigint(
      state.segmentCursors.incoming,
      incoming < batch.cursorCeiling ? incoming : batch.cursorCeiling,
    );
    // The sweep cursor rests at the last inspected entry — (commit stamp,
    // creation time), since a batch enqueue shares one stamp. Its components
    // are read straight off inspected entries, so no ceiling is needed. If
    // capacity cut one of the sweep's entries (rare: the estimate and the
    // real capacity disagree only when a completion didn't free a slot), the
    // cursor stays put and the next sweep re-inspects from the old position.
    if (
      batch.sweepStop !== undefined &&
      batch.sweepStarts.every((s) => handled.has(s._id))
    ) {
      state.segmentCursors.sweep = maxBigint(
        state.segmentCursors.sweep ?? 0n,
        batch.sweepStop,
      );
    }
    // Record this run's own commit stamp: the next run reads this document,
    // so its snapshot is at least this recent, and the mark seeds its ceiling.
    state.lastCommitTs = ctx.db.vars.commitTs;

    await ctx.db.replace("internalState", state._id, state);
    // Return null: batch-worker re-runs `getBatch` immediately to drain, and
    // idles (per getBatch's hints) once there's nothing left.
    return null;
  },
});

// How many `scheduledAt` entries the sweep inspects per iteration — a read
// budget, not a work bound: inspection is read-only and each entry is
// inspected once ever, because the cursor advances a whole commit stamp at a
// time and never revisits a cleared stamp. (Advancing inclusively instead
// would re-read the boundary stamp's group every iteration for as long as its
// entries wait to come due — unbounded for far-future work.) A stamp group
// larger than one page is paged through in the same iteration, so this needn't
// exceed the largest batch enqueue.
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
  // Work completing or canceling this iteration is skipped when reading; the
  // same iteration removes those entries, so the cursors may pass them.
  const excluded = new Set([
    ...completions.map((c) => c.workId),
    ...cancelations.map((c) => c.workId),
  ]);

  // ── The out-of-order sweep ──
  // A scheduled enqueue writes its start time as its `segment`, but commits
  // later — so if the commit took longer than the delay, the document can
  // land behind the incoming cursor, where the segment scan will never see
  // it. Every scheduled enqueue also records its commit stamp in `scanTs`,
  // and commit order is the one order nothing can land behind: walking it
  // from a cursor inspects every document once (plus re-verifying the few
  // live documents sharing the boundary stamp — the cursor is inclusive
  // because a transaction can write several documents with one stamp). A
  // document whose `segment` is at or above the incoming cursor can be passed
  // forever: the scan can only get past it by reading it. One behind the
  // cursor is due (the cursor never passes the current time), and all its
  // entries must start from here, claiming start slots first. Iteration
  // stops at the read budget, or when a behind document's entries exceed the
  // remaining slots, leaving it uninspected rather than reading work that
  // couldn't start.
  const sweepStarts: Start[] = [];
  let sweepStop: bigint | undefined;
  {
    let inspected = 0;
    const stream = ctx.db
      .query("pendingStart")
      .withIndex("scanTs", (q) => q.gte("scanTs", sweepCursor));
    for await (const doc of stream) {
      const segment = doc.segment as bigint;
      const ids = memberIds(doc).filter((id) => !excluded.has(id));
      const behind = segment < incomingCursor;
      if (behind) {
        // Take what fits in the start slots. A partially-taken document keeps
        // the cursor at bay: its started entries patch out, and the next
        // sweep re-finds the remainder.
        const room = startLimit - sweepStarts.length;
        if (room <= 0) break;
        const taken = ids.slice(0, room);
        inspected += taken.length;
        for (const workId of taken) {
          sweepStarts.push({ _id: doc._id, workId, segment });
        }
        if (taken.length < ids.length) break;
      } else {
        if (inspected > 0 && inspected + ids.length > SWEEP_BATCH) break;
        inspected += ids.length;
      }
      sweepStop = doc.scanTs as bigint;
    }
  }

  // Entries the sweep starts take slots first; only fetch ready work for the
  // slots left over. Everything eligible, oldest first; work scheduled for
  // later sorts above the bound, so it's left alone without pinning the
  // cursor. Documents are read whole, but a partially-taken document is safe:
  // the cursor stops at its `segment`, and the inclusive re-read picks up the
  // entries left behind.
  const readyLimit = Math.max(0, startLimit - sweepStarts.length);
  const starts: Start[] = [];
  const readyStamps: bigint[] = [];
  if (readyLimit > 0) {
    const stream = ctx.db
      .query("pendingStart")
      .withIndex("segment", (q) =>
        q.gte("segment", incomingCursor).lt("segment", eligibleBefore),
      );
    scan: for await (const doc of stream) {
      const segment = doc.segment as bigint;
      // A document from before commit-timestamp ordering keeps its start time
      // in `segment`; recovering it here means `run` handles it like any
      // other not-yet-due entry and re-keys it as a timestamp.
      const legacyStartTime = legacyRunAt(segment);
      if (doc.scheduled !== true && legacyStartTime === undefined) {
        readyStamps.push(segment);
      }
      for (const workId of memberIds(doc)) {
        if (excluded.has(workId)) continue;
        if (starts.length >= readyLimit) break scan;
        starts.push({ _id: doc._id, workId, segment, legacyStartTime });
      }
    }
  }
  // The highest commit timestamp observed while building this batch. Every
  // source is the stamp of a transaction visible to this snapshot, so
  // anything committing later is stamped above it — the frontier the cursor
  // may advance to. `scheduled` documents are wall-clock-keyed and don't
  // count (their stamps do, via the sweep); the rest carry an observed commit
  // stamp in `segment` (an older version's tiny buckets are excluded with the
  // same check that recovers their start time).
  const cursorCeiling = [
    lastCommitTs,
    ...completions.map((c) => c.segment as bigint),
    ...cancelations.map((c) => c.segment as bigint),
    ...readyStamps,
    ...(sweepStop === undefined ? [] : [sweepStop]),
  ].reduce(maxBigint);

  return {
    completions,
    cancelations,
    starts,
    sweepStarts,
    sweepStop,
    cursorCeiling,
  };
}

/** The work queued in a pendingStart document. */
function memberIds(doc: {
  workIds?: Id<"work">[];
  workId?: Id<"work">;
}): Id<"work">[] {
  return doc.workIds ?? (doc.workId ? [doc.workId] : []);
}

/**
 * Remove entries from a queue document, deleting it once empty (also clearing
 * the deprecated single-entry field a ≤ 0.4.9 version may have written).
 */
async function removeFromPendingStart(
  ctx: MutationCtx,
  doc: Doc<"pendingStart">,
  workIds: Id<"work">[],
) {
  const remaining = memberIds(doc).filter((id) => !workIds.includes(id));
  if (remaining.length === 0) {
    await ctx.db.delete("pendingStart", doc._id);
  } else {
    await ctx.db.patch("pendingStart", doc._id, {
      workIds: remaining,
      workId: undefined,
    });
  }
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
        if (await rescheduleJob(ctx, work, console)) {
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
            // A pendingStart left pointing at it, if any, is dropped when the
            // segment scan reads it and finds the work gone.
            console.warn(`[main] ${workId} is gone, but trying to cancel`);
            return null;
          }
          // Ensure it doesn't retry — and doesn't start, if its pendingStart
          // is only reachable through the scan (the pointer can be missing on
          // entries older versions wrote; `handleStart` checks this flag).
          await ctx.db.patch("work", workId, { canceled: true });
          // Ensure it doesn't start: remove it from its queue document. The
          // pointer can be stale; check membership.
          const pendingStart = work.pendingStartId
            ? await ctx.db.get("pendingStart", work.pendingStartId)
            : null;
          if (
            pendingStart &&
            memberIds(pendingStart).includes(workId) &&
            !canceledWork.has(workId)
          ) {
            state.report.canceled++;
            await removeFromPendingStart(ctx, pendingStart, [workId]);
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

function maxBigint(a: bigint, b: bigint) {
  return a > b ? a : b;
}

/**
 * Re-keys entries that are visible but not due to sort at their start time,
 * so the cursor can pass them and they don't come back until they're actually
 * due. New-format entries are keyed at their start time and only become
 * visible once due, so this only handles the 100ms buckets older versions
 * wrote. The new key needs no clamping: a not-yet-due start time rounds up to
 * at least the end of the current millisecond, which no cursor ever reaches
 * (the scan's bound is exclusive, and the ceiling can only pull it lower).
 */
async function promoteScheduled(ctx: MutationCtx, notYet: Start[]) {
  await Promise.all(
    notYet.map(async ({ _id, legacyStartTime }) => {
      // A concurrent cancelation may have removed it.
      if (!(await ctx.db.get("pendingStart", _id))) return;
      await ctx.db.patch("pendingStart", _id, {
        segment: dueTimestamp(legacyStartTime!),
        // The key is now a wall-clock time, and `scheduled` is what records
        // that (so the cursor ceiling doesn't count it as a commit stamp).
        // No `scanTs`: written by the loop, it can't be out of order.
        scheduled: true,
      });
    }),
  );
}

/**
 * Starts pending work. Entries are removed from their queue documents as
 * they're handled — started, found gone, or found canceled — one patch (or
 * delete, when empty) per document.
 */
async function handleStart(
  ctx: MutationCtx,
  state: Doc<"internalState">,
  pending: Start[],
  console: Logger,
  { logLevel }: Config,
) {
  console.debug(`[main] scheduling ${pending.length} pending work`);
  const byDoc = new Map<Id<"pendingStart">, Start[]>();
  for (const entry of pending) {
    const entries = byDoc.get(entry._id);
    if (entries) entries.push(entry);
    else byDoc.set(entry._id, [entry]);
  }
  const starts: { work: Doc<"work">; lagMs: number }[] = [];
  for (const [docId, entries] of byDoc) {
    // Guard against a document a concurrent cancelation emptied.
    const doc = await ctx.db.get("pendingStart", docId);
    if (!doc) continue;
    const members = memberIds(doc);
    const removed: Id<"work">[] = [];
    for (const { workId, segment, legacyStartTime } of entries) {
      // A concurrent cancelation may have removed just this entry.
      if (!members.includes(workId)) continue;
      // Whatever happens below, the entry leaves the queue: nothing rescans
      // behind the cursor, so it must not be left unreadable.
      removed.push(workId);
      if (state.running.some((r) => r.workId === workId)) {
        console.error(`[main] ${workId} already running (skipping start)`);
        continue;
      }
      const work = await ctx.db.get("work", workId);
      if (!work) {
        console.error(`Trying to start, but work not found: ${workId}`);
        continue;
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
        continue;
      }
      starts.push({
        work,
        // `segment` round-trips to when this became eligible: the
        // scheduled start time, or the commit timestamp of the enqueue if
        // it was ready then. `legacyStartTime` only exists for an entry an
        // older version wrote, whose `segment` is a 100ms bucket rather
        // than a timestamp.
        lagMs: Date.now() - (legacyStartTime ?? fromTimestamp(segment)),
      });
    }
    await removeFromPendingStart(ctx, doc, removed);
  }

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
  // The pointer can be stale (its entry started); check before declaring a
  // duplicate.
  const existing = work.pendingStartId
    ? await ctx.db.get("pendingStart", work.pendingStartId)
    : null;
  if (existing && memberIds(existing).includes(work._id)) {
    // Not sure why this would ever happen, but ensure uniqueness explicitly.
    console.error(`[main] ${work._id} already in pendingStart so not retrying`);
    return false;
  }
  const backoffMs =
    work.retryBehavior.initialBackoffMs *
    Math.pow(work.retryBehavior.base, work.attempts - 1);
  const nextAttempt = withJitter(backoffMs);
  // The key needs no clamping against the cursor: it's strictly in the
  // future, so it rounds up to at least the end of the current millisecond,
  // which no cursor ever reaches — the scan's bound is exclusive and the
  // ceiling can only pull it lower. So unlike an enqueue, this can't land
  // behind the cursor and needs no `scanTs` for the sweep. `scheduled` still
  // marks the key as a wall-clock time, so the cursor ceiling doesn't count
  // it as an observed commit stamp.
  const segment = dueTimestamp(Date.now() + Math.max(nextAttempt, 1));
  const pendingStartId = await ctx.db.insert("pendingStart", {
    workIds: [work._id],
    segment,
    scheduled: true,
  });
  await ctx.db.patch("work", work._id, { pendingStartId });
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
  const state = await ctx.db.query("internalState").order("desc").first();
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
