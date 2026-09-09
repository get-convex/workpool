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
  eligibilityBound,
  fromTimestamp,
  fromSegment,
  maxBigint,
  MINUTE,
  MIN_TIMESTAMP,
  SECOND,
  snapshotTs,
  toTimestamp,
  vResult,
} from "./shared.js";
import { generateReport, recordCompleted, recordStarted } from "./stats.js";
import { findPendingStart } from "./pendingStart.js";

const CANCELLATION_BATCH_SIZE = 64; // the only queue that can get unbounded.
const RECOVERY_BATCH_SIZE = 32;
const START_BATCH_SIZE = 32;
// Bound per-iteration work to keep loop latency low.
const MAIN_BATCH_SIZE = 64;
const RECOVERY_THRESHOLD_MS = 5 * MINUTE; // attempt to recover jobs this old.
export const RECOVERY_PERIOD_NS = toTimestamp(MINUTE); // how often to check.
// Keep an idle loop warm briefly to avoid status churn under light traffic.
export const STATUS_COOLDOWN = 2 * SECOND;
// At full capacity, keep batch-worker's status stable longer so enqueue pings
// generally observe `running` and no-op instead of racing an idle transition.
export const SATURATED_STATUS_COOLDOWN = 10 * SECOND;
export const COOLDOWN_CHECK_INTERVAL = 200;

export const INITIAL_STATE: WithoutSystemFields<Doc<"internalState">> = {
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

// batch-worker owns loop scheduling, generation checks, and liveness recovery.

const vCompletion = v.object({
  pendingId: v.id("pendingCompletion"),
  workId: v.id("work"),
  runResult: vResult,
  retry: v.boolean(),
  segment: v.int64(),
});
type Completion = Infer<typeof vCompletion>;

const vCancelation = v.object({
  pendingId: v.id("pendingCancelation"),
  workId: v.id("work"),
  segment: v.int64(),
});
type Cancelation = Infer<typeof vCancelation>;

const vStart = v.object({
  pendingId: v.id("pendingStart"),
  workId: v.id("work"),
  segment: v.int64(),
});
type Start = Infer<typeof vStart>;

// batch-worker calls getBatch and run in one transaction, sharing a snapshot.
const batchFields = {
  upgrade: v.optional(v.object({ starts: v.array(vStart), done: v.boolean() })),
  // Whether this iteration should run the periodic work-recovery scan.
  recovery: v.boolean(),
  completions: v.array(vCompletion),
  cancelations: v.array(vCancelation),
  starts: v.array(vStart),
  // Due entries behind the incoming cursor, and the last inspected scanTs.
  sweepStarts: v.array(vStart),
  sweepStop: v.optional(v.int64()),
};
type Batch = Infer<ReturnType<typeof v.object<typeof batchFields>>>;

/** Return a batch or idle hints. batch-worker confirms idle with a tracked read. */
export const getBatch = internalQuery({
  args: { name: v.string() },
  returns: vBatchResult(v.object(batchFields)),
  handler: async (ctx): Promise<BatchResult<Batch>> => {
    const globals = await getGlobals(ctx);
    const state = await ctx.db.query("internalState").order("desc").first();
    if (state?.segmentCursors.sweep === undefined) {
      const docs = await ctx.db
        .query("pendingStart")
        .withIndex("segment", (q) => q.lt("segment", MIN_TIMESTAMP))
        .take(MAIN_BATCH_SIZE);
      if (state || docs.length > 0)
        return {
          kind: "work",
          batch: {
            recovery: false,
            completions: [],
            cancelations: [],
            starts: [],
            sweepStarts: [],
            upgrade: {
              starts: docs.map((doc) => ({
                pendingId: doc._id,
                workId: doc.workId,
                segment: doc.segment as bigint,
              })),
              done: docs.length < MAIN_BATCH_SIZE,
            },
          },
        };
    }
    const running = state?.running ?? INITIAL_STATE.running;
    const cursors = state?.segmentCursors ?? INITIAL_STATE.segmentCursors;
    const lastRecovery = state?.lastRecovery ?? INITIAL_STATE.lastRecovery;
    const nowTs = toTimestamp(Date.now());

    // Periodically recover stuck workers; pending queues use their own cursors.
    const isRecoveryIter =
      running.length > 0 && nowTs - lastRecovery >= RECOVERY_PERIOD_NS;

    const { starts, sweepStarts, sweepStop, cancelations, completions } =
      await queryPending(ctx, {
        completionCursor: cursors.completion,
        cancelationCursor: cursors.cancelation,
        incomingCursor: cursors.incoming,
        sweepCursor: cursors.sweep ?? 0n,
        maxParallelism: globals.maxParallelism,
        runningCount: running.length,
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
        recovery: isRecoveryIter,
        completions: completions.map((c) => ({
          pendingId: c._id,
          workId: c.workId,
          runResult: c.runResult,
          retry: c.retry,
          segment: c.segment as bigint,
        })),
        cancelations: cancelations.map((c) => ({
          pendingId: c._id,
          workId: c.workId,
          segment: c.segment as bigint,
        })),
        starts,
        sweepStarts,
        sweepStop,
      };
      return { kind: "work" as const, batch };
    }

    // Wake for the next scheduled start or recovery scan. Enqueues and
    // completions can wake us sooner, including when all slots are occupied.
    const futureStart = await ctx.db
      .query("pendingStart")
      .withIndex("segment", (q) => q.gt("segment", eligibilityBound()))
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
    return {
      kind: "idle" as const,
      cooldownMs:
        running.length >= globals.maxParallelism
          ? SATURATED_STATUS_COOLDOWN
          : STATUS_COOLDOWN,
      pollIntervalMs: COOLDOWN_CHECK_INTERVAL,
      ...(timeoutMs !== undefined ? { timeoutMs } : {}),
    };
  },
});

/** Process a batch and persist its cursors. Returning null keeps draining. */
export const run = internalMutation({
  args: batchFields,
  returns: v.null(),
  handler: async (ctx, batch) => {
    const state = await getOrCreateState(ctx);
    if (batch.upgrade) {
      // Repair old pointers and keys once, before normal cursors can pass them.
      const snapshot = snapshotTs();
      for (const start of batch.upgrade.starts) {
        const work = await ctx.db.get("work", start.workId);
        if (!work) {
          await ctx.db.delete("pendingStart", start.pendingId);
          continue;
        }
        await ctx.db.patch("work", work._id, {
          pendingStartId: start.pendingId,
        });
        await ctx.db.patch("pendingStart", start.pendingId, {
          segment: maxBigint(toTimestamp(fromSegment(start.segment)), snapshot),
        });
      }
      await ctx.db.patch("internalState", state._id, {
        segmentCursors: batch.upgrade.done
          ? { ...INITIAL_STATE.segmentCursors }
          : { ...state.segmentCursors, sweep: undefined },
      });
      return null;
    }
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

    // Merge the segment scan and sweep in eligibility order.
    const eligible = [...batch.starts, ...batch.sweepStarts].sort((a, b) =>
      a.segment < b.segment ? -1 : a.segment > b.segment ? 1 : 0,
    );

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
    // Advance only across handled entries, capped at the snapshot so racing
    // commits remain ahead. Inclusive reads retain entries sharing a timestamp.
    const handled = new Set(pending.map((s) => s.pendingId));
    const snapshot = snapshotTs();
    for (const start of batch.starts) {
      if (!handled.has(start.pendingId)) break;
      state.segmentCursors.incoming =
        start.segment < snapshot ? start.segment : snapshot;
    }
    // Advance the sweep only after handling every entry it selected.
    // Entries left at the boundary stamp are revisited by an inclusive read.
    if (
      batch.sweepStop !== undefined &&
      batch.sweepStarts.every((s) => handled.has(s.pendingId))
    ) {
      state.segmentCursors.sweep = maxBigint(
        state.segmentCursors.sweep ?? 0n,
        batch.sweepStop,
      );
    }
    await ctx.db.replace("internalState", state._id, state);
    return null;
  },
});

// Bound sweep reads per iteration.
const SWEEP_DOC_BATCH = 256;

/** Read the three pending tables the loop processes. */
async function queryPending(
  ctx: QueryCtx,
  {
    completionCursor,
    cancelationCursor,
    incomingCursor,
    sweepCursor,
    maxParallelism,
    runningCount,
  }: {
    completionCursor: bigint;
    cancelationCursor: bigint;
    incomingCursor: bigint;
    sweepCursor: bigint;
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

  // Rescue scheduled entries that committed behind the incoming cursor.
  // Revisit only that subset at the boundary scanTs, then inspect later stamps.
  // The compound index sorts behind-cursor entries first within each stamp,
  // so stopping midway is safe and future entries need not be rescanned.
  const sweepStarts: Start[] = [];
  let sweepStop: bigint | undefined;
  {
    let docs = 0;
    const take = (doc: Doc<"pendingStart">): "more" | "stop" => {
      const segment = doc.segment as bigint;
      if (!excluded.has(doc.workId)) {
        if (sweepStarts.length >= startLimit) return "stop";
        sweepStarts.push({ pendingId: doc._id, workId: doc.workId, segment });
      }
      return ++docs >= SWEEP_DOC_BATCH ? "stop" : "more";
    };

    // (1) Out-of-order entries left at the boundary stamp. Nothing at or above
    // the cursor is read, so a bulk enqueue sharing this stamp costs nothing.
    let boundaryDone = true;
    for await (const doc of ctx.db
      .query("pendingStart")
      .withIndex("scanTs", (q) =>
        q.eq("scanTs", sweepCursor).lt("segment", incomingCursor),
      )) {
      if (take(doc) === "stop") {
        boundaryDone = false;
        break;
      }
    }

    // (2) Later stamps. Reaching one is what lets the cursor move off the
    // boundary; until then it stays put and (1) repeats, which is cheap.
    if (boundaryDone) {
      for await (const doc of ctx.db
        .query("pendingStart")
        .withIndex("scanTs", (q) => q.gt("scanTs", sweepCursor))) {
        const scanTs = doc.scanTs as bigint;
        const behind = (doc.segment as bigint) < incomingCursor;
        // Not behind: the segment scan owns it. Pass over it so the cursor can
        // advance, without reading its entries.
        if (behind && take(doc) === "stop") break;
        sweepStop = scanTs;
        if (!behind && ++docs >= SWEEP_DOC_BATCH) break;
      }
    }
  }

  // Entries the sweep starts take slots first; only fetch ready work for the
  // slots left over. Everything eligible, oldest first. Inclusive reads retain
  // entries sharing a timestamp when capacity cuts a batch short.
  const readyLimit = Math.max(0, startLimit - sweepStarts.length);
  const starts: Start[] = [];
  if (readyLimit > 0) {
    const stream = ctx.db
      .query("pendingStart")
      .withIndex("segment", (q) =>
        q.gte("segment", incomingCursor).lte("segment", eligibilityBound()),
      );
    for await (const doc of stream) {
      const segment = doc.segment as bigint;
      if (excluded.has(doc.workId)) continue;
      if (starts.length >= readyLimit) break;
      starts.push({
        pendingId: doc._id,
        workId: doc.workId,
        segment,
      });
    }
  }
  return { completions, cancelations, starts, sweepStarts, sweepStop };
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
      await ctx.db.delete("pendingCompletion", c.pendingId);

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
  const canceledWork = new Set<Id<"work">>();
  const jobs: CompleteJob[] = [...toCancel];
  await Promise.all(
    canceled.map(async ({ pendingId, workId }) => {
      if (!(await ctx.db.get("pendingCancelation", pendingId))) return;
      await ctx.db.delete("pendingCancelation", pendingId);
      if (canceledWork.has(workId)) {
        console.error(`[main] ${workId} already canceled`);
        return;
      }
      canceledWork.add(workId);
      const work = await ctx.db.get("work", workId);
      if (!work) {
        console.warn(`[main] ${workId} is gone, but trying to cancel`);
        return;
      }
      // Prevent retries even if the work is already running.
      await ctx.db.patch("work", workId, { canceled: true });
      const pendingStart = await findPendingStart(ctx, work);
      if (!pendingStart) return;
      await ctx.db.delete("pendingStart", pendingStart._id);
      state.report.canceled++;
      jobs.push({
        workId,
        runResult: { kind: "canceled" },
        attempt: work.attempts,
      });
    }),
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

/** Remove handled queue entries and start eligible work. */
async function handleStart(
  ctx: MutationCtx,
  state: Doc<"internalState">,
  pending: Start[],
  console: Logger,
  { logLevel }: Config,
) {
  console.debug(`[main] scheduling ${pending.length} pending work`);
  const entries = await Promise.all(
    pending.map(async (entry) => {
      const [doc, work] = await Promise.all([
        ctx.db.get("pendingStart", entry.pendingId),
        ctx.db.get("work", entry.workId),
      ]);
      return { ...entry, doc, work };
    }),
  );
  const starts: { work: Doc<"work">; lagMs: number }[] = [];
  for (const { pendingId, workId, segment, doc, work } of entries) {
    if (!doc) continue;
    await ctx.db.delete("pendingStart", pendingId);
    if (state.running.some((r) => r.workId === workId)) {
      console.error(`[main] ${workId} already running (skipping start)`);
      continue;
    }
    if (!work) {
      console.error(`Trying to start, but work not found: ${workId}`);
      continue;
    }
    if (work.canceled) {
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
    if (work.pendingStartId === undefined) {
      await ctx.db.patch("work", workId, { pendingStartId: pendingId });
    }
    starts.push({
      work,
      lagMs: Date.now() - fromTimestamp(segment),
    });
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
  if (await findPendingStart(ctx, work)) {
    // Not sure why this would ever happen, but ensure uniqueness explicitly.
    console.error(`[main] ${work._id} already in pendingStart so not retrying`);
    return false;
  }
  const backoffMs =
    work.retryBehavior.initialBackoffMs *
    Math.pow(work.retryBehavior.base, work.attempts - 1);
  const nextAttempt = withJitter(backoffMs);
  // Raised to at least the snapshot, which the cursor may reach this run, so
  // the retry can't land behind it; a backoff shorter than the clocks' skew
  // just starts next iteration.
  const segment = maxBigint(
    toTimestamp(Date.now() + nextAttempt),
    snapshotTs(),
  );
  const pendingStartId = await ctx.db.insert("pendingStart", {
    workId: work._id,
    segment,
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
