import type { WithoutSystemFields } from "convex/server";
import { v } from "convex/values";
import { runSnapshotQuery } from "./future.js";
import { internal } from "./_generated/api.js";
import type { Doc, Id } from "./_generated/dataModel.js";
import {
  internalMutation,
  internalQuery,
  type MutationCtx,
} from "./_generated/server.js";
import type { CompleteJob } from "./complete.js";
import {
  createLogger,
  DEFAULT_LOG_LEVEL,
  type Logger,
  type LogLevel,
} from "./logging.js";
import {
  boundScheduledTime,
  type Config,
  DEFAULT_MAX_PARALLELISM,
  fromSegment,
  getCurrentSegment,
  max,
  min,
  type RunResult,
  toSegment,
} from "./shared.js";
import { generateReport, recordCompleted, recordStarted } from "./stats.js";

const CANCELLATION_BATCH_SIZE = 64; // the only queue that can get unbounded.
const RECOVERY_BATCH_SIZE = 32;
const MS = 1;
const SECOND = 1000 * MS;
const MINUTE = 60 * SECOND;
const RECOVERY_THRESHOLD_MS = 5 * MINUTE; // attempt to recover jobs this old.
export const RECOVERY_PERIOD_SEGMENTS = toSegment(1 * MINUTE); // how often to check.
export const STATUS_COOLDOWN = 2 * SECOND;
export const COOLDOWN_CHECK_INTERVAL = 200 * MS;
// Buffer applied when querying with cursors. Transactions that started
// before ours may still be running and commit inserts at segments behind
// a previously advanced cursor — the buffer lets us pick those up.
const CURSOR_BUFFER_SEGMENTS = toSegment(30 * SECOND);

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

/**
 * Single query that returns everything the main loop needs to process.
 */
export const getPending = internalQuery({
  args: {
    completionCursor: v.int64(),
    cancelationCursor: v.int64(),
    incomingCursor: v.int64(),
    maxParallelism: v.number(),
    runningCount: v.number(),
  },
  handler: async (
    ctx,
    {
      completionCursor,
      cancelationCursor,
      incomingCursor,
      maxParallelism,
      runningCount,
    },
  ) => {
    const completions = await ctx.db
      .query("pendingCompletion")
      .withIndex("segment", (q) => q.gte("segment", completionCursor))
      .take(maxParallelism);
    const cancelations = await ctx.db
      .query("pendingCancelation")
      .withIndex("segment", (q) => q.gte("segment", cancelationCursor))
      .take(CANCELLATION_BATCH_SIZE);
    // Available slots after we process this batch's completions, plus 1
    // for the +1 trick (detect overflow vs. a future-scheduled retry).
    const startLimit = Math.max(
      0,
      maxParallelism - runningCount + completions.length,
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
  },
});

// There should only ever be at most one of these scheduled or running.
export const main = internalMutation({
  // `segment` is kept for backwards compatibility with in-flight scheduled
  // calls from before the upgrade — it's no longer used internally.
  args: { generation: v.int64(), segment: v.optional(v.int64()) },
  handler: async (ctx, { generation }) => {
    // State will be modified and patched at the end of the function.
    const state = await getOrCreateState(ctx);
    if (generation !== state.generation) {
      throw new Error(
        `generation mismatch: ${generation} !== ${state.generation}`,
      );
    }
    state.generation++;
    const runStatus = await getOrCreateRunningStatus(ctx);
    if (runStatus.state.kind !== "running") {
      await ctx.db.patch("runStatus", runStatus._id, {
        state: { kind: "running" },
      });
    }

    const globals = await getGlobals(ctx);
    const console = createLogger(globals.logLevel);
    const segment = getCurrentSegment();

    // Pass maxParallelism + runningCount so the query bounds each batch to
    // what we can actually consume this iteration. Apply CURSOR_BUFFER_SEGMENTS
    // so we still pick up out-of-order inserts that landed behind the cursor
    // since our last scan.
    const queryArgs = {
      completionCursor:
        state.segmentCursors.completion - CURSOR_BUFFER_SEGMENTS,
      cancelationCursor:
        state.segmentCursors.cancelation - CURSOR_BUFFER_SEGMENTS,
      incomingCursor: state.segmentCursors.incoming - CURSOR_BUFFER_SEGMENTS,
      maxParallelism: globals.maxParallelism,
      runningCount: state.running.length,
    };

    // Snapshot read — no read dependency, no OCC conflicts.
    console.time("[main] getPending");
    const { allStarts, cancelations, completions } = await runSnapshotQuery(
      internal.loop.getPending,
      queryArgs,
    );
    const toStart = allStarts.filter((s) => s.segment <= segment);
    console.timeEnd("[main] getPending");

    console.time("[main] pendingCompletion");
    const toCancel = await handleCompletions(ctx, state, completions, console);
    console.timeEnd("[main] pendingCompletion");

    console.time("[main] pendingCancelation");
    await handleCancelation(ctx, state, cancelations, console, toCancel);
    console.timeEnd("[main] pendingCancelation");

    if (state.running.length === 0) {
      // If there's nothing active, reset lastRecovery.
      state.lastRecovery = segment;
    } else if (segment - state.lastRecovery >= RECOVERY_PERIOD_SEGMENTS) {
      // Otherwise schedule recovery for any old jobs.
      await handleRecovery(ctx, state, console);
      state.lastRecovery = segment;
    }

    // ── Start new work ──
    // Slice to actual available capacity (completions may have freed slots).
    // Guard against negative numbers in case running.length > maxParallelism.
    const actualCapacity = globals.maxParallelism - state.running.length;
    const pending: Doc<"pendingStart">[] =
      actualCapacity > 0 ? toStart.slice(0, actualCapacity) : [];
    console.time("[main] pendingStart");
    await handleStart(ctx, state, pending, console, globals);
    console.timeEnd("[main] pendingStart");

    if (Date.now() - state.report.lastReportTs >= MINUTE) {
      // If minute rollover since last report, log report.
      // Try to avoid clock skew by shifting by a minute.
      let lastReportTs = state.report.lastReportTs + MINUTE;
      if (Date.now() > lastReportTs + MINUTE / 2) {
        // It's been a while, let's start fresh.
        lastReportTs = Date.now();
      }
      await generateReport(ctx, console, state, globals);
      state.report = {
        completed: 0,
        succeeded: 0,
        failed: 0,
        retries: 0,
        canceled: 0,
        lastReportTs,
      };
    }

    // Advance cursors to skip tombstones on next scan. Only do this when
    // we actually did work — the cursor doubles as the cooldown signal
    // ("how long since we last processed something").
    const didWork =
      completions.length > 0 || cancelations.length > 0 || pending.length > 0;
    if (didWork) {
      state.segmentCursors.completion = completions.at(-1)?.segment ?? segment;
      state.segmentCursors.cancelation =
        cancelations.at(-1)?.segment ?? segment;
      if (pending.length > 0) {
        state.segmentCursors.incoming = pending.at(-1)!.segment;
      } else if (actualCapacity > 0) {
        // We have no more pending work, update to now
        state.segmentCursors.incoming = segment;
      }
    }

    await ctx.db.replace("internalState", state._id, state);

    // ── Schedule next iteration ──
    if (didWork) {
      // More work might have arrived while we were processing. Check again.
      await ctx.scheduler.runAfter(0, internal.loop.main, {
        generation: state.generation,
      });
      return;
    }

    // Nothing found in snapshot. Re-read with a real dependency so a
    // concurrent insert forces an OCC retry. Override runningCount so it
    // reflects post-recovery state — otherwise a stale count can leave
    // startLimit pinned to 0 and miss now-runnable pendingStart.
    console.debug("[main] no work — confirming with read dependency");
    const confirm = await ctx.runQuery(internal.loop.getPending, {
      ...queryArgs,
      runningCount: state.running.length,
    });
    const confirmStarts = confirm.allStarts;
    const confirmStartsNow = confirmStarts.filter((s) => s.segment <= segment);
    const confirmFuture = confirmStarts.find((s) => s.segment > segment);
    if (
      confirm.completions.length > 0 ||
      confirm.cancelations.length > 0 ||
      confirmStartsNow.length > 0
    ) {
      await ctx.scheduler.runAfter(0, internal.loop.main, {
        generation: state.generation,
      });
      return;
    }

    // Cooldown: if any cursor was active within STATUS_COOLDOWN, stay running.
    const { incoming, completion, cancelation } = state.segmentCursors;
    const latestCursor = fromSegment(
      max(incoming, max(completion, cancelation)),
    );
    if (Date.now() - latestCursor < STATUS_COOLDOWN) {
      const remaining = STATUS_COOLDOWN - (Date.now() - latestCursor);
      console.debug(
        `[main] cooldown: ${remaining}ms remaining, checking again in ${COOLDOWN_CHECK_INTERVAL}ms`,
      );
      await ctx.scheduler.runAt(
        Date.now() + COOLDOWN_CHECK_INTERVAL,
        internal.loop.main,
        { generation: state.generation },
      );
      return;
    }

    if (state.running.length > 0 || confirmFuture) {
      // Jobs are running and/or there's future-scheduled work.
      // Schedule for the future start or next recovery, whichever is sooner.
      const nextRecoverySegment = state.lastRecovery + RECOVERY_PERIOD_SEGMENTS;
      const target = confirmFuture
        ? min(confirmFuture.segment, nextRecoverySegment)
        : nextRecoverySegment;

      const scheduledId = await ctx.scheduler.runAt(
        boundScheduledTime(fromSegment(target), console),
        internal.loop.main,
        { generation: state.generation },
      );
      await ctx.db.patch("runStatus", runStatus._id, {
        state: {
          kind: "scheduled",
          scheduledId,
          saturated: state.running.length >= globals.maxParallelism,
          generation: state.generation,
          segment: target,
        },
      });
      return;
    }

    // Nothing to do — go idle.
    await ctx.db.patch("runStatus", runStatus._id, {
      state: { kind: "idle", generation: state.generation },
    });
  },
});

/**
 * @deprecated Forwarder for in-flight scheduled calls from before the
 * upgrade. The scheduling logic has been merged into `main`.
 */
export const updateRunStatus = internalMutation({
  args: { generation: v.int64(), segment: v.int64() },
  handler: async (ctx, { generation }) => {
    await ctx.scheduler.runAfter(0, internal.loop.main, { generation });
  },
});

/**
 * Handles the completion of pending completions.
 * This only processes work that succeeded or failed, not canceled.
 * Accepts pre-fetched completion docs (from snapshot query).
 */
async function handleCompletions(
  ctx: MutationCtx,
  state: Doc<"internalState">,
  completed: Doc<"pendingCompletion">[],
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
 * Handles cancelation. Accepts pre-fetched cancelation docs.
 */
async function handleCancelation(
  ctx: MutationCtx,
  state: Doc<"internalState">,
  canceled: Doc<"pendingCancelation">[],
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
  for (let i = 0; i < jobs.length; i += RECOVERY_BATCH_SIZE) {
    const batch = jobs.slice(i, i + RECOVERY_BATCH_SIZE);
    await ctx.scheduler.runAfter(0, internal.recovery.recover, { jobs: batch });
  }
}

/**
 * Starts pending work. Accepts pre-fetched pendingStart docs.
 */
async function handleStart(
  ctx: MutationCtx,
  state: Doc<"internalState">,
  pending: Doc<"pendingStart">[],
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
  const startTime = boundScheduledTime(Date.now() + nextAttempt, console);
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

async function getGlobals(ctx: MutationCtx) {
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
  console.error("No internalState in running loop! Re-creating empty one...");
  return (await ctx.db.get(
    "internalState",
    await ctx.db.insert("internalState", INITIAL_STATE),
  ))!;
}

async function getOrCreateRunningStatus(ctx: MutationCtx) {
  const runStatus = await ctx.db.query("runStatus").unique();
  if (runStatus) return runStatus;
  const globals = await getGlobals(ctx);
  const console = createLogger(globals.logLevel);
  console.error("No runStatus in running loop! Re-creating one...");
  return (await ctx.db.get(
    "runStatus",
    await ctx.db.insert("runStatus", { state: { kind: "running" } }),
  ))!;
}
// eslint-disable-next-line @typescript-eslint/no-unused-vars
const console = "THIS IS A REMINDER TO USE createLogger";
