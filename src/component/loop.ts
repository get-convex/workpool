import type { WithoutSystemFields } from "convex/server";
import { v } from "convex/values";
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
 * Called via ctx.runSnapshotQuery (no read dependency) for the hot path,
 * and via ctx.runQuery (with read dependency) before going idle/scheduled.
 * Cursors are used to skip tombstones from previously deleted rows.
 */
export const getPendingWork = internalQuery({
  args: {
    completionCursor: v.int64(),
    cancelationCursor: v.int64(),
    incomingCursor: v.int64(),
    currentSegment: v.int64(),
    startLimit: v.number(),
  },
  handler: async (
    ctx,
    {
      completionCursor,
      cancelationCursor,
      incomingCursor,
      currentSegment,
      startLimit,
    },
  ) => {
    const completions = await ctx.db
      .query("pendingCompletion")
      .withIndex("segment", (q) => q.gte("segment", completionCursor))
      .take(startLimit);
    const cancelations = await ctx.db
      .query("pendingCancelation")
      .withIndex("segment", (q) => q.gte("segment", cancelationCursor))
      .take(CANCELLATION_BATCH_SIZE);
    // Only fetch starts up to current segment (respects retry backoff).
    const starts =
      startLimit > 0
        ? await ctx.db
            .query("pendingStart")
            .withIndex("segment", (q) =>
              q
                .gte("segment", incomingCursor)
                .lte("segment", currentSegment),
            )
            .take(startLimit)
        : [];
    // Next future start, for scheduling when there's nothing to do now.
    const nextFutureStart = await ctx.db
      .query("pendingStart")
      .withIndex("segment", (q) => q.gt("segment", currentSegment))
      .first();
    return {
      completions,
      cancelations,
      starts,
      nextStartSegment: nextFutureStart?.segment ?? null,
    };
  },
});

// ── Main loop ─────────────────────────────────────────────────────────
// There should only ever be at most one of these scheduled or running.
export const main = internalMutation({
  args: { generation: v.int64() },
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
      await ctx.db.patch(runStatus._id, {
        state: { kind: "running" },
      });
    }

    const globals = await getGlobals(ctx);
    const console = createLogger(globals.logLevel);
    const segment = getCurrentSegment();

    // ── Snapshot read — no read dependency, no OCC conflicts. ──
    // Pass maxParallelism as startLimit so we fetch enough starts to
    // fill slots freed by completions processed in this same iteration.
    console.time("[main] snapshot read");
    const queryArgs = {
      completionCursor: state.segmentCursors.completion,
      cancelationCursor: state.segmentCursors.cancelation,
      incomingCursor: state.segmentCursors.incoming,
      currentSegment: segment,
      startLimit: globals.maxParallelism,
    };
    const work = await (ctx as any).runSnapshotQuery(
      internal.loop.getPendingWork,
      queryArgs,
    );
    const completed: Doc<"pendingCompletion">[] = work.completions;
    const canceled: Doc<"pendingCancelation">[] = work.cancelations;
    console.timeEnd("[main] snapshot read");

    // ── Process completions ──
    console.time("[main] pendingCompletion");
    const toCancel = await handleCompletions(ctx, state, completed, console);
    console.timeEnd("[main] pendingCompletion");

    // ── Process cancelations ──
    console.time("[main] pendingCancelation");
    await handleCancelation(ctx, state, canceled, console, toCancel);
    console.timeEnd("[main] pendingCancelation");

    // ── Recovery ──
    if (state.running.length === 0) {
      state.lastRecovery = segment;
    } else if (segment - state.lastRecovery >= RECOVERY_PERIOD_SEGMENTS) {
      await handleRecovery(ctx, state, console);
      state.lastRecovery = segment;
    }

    // ── Start new work ──
    // Slice to actual available capacity (completions may have freed slots).
    const actualCapacity = globals.maxParallelism - state.running.length;
    const pending: Doc<"pendingStart">[] = (
      work.starts as Doc<"pendingStart">[]
    ).slice(0, actualCapacity);
    console.time("[main] pendingStart");
    await handleStart(ctx, state, pending, console, globals);
    console.timeEnd("[main] pendingStart");

    // ── Periodic report ──
    if (Date.now() - state.report.lastReportTs >= MINUTE) {
      let lastReportTs = state.report.lastReportTs + MINUTE;
      if (Date.now() > lastReportTs + MINUTE / 2) {
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

    // ── Update cursors to skip tombstones on next scan ──
    const didWork =
      completed.length > 0 || canceled.length > 0 || pending.length > 0;
    if (didWork) {
      state.segmentCursors.completion = completed.at(-1)?.segment ?? segment;
      state.segmentCursors.cancelation =
        canceled.at(-1)?.segment ?? segment;
      if (pending.length > 0) {
        state.segmentCursors.incoming = pending.at(-1)!.segment;
      } else {
        state.segmentCursors.incoming = segment;
      }
    }

    await ctx.db.replace(state._id, state);

    // ── Schedule next iteration ──
    if (didWork) {
      // More work might have arrived while we were processing. Check again.
      await ctx.scheduler.runAfter(0, internal.loop.main, {
        generation: state.generation,
      });
      return;
    }

    // Nothing found in snapshot. Re-read with a real dependency (same args
    // for cache-hit efficiency) so a concurrent insert forces an OCC retry.
    console.debug("[main] no work — confirming with read dependency");
    const confirm = await (ctx as any).runQuery(
      internal.loop.getPendingWork,
      queryArgs,
    );
    if (
      confirm.completions.length > 0 ||
      confirm.cancelations.length > 0 ||
      confirm.starts.length > 0
    ) {
      await ctx.scheduler.runAfter(0, internal.loop.main, {
        generation: state.generation,
      });
      return;
    }

    // Cooldown: if we were recently active, keep checking before
    // transitioning to scheduled/idle.
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

    if (state.running.length > 0) {
      // Jobs are running but nothing new to process right now.
      // Schedule for the next future start or next recovery, whichever is sooner.
      const nextRecoverySegment = state.lastRecovery + RECOVERY_PERIOD_SEGMENTS;
      const target =
        confirm.nextStartSegment !== null
          ? min(confirm.nextStartSegment as bigint, nextRecoverySegment)
          : nextRecoverySegment;

      const scheduledId = await ctx.scheduler.runAt(
        boundScheduledTime(fromSegment(target), console),
        internal.loop.main,
        { generation: state.generation },
      );
      await ctx.db.patch(runStatus._id, {
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
    await ctx.db.patch(runStatus._id, {
      state: { kind: "idle", generation: state.generation },
    });
  },
});

// ── Handler functions ─────────────────────────────────────────────────

/**
 * Handles the completion of pending completions.
 * Accepts pre-fetched completion docs (from snapshot query).
 */
async function handleCompletions(
  ctx: MutationCtx,
  state: Doc<"internalState">,
  completed: Doc<"pendingCompletion">[],
  console: Logger,
) {
  const toCancel: CompleteJob[] = [];
  await Promise.all(
    completed.map(async (c) => {
      await ctx.db.delete(c._id);

      const running = state.running.find((r) => r.workId === c.workId);
      if (!running) {
        console.error(
          `[main] completing ${c.workId} but it's not in "running"`,
        );
        return;
      }
      if (c.retry) {
        const work = await ctx.db.get(c.workId);
        if (!work) {
          console.warn(`[main] ${c.workId} is gone, but trying to complete`);
          return;
        }
        const retried = await rescheduleJob(ctx, work, console);
        if (retried) {
          state.report.retries++;
          recordCompleted(console, work, "retrying", undefined);
        } else {
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
          await ctx.db.delete(_id);
          if (canceledWork.has(workId)) {
            console.error(`[main] ${workId} already canceled`);
            return null;
          }
          const work = await ctx.db.get(workId);
          if (!work) {
            console.warn(`[main] ${workId} is gone, but trying to cancel`);
            return null;
          }
          await ctx.db.patch(workId, { canceled: true });
          const pendingStart = await ctx.db
            .query("pendingStart")
            .withIndex("workId", (q) => q.eq("workId", workId))
            .unique();
          if (pendingStart && !canceledWork.has(workId)) {
            state.report.canceled++;
            await ctx.db.delete(pendingStart._id);
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
        const work = await ctx.db.get(r.workId);
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
          await ctx.db.delete(_id);
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
  const work = await ctx.db.get(workId);
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
    await ctx.db.insert("runStatus", { state: { kind: "running" } }),
  ))!;
}
// eslint-disable-next-line @typescript-eslint/no-unused-vars
const console = "THIS IS A REMINDER TO USE createLogger";
