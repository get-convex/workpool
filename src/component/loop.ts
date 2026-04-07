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
  min,
  max,
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

export const INITIAL_STATE: WithoutSystemFields<Doc<"internalState">> = {
  generation: 0n,
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

// ── Snapshot query functions ──────────────────────────────────────────
// These are called via ctx.runSnapshotQuery (no read dependency) or
// ctx.runQuery (with read dependency) from within the main mutation.

export const getPendingCompletions = internalQuery({
  args: {},
  handler: async (ctx) => {
    // Bounded by MAX_PARALLELISM — one completion per running job.
    return await ctx.db.query("pendingCompletion").collect();
  },
});

export const getPendingStarts = internalQuery({
  args: { upToSegment: v.int64(), limit: v.number() },
  handler: async (ctx, { upToSegment, limit }) => {
    return await ctx.db
      .query("pendingStart")
      .withIndex("segment", (q) => q.lte("segment", upToSegment))
      .take(limit);
  },
});

export const getPendingCancelations = internalQuery({
  args: { limit: v.number() },
  handler: async (ctx, { limit }) => {
    return await ctx.db.query("pendingCancelation").take(limit);
  },
});

/** Check if any of the three pending tables have work. */
export const hasPendingWork = internalQuery({
  args: { includeStarts: v.boolean() },
  handler: async (ctx, { includeStarts }) => {
    if (await ctx.db.query("pendingCompletion").first()) return true;
    if (await ctx.db.query("pendingCancelation").first()) return true;
    if (includeStarts && (await ctx.db.query("pendingStart").first()))
      return true;
    return false;
  },
});

/** Find the earliest segment across pending tables. */
export const getNextPendingSegment = internalQuery({
  args: { includeStarts: v.boolean() },
  handler: async (ctx, { includeStarts }) => {
    const completion = await ctx.db
      .query("pendingCompletion")
      .withIndex("segment")
      .first();
    const cancelation = await ctx.db
      .query("pendingCancelation")
      .withIndex("segment")
      .first();
    const start = includeStarts
      ? await ctx.db.query("pendingStart").withIndex("segment").first()
      : null;
    const segments = [
      completion?.segment,
      cancelation?.segment,
      start?.segment,
    ].filter((s): s is bigint => s != null);
    if (segments.length === 0) return null;
    return segments.reduce((a, b) => (a < b ? a : b));
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

    // ── Snapshot reads — no read dependency, no OCC conflicts. ──
    console.time("[main] snapshot reads");
    const completed: Doc<"pendingCompletion">[] =
      await (ctx as any).runSnapshotQuery(
        internal.loop.getPendingCompletions,
        {},
      );
    const canceled: Doc<"pendingCancelation">[] =
      await (ctx as any).runSnapshotQuery(
        internal.loop.getPendingCancelations,
        { limit: CANCELLATION_BATCH_SIZE },
      );
    const toSchedule = globals.maxParallelism - state.running.length;
    const pending: Doc<"pendingStart">[] =
      toSchedule > 0
        ? await (ctx as any).runSnapshotQuery(
            internal.loop.getPendingStarts,
            { upToSegment: segment, limit: toSchedule },
          )
        : [];
    console.timeEnd("[main] snapshot reads");

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

    await ctx.db.replace(state._id, state);

    // ── Schedule next iteration ──
    const didWork =
      completed.length > 0 || canceled.length > 0 || pending.length > 0;

    if (didWork) {
      // More work might have arrived while we were processing. Check again.
      await ctx.scheduler.runAfter(0, internal.loop.main, {
        generation: state.generation,
      });
      return;
    }

    if (state.running.length > 0) {
      // Jobs are running but nothing new to process right now.
      // Find the next time we should wake up.
      const nextPending: bigint | null = await (
        ctx as any
      ).runSnapshotQuery(internal.loop.getNextPendingSegment, {
        includeStarts: state.running.length < globals.maxParallelism,
      });
      const nextRecoverySegment = state.lastRecovery + RECOVERY_PERIOD_SEGMENTS;
      const target =
        nextPending !== null
          ? min(nextPending, nextRecoverySegment)
          : nextRecoverySegment;

      if (target <= segment + 1n) {
        // Imminent — run immediately.
        await ctx.scheduler.runAfter(0, internal.loop.main, {
          generation: state.generation,
        });
      } else {
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
      }
      return;
    }

    // Nothing running, nothing processed. Before going idle, take a real
    // read dependency so that a concurrent insert will conflict with us
    // and force a retry (ensuring we never miss work).
    console.debug("[main] nothing to do — taking read dependency before idle");
    const hasPending: boolean = await (ctx as any).runQuery(
      internal.loop.hasPendingWork,
      { includeStarts: true },
    );
    if (hasPending) {
      await ctx.scheduler.runAfter(0, internal.loop.main, {
        generation: state.generation,
      });
    } else {
      await ctx.db.patch(runStatus._id, {
        state: { kind: "idle", generation: state.generation },
      });
    }
  },
});

// Keep the export so that existing scheduled calls don't cause a missing
// function error, but this is no longer actively used.
export const updateRunStatus = internalMutation({
  args: { generation: v.int64(), segment: v.int64() },
  handler: async (ctx, { generation }) => {
    // Scheduling logic has been merged into main. Just kick main.
    await ctx.scheduler.runAfter(0, internal.loop.main, { generation });
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
