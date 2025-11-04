import type { WithoutSystemFields } from "convex/server";
import { v } from "convex/values";
import { internal } from "./_generated/api.js";
import type { Doc, Id } from "./_generated/dataModel.js";
import { internalMutation, type MutationCtx } from "./_generated/server.js";
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
  getNextSegment,
  max,
  type RunResult,
  toSegment,
} from "./shared.js";
import { generateReport, recordCompleted, recordStarted } from "./stats.js";

const CANCELLATION_BATCH_SIZE = 64; // the only queue that can get unbounded.
const SECOND = 1000;
const MINUTE = 60 * SECOND;
const RECOVERY_THRESHOLD_MS = 5 * MINUTE; // attempt to recover jobs this old.
export const RECOVERY_PERIOD_SEGMENTS = toSegment(1 * MINUTE); // how often to check.
const CURSOR_BUFFER_SEGMENTS = toSegment(30 * SECOND); // buffer for cursor updates.
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

// There should only ever be at most one of these scheduled or running.
export const main = internalMutation({
  args: { generation: v.int64(), segment: v.int64() },
  handler: async (ctx, { generation, segment }) => {
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
    const delayMs = Date.now() - fromSegment(segment);
    console.debug(`[main] generation ${generation} behind: ${delayMs}ms`);

    // Read pendingCompletions, including retry handling.
    console.time("[main] pendingCompletion");
    const toCancel = await handleCompletions(ctx, state, segment, console);
    console.timeEnd("[main] pendingCompletion");

    // Read pendingCancelation, deleting from pendingStart. If it's still running, queue to cancel.
    console.time("[main] pendingCancelation");
    await handleCancelation(ctx, state, segment, console, toCancel);
    console.timeEnd("[main] pendingCancelation");

    if (state.running.length === 0) {
      // If there's nothing active, reset lastRecovery.
      state.lastRecovery = segment;
    } else if (segment - state.lastRecovery >= RECOVERY_PERIOD_SEGMENTS) {
      // Otherwise schedule recovery for any old jobs.
      await handleRecovery(ctx, state, console);
      state.lastRecovery = segment;
    }

    // Read pendingStart up to max capacity. Update the config, and incomingSegmentCursor.
    console.time("[main] pendingStart");
    await handleStart(ctx, state, segment, console, globals);
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

    await ctx.db.replace(state._id, state);
    await ctx.scheduler.runAfter(0, internal.loop.updateRunStatus, {
      generation: state.generation,
      segment,
    });
    // TODO: if there were more cancellations, schedule main directly.
  },
});

export const updateRunStatus = internalMutation({
  args: { generation: v.int64(), segment: v.int64() },
  handler: async (ctx, { generation, segment }) => {
    const globals = await getGlobals(ctx);
    const console = createLogger(globals.logLevel);
    const maxParallelism = globals.maxParallelism;
    const state = await getOrCreateState(ctx);
    if (generation !== state.generation) {
      throw new Error(
        `generation mismatch: ${generation} !== ${state.generation}`,
      );
    }

    console.time("[updateRunStatus] outstandingCancelations");
    const outstandingCancelations = await getNextUp(ctx, "pendingCancelation", {
      start: state.segmentCursors.cancelation,
      end: segment,
    });
    console.timeEnd("[updateRunStatus] outstandingCancelations");
    if (outstandingCancelations) {
      await ctx.scheduler.runAfter(0, internal.loop.main, {
        generation,
        segment,
      });
      return;
    }

    // TODO: check for current segment (or from args) first, to avoid OCCs.
    console.time("[updateRunStatus] nextSegmentIsActionable");
    const nextSegment = max(segment + 1n, getCurrentSegment());
    const nextIsActionable = await nextSegmentIsActionable(
      ctx,
      state,
      maxParallelism,
      nextSegment,
    );
    console.timeEnd("[updateRunStatus] nextSegmentIsActionable");

    if (nextIsActionable) {
      await ctx.scheduler.runAt(
        boundScheduledTime(fromSegment(nextSegment), console),
        internal.loop.main,
        {
          generation,
          segment: nextSegment,
        },
      );
      return;
    }

    console.time("[updateRunStatus] oldSegmentIsActionable");
    const [oldIsActionable, cursors] = await oldSegmentIsActionable(
      ctx,
      state,
      maxParallelism,
    );
    console.timeEnd("[updateRunStatus] oldSegmentIsActionable");

    if (oldIsActionable) {
      await ctx.db.patch(state._id, {
        segmentCursors: {
          ...state.segmentCursors,
          ...cursors,
        },
      });
      await ctx.scheduler.runAfter(0, internal.loop.main, {
        generation,
        segment: getCurrentSegment(),
      });
      return;
    }

    //  Find next actionable segment (min next segment).
    console.time("[updateRunStatus] findNextSegment");
    const actionableTables: (
      | "pendingCompletion"
      | "pendingCancelation"
      | "pendingStart"
    )[] = ["pendingCompletion", "pendingCancelation"];
    if (state.running.length < maxParallelism) {
      actionableTables.push("pendingStart");
    }
    const docs = await Promise.all(
      actionableTables.map(async (tableName) =>
        getNextUp(ctx, tableName, { start: nextSegment }),
      ),
    );
    console.timeEnd("[updateRunStatus] findNextSegment");
    let targetSegment = docs.map((d) => d?.segment).sort()[0];
    const runStatus = await getOrCreateRunningStatus(ctx);
    const saturated = state.running.length >= maxParallelism;
    if (targetSegment !== undefined || state.running.length > 0) {
      // If there's something to do, schedule for next actionable segment.
      // Or the next recovery, whichever comes first.
      const nextRecoverySegment = state.lastRecovery + RECOVERY_PERIOD_SEGMENTS;
      if (!targetSegment || targetSegment > nextRecoverySegment) {
        targetSegment = nextRecoverySegment;
      }
      const scheduledId = await ctx.scheduler.runAt(
        boundScheduledTime(fromSegment(targetSegment), console),
        internal.loop.main,
        { generation, segment: targetSegment },
      );
      if (targetSegment > getNextSegment()) {
        await ctx.db.patch(runStatus._id, {
          state: {
            kind: "scheduled",
            scheduledId,
            saturated,
            generation,
            segment: targetSegment,
          },
        });
      } else {
        console.debug(
          `[updateRunStatus] staying running because it's the next segment`,
        );
      }
      return;
    }
    // There seems to be nothing in the future to do, so go idle.
    await ctx.db.patch(runStatus._id, {
      state: { kind: "idle", generation },
    });
  },
});

async function nextSegmentIsActionable(
  ctx: MutationCtx,
  state: Doc<"internalState">,
  maxParallelism: number,
  end: bigint,
): Promise<boolean> {
  // First, try with our cursor range, up to end.
  if (
    await getNextUp(ctx, "pendingCancelation", {
      start: state.segmentCursors.cancelation,
      end,
    })
  ) {
    return true;
  }
  if (
    await getNextUp(ctx, "pendingCompletion", {
      start: state.segmentCursors.completion,
      end,
    })
  ) {
    return true;
  }
  if (state.running.length < maxParallelism) {
    if (
      await getNextUp(ctx, "pendingStart", {
        start: state.segmentCursors.incoming,
        end,
      })
    ) {
      return true;
    }
  }
  return false;
}

async function oldSegmentIsActionable(
  ctx: MutationCtx,
  state: Doc<"internalState">,
  maxParallelism: number,
): Promise<
  [boolean, { completion?: bigint; cancelation?: bigint; incoming?: bigint }]
> {
  // Next, we look for out-of-order additions we may have missed.
  const oldCompletion = await getNextUp(ctx, "pendingCompletion", {
    end: state.segmentCursors.completion,
  });
  if (oldCompletion) {
    return [true, { completion: oldCompletion.segment }];
  }
  const oldCancelation = await getNextUp(ctx, "pendingCancelation", {
    end: state.segmentCursors.cancelation,
  });
  if (oldCancelation) {
    return [true, { cancelation: oldCancelation.segment }];
  }
  if (state.running.length < maxParallelism) {
    const oldStart = await getNextUp(ctx, "pendingStart", {
      end: state.segmentCursors.incoming,
    });
    if (oldStart) {
      return [true, { incoming: oldStart.segment }];
    }
  }
  return [false, {}];
}

// Fetch the next item. If only one of start & end are provided, it's exclusive.
async function getNextUp(
  ctx: MutationCtx,
  table: "pendingCompletion" | "pendingCancelation" | "pendingStart",
  range: { start?: bigint; end?: bigint },
) {
  return ctx.db
    .query(table)
    .withIndex("segment", (q) =>
      range.start !== undefined
        ? range.end !== undefined
          ? q
              .gte("segment", range.start - CURSOR_BUFFER_SEGMENTS)
              .lte("segment", range.end)
          : q.gt("segment", range.start - CURSOR_BUFFER_SEGMENTS)
        : range.end !== undefined
          ? q.lt("segment", range.end)
          : q,
    )
    .first();
}

/**
 * Handles the completion of pending completions.
 * This only processes work that succeeded or failed, not canceled.
 */
async function handleCompletions(
  ctx: MutationCtx,
  state: Doc<"internalState">,
  segment: bigint,
  console: Logger,
) {
  const startSegment = state.segmentCursors.completion - CURSOR_BUFFER_SEGMENTS;
  // This won't be too many because the jobs all correspond to being scheduled
  // by a single main (the previous one), so they're limited by MAX_PARALLELISM.
  const completed = await ctx.db
    .query("pendingCompletion")
    .withIndex("segment", (q) =>
      q.gte("segment", startSegment).lte("segment", segment),
    )
    .collect();
  state.segmentCursors.completion = segment;
  // Completions that were going to be retried but have since been canceled.
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
        // Only check for work if it's going to be retried.
        const work = await ctx.db.get(c.workId);
        if (!work) {
          console.warn(`[main] ${c.workId} is gone, but trying to complete`);
          return;
        }
        const retried = await rescheduleJob(ctx, work, console);
        if (retried) {
          state.report.retries++;
          recordCompleted(console, work, "retrying");
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

async function handleCancelation(
  ctx: MutationCtx,
  state: Doc<"internalState">,
  segment: bigint,
  console: Logger,
  toCancel: CompleteJob[],
) {
  const start = state.segmentCursors.cancelation - CURSOR_BUFFER_SEGMENTS;
  const canceled = await ctx.db
    .query("pendingCancelation")
    .withIndex("segment", (q) =>
      q.gte("segment", start).lte("segment", segment),
    )
    .take(CANCELLATION_BATCH_SIZE);
  state.segmentCursors.cancelation = canceled.at(-1)?.segment ?? segment;
  if (canceled.length) {
    console.debug(`[main] attempting to cancel ${canceled.length}`);
  }
  const canceledWork: Set<Id<"work">> = new Set();
  const runResult: RunResult = { kind: "canceled" };
  const jobs = toCancel.concat(
    ...(
      await Promise.all(
        canceled.map(async ({ _id, _creationTime, workId }) => {
          await ctx.db.delete(_id);
          if (canceledWork.has(workId)) {
            // We shouldn't have multiple pending cancelations for the same work.
            console.error(`[main] ${workId} already canceled`);
            return null;
          }
          const work = await ctx.db.get(workId);
          if (!work) {
            console.warn(`[main] ${workId} is gone, but trying to cancel`);
            return null;
          }
          // Ensure it doesn't retry.
          await ctx.db.patch(workId, { canceled: true });
          // Ensure it doesn't start.
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
          missing.add(r.workId);
          console.error(`[main] ${r.workId} already gone (skipping recovery)`);
          return null;
        }
        return { ...r, attempt: work.attempts };
      }),
    )
  ).flatMap((r) => (r ? [r] : []));
  state.running = state.running.filter((r) => !missing.has(r.workId));
  if (jobs.length) {
    await ctx.scheduler.runAfter(0, internal.recovery.recover, { jobs });
  }
}

async function handleStart(
  ctx: MutationCtx,
  state: Doc<"internalState">,
  segment: bigint,
  console: Logger,
  { maxParallelism, logLevel }: Config,
) {
  // Schedule as many as needed to reach maxParallelism.
  const toSchedule = maxParallelism - state.running.length;

  const pending =
    toSchedule > 0
      ? await ctx.db
          .query("pendingStart")
          .withIndex("segment", (q) =>
            q
              .gte(
                "segment",
                state.segmentCursors.incoming - CURSOR_BUFFER_SEGMENTS,
              )
              .lte("segment", segment),
          )
          .take(toSchedule)
      : [];

  if (pending) {
    if (pending.length > 0) {
      state.segmentCursors.incoming = pending.at(-1)!.segment;
    } else if (toSchedule > 0) {
      // We have no more pending work, update to now
      state.segmentCursors.incoming = segment;
    }
  }
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
          await ctx.db.delete(_id);
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
): Promise<Id<"_scheduled_functions">> {
  const console = createLogger(logLevel);
  const work = await ctx.db.get(workId);
  if (!work) {
    throw new Error("work not found");
  }
  recordStarted(console, work, lagMs);
  const { attempts: attempt, fnHandle, fnArgs } = work;
  const args = { workId, fnHandle, fnArgs, logLevel, attempt };
  if (work.fnType === "action") {
    return ctx.scheduler.runAfter(0, internal.worker.runActionWrapper, args);
  } else if (work.fnType === "mutation" || work.fnType === "query") {
    return ctx.scheduler.runAfter(0, internal.worker.runMutationWrapper, {
      ...args,
      fnType: work.fnType,
    });
  } else {
    throw new Error(`Unexpected fnType ${work.fnType}`);
  }
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
