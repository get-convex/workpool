import { WithoutSystemFields } from "convex/server";
import { v } from "convex/values";
import { internal } from "./_generated/api.js";
import { Doc, Id } from "./_generated/dataModel.js";
import { internalMutation, MutationCtx } from "./_generated/server.js";
import { DEFAULT_MAX_PARALLELISM } from "./kick.js";
import {
  createLogger,
  DEFAULT_LOG_LEVEL,
  Logger,
  LogLevel,
} from "./logging.js";
import {
  Config,
  currentSegment,
  fromSegment,
  nextSegment,
  toSegment,
} from "./shared.js";
import { recordReport, recordStarted } from "./stats.js";

const CANCELLATION_BATCH_SIZE = 64; // the only queue that can get unbounded.
const SECOND = 1000;
const MINUTE = 60 * SECOND;
const RECOVERY_THRESHOLD_MS = 5 * MINUTE; // attempt to recover jobs this old.
const RECOVERY_PERIOD_SEGMENTS = toSegment(1 * MINUTE); // how often to check.
const CURSOR_BUFFER_SEGMENTS = toSegment(2 * SECOND); // buffer for cursor updates.
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
  args: {
    generation: v.int64(),
    segment: v.int64(),
  },
  handler: async (ctx, args) => {
    // State will be modified and patched at the end of the function.
    const state = await getOrCreateState(ctx);
    if (args.generation !== state.generation) {
      throw new Error(
        `generation mismatch: ${args.generation} !== ${state.generation}`
      );
    }
    state.generation++;

    const globals = await getGlobals(ctx);
    const console = createLogger(globals.logLevel);

    // Read pendingCompletions, including retry handling.
    console.time("[main] pendingCompletion");
    await handleCompletions(ctx, state, args.segment, console);
    console.timeEnd("[main] pendingCompletion");

    // Read pendingCancelation, deleting from pendingStart. If it's still running, queue to cancel.
    console.time("[main] pendingCancelation");
    await handleCancelation(ctx, state, args.segment, console);
    console.timeEnd("[main] pendingCancelation");

    if (state.running.length === 0) {
      // If there's nothing active, reset lastRecovery.
      state.lastRecovery = args.segment;
    } else if (args.segment - state.lastRecovery >= RECOVERY_PERIOD_SEGMENTS) {
      // Otherwise schedule recovery for any old jobs.
      await handleRecovery(ctx, state, console);
      state.lastRecovery = args.segment;
    }

    // Read pendingStart up to max capacity. Update the config, and incomingSegmentCursor.
    console.time("[main] pendingStart");
    await handleStart(ctx, state, args.segment, console, globals);
    console.timeEnd("[main] pendingStart");

    if (Date.now() - state.report.lastReportTs >= MINUTE) {
      // If minute rollover since last report, log report.
      // Try to avoid clock skew by shifting by a minute.
      let lastReportTs = state.report.lastReportTs + MINUTE;
      if (Date.now() > lastReportTs + MINUTE / 2) {
        // It's been a while, let's start fresh.
        lastReportTs = Date.now();
      }
      console.info(recordReport(state));
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
    });
  },
});

export const updateRunStatus = internalMutation({
  args: { generation: v.int64() },
  handler: async (ctx, args) => {
    const globals = await getGlobals(ctx);
    const console = createLogger(globals.logLevel);
    const maxParallelism = globals.maxParallelism;
    const state = await getOrCreateState(ctx);
    if (args.generation !== state.generation) {
      throw new Error(
        `generation mismatch: ${args.generation} !== ${state.generation}`
      );
    }

    console.time("[updateRunStatus] outstandingCancelations");
    const thisSegment = currentSegment();
    const outstandingCancelations = await getNextUp(ctx, "pendingCancelation", {
      start: state.segmentCursors.cancelation,
      end: thisSegment,
    });
    console.timeEnd("[updateRunStatus] outstandingCancelations");
    if (outstandingCancelations) {
      await ctx.scheduler.runAfter(0, internal.loop.main, {
        generation: args.generation,
        segment: thisSegment,
      });
      return;
    }

    // TODO: check for current segment (or from args) first, to avoid OCCs.
    console.time("[updateRunStatus] nextSegmentIsActionable");
    const [nextIsActionable, cursors] = await nextSegmentIsActionable(
      ctx,
      state,
      maxParallelism
    );
    console.timeEnd("[updateRunStatus] nextSegmentIsActionable");

    if (nextIsActionable) {
      await ctx.db.patch(state._id, {
        segmentCursors: {
          ...state.segmentCursors,
          ...cursors,
        },
      });
      const segment = nextSegment();
      await ctx.scheduler.runAt(fromSegment(segment), internal.loop.main, {
        generation: args.generation,
        segment,
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
    const start = nextSegment();
    const docs = await Promise.all(
      actionableTables.map(async (tableName) =>
        getNextUp(ctx, tableName, { start })
      )
    );
    console.timeEnd("[updateRunStatus] findNextSegment");
    let segment = docs.map((d) => d?.segment).sort()[0];
    const runStatus = await getOrCreateRunningStatus(ctx);
    const saturated = state.running.length >= maxParallelism;
    if (segment || state.running.length > 0) {
      // If there's something to do, schedule for next actionable segment.
      // Or the next recovery, whichever comes first.
      const nextRecoverySegment = state.lastRecovery + RECOVERY_PERIOD_SEGMENTS;
      if (!segment || segment > nextRecoverySegment) {
        segment = nextRecoverySegment;
      }
      const scheduledId = await ctx.scheduler.runAt(
        fromSegment(segment),
        internal.loop.main,
        { generation: args.generation, segment }
      );
      await ctx.db.patch(runStatus._id, {
        state: {
          kind: "scheduled",
          scheduledId,
          saturated,
          generation: args.generation,
          segment,
        },
      });
      return;
    }
    // There seems to be nothing in the future to do, so go idle.
    await ctx.db.patch(runStatus._id, {
      state: { kind: "idle", generation: args.generation },
    });
  },
});

async function nextSegmentIsActionable(
  ctx: MutationCtx,
  state: Doc<"internalState">,
  maxParallelism: number
): Promise<
  [boolean, { completion?: bigint; cancelation?: bigint; incoming?: bigint }]
> {
  // First, try with our cursor range, up to next segment.
  const end = nextSegment();
  if (
    await getNextUp(ctx, "pendingCancelation", {
      start: state.segmentCursors.cancelation,
      end,
    })
  ) {
    return [true, {}];
  }
  if (
    await getNextUp(ctx, "pendingCompletion", {
      start: state.segmentCursors.completion,
      end,
    })
  ) {
    return [true, {}];
  }
  if (state.running.length < maxParallelism) {
    if (
      await getNextUp(ctx, "pendingStart", {
        start: state.segmentCursors.incoming,
        end,
      })
    ) {
      return [true, {}];
    }
  }
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
  range: { start?: bigint; end?: bigint }
) {
  return ctx.db
    .query(table)
    .withIndex("segment", (q) =>
      range.start
        ? range.end
          ? q
              .gte("segment", range.start - CURSOR_BUFFER_SEGMENTS)
              .lte("segment", range.end)
          : q.gt("segment", range.start - CURSOR_BUFFER_SEGMENTS)
        : range.end
          ? q.lt("segment", range.end)
          : q
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
  console: Logger
) {
  const startSegment = state.segmentCursors.completion - CURSOR_BUFFER_SEGMENTS;
  // This won't be too many because the jobs all correspond to being scheduled
  // by a single main (the previous one), so they're limited by MAX_PARALLELISM.
  const completed = await ctx.db
    .query("pendingCompletion")
    .withIndex("segment", (q) =>
      q.gte("segment", startSegment).lte("segment", segment)
    )
    .collect();
  state.segmentCursors.completion = segment;
  await Promise.all(
    completed.map(async (c) => {
      await ctx.db.delete(c._id);

      if (state.running.some((r) => r.workId === c.workId)) {
        if (c.runResult.kind === "success") {
          state.report.succeeded++;
        } else if (c.runResult.kind === "failed") {
          if (c.retrying) {
            state.report.retries++;
          } else {
            state.report.failed++;
          }
        }
      } else {
        console.error(
          `[main] completing ${c.workId} but it's not in "running"`
        );
      }
    })
  );
  // We do this after so the stats above know if it was in progress.
  const before = state.running.length;
  state.running = state.running.filter(
    (r) => !completed.some((c) => c.workId === r.workId)
  );
  const numCompleted = before - state.running.length;
  state.report.completed += numCompleted;
  console.debug(`[main] completed ${numCompleted} work`);
}

async function handleCancelation(
  ctx: MutationCtx,
  state: Doc<"internalState">,
  segment: bigint,
  console: Logger
) {
  const start = state.segmentCursors.cancelation - CURSOR_BUFFER_SEGMENTS;
  const canceled = await ctx.db
    .query("pendingCancelation")
    .withIndex("segment", (q) =>
      q.gte("segment", start).lte("segment", segment)
    )
    .take(CANCELLATION_BATCH_SIZE);
  state.segmentCursors.cancelation = canceled.at(-1)?.segment ?? segment;
  console.debug(`[main] attempting to cancel ${canceled.length}`);
  const canceledWork: Set<Id<"work">> = new Set();
  await Promise.all(
    canceled.map(async ({ _id, workId }) => {
      await ctx.db.delete(_id);
      if (canceledWork.has(workId)) {
        // We shouldn't have multiple pending cancelations for the same work.
        console.error(`[main] ${workId} already canceled`);
        return;
      }
      const work = await ctx.db.get(workId);
      if (!work) {
        console.warn(`[main] ${workId} is gone, but trying to cancel`);
        return;
      }
      // Ensure it doesn't retry.
      await ctx.db.patch(workId, { retryBehavior: undefined });
      // Ensure it doesn't start.
      const pendingStart = await ctx.db
        .query("pendingStart")
        .withIndex("workId", (q) => q.eq("workId", workId))
        .unique();
      if (pendingStart && !canceledWork.has(workId)) {
        state.report.canceled++;
        await ctx.db.delete(pendingStart._id);
        canceledWork.add(workId);
      }
    })
  );
  await ctx.scheduler.runAfter(0, internal.complete.complete, {
    jobs: canceled.map((c) => ({
      workId: c.workId,
      runResult: { kind: "canceled" as const },
    })),
  });
}

async function handleRecovery(
  ctx: MutationCtx,
  state: Doc<"internalState">,
  console: Logger
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
          console.warn(`[main] ${r.workId} already gone (skipping recovery)`);
          return null;
        }
        return { ...r, attempts: work.attempts };
      })
    )
  ).filter((r) => r !== null);
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
  globals: Config
) {
  const maxParallelism = globals.maxParallelism;
  // Schedule as many as needed to reach maxParallelism.
  const toSchedule = maxParallelism - state.running.length;

  const pending = await ctx.db
    .query("pendingStart")
    .withIndex("segment", (q) =>
      q
        .gte("segment", state.segmentCursors.incoming - CURSOR_BUFFER_SEGMENTS)
        .lte("segment", segment)
    )
    // We filter out any work that's already running.
    // This can happen if we are retrying and for some reason we haven't
    // processed the completion before the next attempt.
    .filter((q) =>
      q.and(...state.running.map((r) => q.neq(q.field("workId"), r.workId)))
    )
    .take(toSchedule);

  state.segmentCursors.incoming = pending.at(-1)?.segment ?? segment;
  console.debug(`[main] scheduling ${pending.length} pending work`);
  // Start new work.
  state.running.push(
    ...(await Promise.all(
      pending.map(async ({ _id, workId }) => {
        const scheduledId = await beginWork(ctx, workId, globals.logLevel);
        await ctx.db.delete(_id);
        return { scheduledId, workId, started: Date.now() };
      })
    ))
  );
}

async function beginWork(
  ctx: MutationCtx,
  workId: Id<"work">,
  logLevel: LogLevel
): Promise<Id<"_scheduled_functions">> {
  const console = createLogger(logLevel);
  const work = await ctx.db.get(workId);
  if (!work) {
    throw new Error("work not found");
  }
  console.info(recordStarted(work));
  if (work.fnType === "action") {
    return await ctx.scheduler.runAfter(0, internal.worker.runActionWrapper, {
      workId: work._id,
      fnHandle: work.fnHandle,
      fnArgs: work.fnArgs,
      logLevel,
    });
  } else if (work.fnType === "mutation") {
    return await ctx.scheduler.runAfter(0, internal.worker.runMutationWrapper, {
      workId: work._id,
      fnHandle: work.fnHandle,
      fnArgs: work.fnArgs,
      logLevel,
    });
  } else {
    throw new Error(`Unexpected fnType ${work.fnType}`);
  }
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
    await ctx.db.insert("internalState", INITIAL_STATE)
  ))!;
}

async function getOrCreateRunningStatus(ctx: MutationCtx) {
  const runStatus = await ctx.db.query("runStatus").unique();
  if (runStatus) return runStatus;
  const globals = await getGlobals(ctx);
  const console = createLogger(globals.logLevel);
  console.error("No runStatus in running loop! Re-creating one...");
  return (await ctx.db.get(
    await ctx.db.insert("runStatus", { state: { kind: "running" } })
  ))!;
}
// eslint-disable-next-line @typescript-eslint/no-unused-vars
const console = "THIS IS A REMINDER TO USE createLogger";
