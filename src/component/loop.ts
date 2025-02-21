import { v } from "convex/values";
import { internal } from "./_generated/api";
import { Doc, Id } from "./_generated/dataModel";
import { internalMutation, MutationCtx } from "./_generated/server";
import { createLogger, DEFAULT_LOG_LEVEL, Logger } from "./logging.js";
import { DEFAULT_MAX_PARALLELISM } from "./kick";
import {
  Config,
  currentSegment,
  fromSegment,
  LogLevel,
  nextSegment,
  toSegment,
  runResult,
  OnCompleteArgs,
} from "./shared";
import { FunctionHandle, WithoutSystemFields } from "convex/server";
import { recordCompleted, recordReport } from "./stats";

const CANCELLATION_BATCH_SIZE = 64; // the only queue that can get unbounded.
const SECOND = 1000;
import { recordStarted } from "./stats";
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

/** State machine
```mermaid
flowchart TD
    idle -->|enqueue| running
    running-->|"all started, leftover capacity"| scheduled
    scheduled -->|"enqueue, cancel, saveResult, recovery"| running
    running -->|"maxed out"| saturated
    saturated -->|"cancel, saveResult, recovery"| running
    running-->|"all done"| idle
```
 */

// There should only ever be at most one of these scheduled or running.
export const mainLoop = internalMutation({
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
    console.time("[mainLoop] pendingCompletion");
    const done = await handleCompletions(ctx, state, args.segment, console);
    console.timeEnd("[mainLoop] pendingCompletion");

    // Read pendingCancelation, deleting from pendingStart. If it's still running, queue to cancel.
    console.time("[mainLoop] pendingCancelation");
    const toCancel = await handleCancelation(ctx, state, args.segment, console);
    console.timeEnd("[mainLoop] pendingCancelation");

    if (state.running.length === 0) {
      // If there's nothing active, reset lastRecovery.
      state.lastRecovery = args.segment;
    } else if (args.segment - state.lastRecovery >= RECOVERY_PERIOD_SEGMENTS) {
      // Otherwise schedule recovery for any old jobs.
      const oldEnoughToConsider = Date.now() - RECOVERY_THRESHOLD_MS;
      const jobs = state.running.filter((r) => r.started < oldEnoughToConsider);
      if (jobs.length) {
        await ctx.scheduler.runAfter(0, internal.recovery.recover, { jobs });
      }
      state.lastRecovery = args.segment;
    }

    // Read pendingStart up to max capacity. Update the config, and incomingSegmentCursor.
    console.time("[mainLoop] pendingStart");
    await handleStart(ctx, state, args.segment, console, globals);
    console.timeEnd("[mainLoop] pendingStart");

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
    await ctx.scheduler.runAfter(0, internal.loop.complete, {
      done,
      toCancel,
    });
    await ctx.scheduler.runAfter(0, internal.loop.updateRunStatus, {
      generation: state.generation,
    });
  },
});

export const complete = internalMutation({
  args: {
    done: v.array(v.object({ runResult, workId: v.id("work") })),
    toCancel: v.array(
      v.object({
        workId: v.id("work"),
        scheduledId: v.id("_scheduled_functions"),
        started: v.number(),
      })
    ),
  },
  handler: async (ctx, args) => {
    const globals = await getGlobals(ctx);
    const console = createLogger(globals.logLevel);
    await Promise.all(
      args.done.map(async ({ runResult, workId }) => {
        const work = await ctx.db.get(workId);
        if (!work) {
          console.warn(`[complete] ${workId} is done, but its work is gone`);
          return;
        }
        if (work.onComplete) {
          try {
            const handle = work.onComplete.fnHandle as FunctionHandle<
              "mutation",
              OnCompleteArgs,
              void
            >;
            await ctx.runMutation(handle, {
              workId: work._id,
              context: work.onComplete.context,
              result: runResult,
            });
            console.debug(`[complete] onComplete for ${workId} completed`);
          } catch (e) {
            console.error(
              `[complete] error running onComplete for ${workId}`,
              e
            );
          }
        }
        await ctx.db.delete(workId);
      })
    );
    await Promise.all(
      args.toCancel.map(async ({ workId, scheduledId }) => {
        const job = await ctx.db.system.get(scheduledId);
        if (!job) {
          console.warn(
            `[complete] trying to cancel ${workId} but its job is gone`
          );
        } else if (
          job.state.kind !== "inProgress" &&
          job.state.kind !== "pending"
        ) {
          console.warn(
            `[complete] trying to cancel ${workId} but its job state is ${job.state.kind}`
          );
        } else {
          await ctx.scheduler.cancel(scheduledId);
        }
        const work = await ctx.db.get(workId);
        if (!work) {
          console.warn(
            `[complete] trying to cancel ${workId} but its work is gone`
          );
          return;
        }
        if (work.onComplete) {
          try {
            const handle = work.onComplete.fnHandle as FunctionHandle<
              "mutation",
              OnCompleteArgs,
              void
            >;
            await ctx.runMutation(handle, {
              workId: work._id,
              context: work.onComplete.context,
              result: { kind: "canceled" },
            });
            console.debug(`[complete] onComplete for ${workId} canceled`);
          } catch (e) {
            console.error(
              `[complete] error running onComplete for ${workId}`,
              e
            );
          }
        }
        await ctx.db.delete(workId);
      })
    );
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
      await ctx.scheduler.runAfter(0, internal.loop.mainLoop, {
        generation: args.generation,
        segment: thisSegment,
      });
      return;
    }

    console.time("[updateRunStatus] nextSegmentIsActionable");
    const [nextIsActionable, cursors] = await nextSegmentIsActionable(
      ctx,
      state,
      maxParallelism
    );
    console.timeEnd("[updateRunStatus] nextSegmentIsActionable");

    const start = nextSegment();
    if (nextIsActionable) {
      await ctx.db.patch(state._id, {
        segmentCursors: {
          ...state.segmentCursors,
          ...cursors,
        },
      });
      await ctx.scheduler.runAt(fromSegment(start), internal.loop.mainLoop, {
        generation: args.generation,
        segment: start,
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
        internal.loop.mainLoop,
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

 * Important: It should handle retries before cancelations are processed,
 * to allow retries to be canceled.
 */
async function handleCompletions(
  ctx: MutationCtx,
  state: Doc<"internalState">,
  segment: bigint,
  console: Logger
) {
  // This won't be too many because the jobs all correspond to being scheduled
  // by a single mainLoop (the previous one), so they're limited by MAX_PARALLELISM.
  const completed = await ctx.db
    .query("pendingCompletion")
    .withIndex("segment", (q) =>
      q
        .gte(
          "segment",
          state.segmentCursors.completion - CURSOR_BUFFER_SEGMENTS
        )
        .lte("segment", segment)
    )
    .collect();
  state.report.completed += completed.length;
  state.segmentCursors.completion = segment;
  const done: Doc<"pendingCompletion">[] = [];
  await Promise.all(
    completed.map(async (c) => {
      const work = await ctx.db.get(c.workId);
      const maxAttempts = work?.retryBehavior?.maxAttempts;
      const pendingCancelations = await ctx.db
        .query("pendingCancelation")
        .withIndex("workId", (q) => q.eq("workId", c.workId))
        .collect();
      if (work && state.running.some((r) => r.workId === c.workId)) {
        if (
          c.runResult.kind === "failed" &&
          maxAttempts &&
          pendingCancelations.length === 0 &&
          work.attempts < maxAttempts
        ) {
          await rescheduleJob(ctx, work);
          state.report.retries++;
        } else {
          if (c.runResult.kind === "success") {
            state.report.succeeded++;
          } else if (c.runResult.kind === "failed") {
            state.report.failed++;
          }
          // Ensure there aren't any pending cancelations for this work.
          for (const pendingCancelation of pendingCancelations) {
            await ctx.db.delete(pendingCancelation._id);
          }
          done.push(c);
        }
        console.info(recordCompleted(work, c.runResult.kind));
      } else if (work) {
        console.warn(
          `[mainLoop] completing ${c.workId} but it's not in "running"`
        );
      } else {
        console.warn(`[mainLoop] completing ${c.workId} but it's not found`);
      }
      await ctx.db.delete(c._id);
    })
  );
  console.debug(`[mainLoop] completing ${done.length}`);
  state.running = state.running.filter(
    (r) => !completed.some((c) => c.workId === r.workId)
  );
  return done.map((c) => ({ runResult: c.runResult, workId: c.workId }));
}

async function rescheduleJob(
  ctx: MutationCtx,
  work: Doc<"work">
): Promise<number> {
  if (!work.retryBehavior) {
    throw new Error("work has no retryBehavior");
  }
  const backoffMs =
    work.retryBehavior.initialBackoffMs *
    Math.pow(work.retryBehavior.base, work.attempts - 1);
  const nextAttempt = withJitter(backoffMs);
  const startTime = Date.now() + nextAttempt;
  const segment = toSegment(startTime);
  await ctx.db.patch(work._id, {
    attempts: work.attempts + 1,
  });
  await ctx.db.insert("pendingStart", {
    workId: work._id,
    segment,
  });
  return nextAttempt;
}

export function withJitter(delay: number) {
  return delay * (0.5 + Math.random());
}

async function handleCancelation(
  ctx: MutationCtx,
  state: Doc<"internalState">,
  segment: bigint,
  console: Logger
) {
  const canceled = await ctx.db
    .query("pendingCancelation")
    .withIndex("segment", (q) =>
      q
        .gte(
          "segment",
          state.segmentCursors.cancelation - CURSOR_BUFFER_SEGMENTS
        )
        .lte("segment", segment)
    )
    .take(CANCELLATION_BATCH_SIZE);
  state.segmentCursors.cancelation = canceled.at(-1)?.segment ?? segment;
  const toCancel = state.running.filter((r) =>
    canceled.some((c) => c.workId === r.workId)
  );
  state.report.canceled += toCancel.length;
  console.debug(`[mainLoop] canceling ${toCancel.length}`);
  state.running = state.running.filter(
    (r) => !canceled.some((c) => c.workId === r.workId)
  );
  await Promise.all(
    canceled.map(async ({ _id, workId }) => {
      const work = await ctx.db.get(workId);
      if (work) console.info(recordCompleted(work, "canceled"));
      const pendingStart = await ctx.db
        .query("pendingStart")
        .withIndex("workId", (q) => q.eq("workId", workId))
        .unique();
      if (pendingStart) await ctx.db.delete(pendingStart._id);
      await ctx.db.delete(_id);
    })
  );
  return toCancel;
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
    .take(toSchedule);
  state.segmentCursors.incoming = pending.at(-1)?.segment ?? segment;
  console.debug(`[mainLoop] scheduling ${pending.length} pending work`);
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
