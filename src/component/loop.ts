import { v } from "convex/values";
import { internal } from "./_generated/api";
import { Doc, Id } from "./_generated/dataModel";
import { internalMutation, MutationCtx } from "./_generated/server";
import { createLogger } from "./logging.js";
import { DEFAULT_MAX_PARALLELISM } from "./kick";
import {
  currentSegment,
  fromSegment,
  LogLevel,
  nextSegment,
  toSegment,
} from "./shared";
import { WithoutSystemFields } from "convex/server";
import { recordCompleted } from "./stats";

const CANCELLATION_BATCH_SIZE = 64; // the only queue that can get unbounded.
const SECOND = 1000;
import { recordStarted } from "./stats";
const MINUTE = 60 * SECOND;
const RECOVERY_THRESHOLD_MS = 5 * MINUTE; // attempt to recover jobs this old.
const RECOVERY_PERIOD_SEGMENTS = toSegment(1 * MINUTE); // how often to check.
export const INITIAL_STATE: WithoutSystemFields<Doc<"internalState">> = {
  generation: 0n,
  segmentCursors: { incoming: 0n, completion: 0n, cancelation: 0n },
  lastRecovery: 0n,
  report: { completed: 0, failed: 0, canceled: 0, lastReportTs: 0 },
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
// TODO: use segment as a generation number to enforce only one running at a time.
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

    const globals = await ctx.db.query("globals").unique();
    const console = createLogger(globals?.logLevel);

    // Read pendingCompletions, including retry handling.
    console.time("[mainLoop] pendingCompletion");
    // This won't be too many because the jobs all correspond to being scheduled
    // by a single mainLoop (the previous one), so they're limited by MAX_PARALLELISM.
    const completed = await ctx.db
      .query("pendingCompletion")
      .withIndex("segment", (q) =>
        q
          .gte("segment", state.segmentCursors.completion)
          .lte("segment", args.segment)
      )
      .collect();
    state.segmentCursors.completion = args.segment;
    state.report.completed += completed.length;
    console.debug(`[mainLoop] completing ${completed.length}`);
    state.running = state.running.filter(
      (r) => !completed.some((c) => c.workId === r.workId)
    );
    await Promise.all(
      completed.map(async (c) => {
        // TODO: handle retries, adding to pendingStart
        const work = await ctx.db.get(c.workId);
        if (work) console.info(recordCompleted(work, c.runResult.kind));
        await ctx.db.delete(c._id);
      })
    );
    // TODO: schedule calling the onComplete functions.
    console.timeEnd("[mainLoop] pendingCompletion");

    // Read pendingCancellation, deleting from pendingStart. If it's still running, queue to cancel.
    console.time("[mainLoop] pendingCancelation");
    const canceled = await ctx.db
      .query("pendingCancelation")
      .withIndex("segment", (q) =>
        q
          .gte("segment", state.segmentCursors.cancelation)
          .lte("segment", args.segment)
      )
      .take(CANCELLATION_BATCH_SIZE);
    state.segmentCursors.cancelation = canceled.at(-1)?.segment ?? args.segment;
    state.report.canceled += canceled.length;
    console.debug(`[mainLoop] canceling ${canceled.length}`);
    // TODO: schedule cancellation as part of onComplete
    const toCancel = state.running.filter((r) =>
      canceled.some((c) => c.workId === r.workId)
    );
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
    console.timeEnd("[mainLoop] pendingCancelation");

    if (state.running.length === 0) {
      // If there's nothing active, reset lastRecovery.
      state.lastRecovery = args.segment;
    } else if (args.segment - state.lastRecovery >= RECOVERY_PERIOD_SEGMENTS) {
      // Otherwise schedule recovery for any old jobs.
      const jobs = state.running.filter(
        (r) => r.started < Date.now() - RECOVERY_THRESHOLD_MS
      );
      if (jobs.length) {
        await ctx.scheduler.runAfter(0, internal.recovery.recover, {
          jobs,
        });
      }
      state.lastRecovery = args.segment;
    }

    // Read pendingStart up to max capacity. Update the config, and incomingSegmentCursor.
    console.time("[mainLoop] pendingStart");
    const maxParallelism = globals?.maxParallelism ?? DEFAULT_MAX_PARALLELISM;
    // Schedule as many as needed to reach maxParallelism.
    const toSchedule = maxParallelism - state.running.length;

    const pending = await ctx.db
      .query("pendingStart")
      .withIndex("segment", (q) =>
        q
          .gte("segment", state.segmentCursors.incoming)
          .lte("segment", args.segment)
      )
      .take(toSchedule);
    state.segmentCursors.incoming = pending.at(-1)?.segment ?? args.segment;
    console.debug(`[mainLoop] scheduling ${pending.length} pending work`);
    // Start new work.
    state.running.push(
      ...(await Promise.all(
        pending.map(async ({ _id, workId }) => {
          const scheduledId = await beginWork(ctx, workId, globals?.logLevel);
          await ctx.db.delete(_id);
          return { scheduledId, workId, started: Date.now() };
        })
      ))
    );
    console.timeEnd("[mainLoop] pendingStart");
    // If minute rollover since last report, log report.
    // runStatus update (schedulable?):

    await ctx.db.replace(state._id, state);
    // TODO: dispatch onComplete
  },
});

export const updateRunStatus = internalMutation({
  args: {
    generation: v.int64(),
    segment: v.int64(),
  },
  handler: async (ctx, args) => {
    const globals = await ctx.db.query("globals").unique();
    const console = createLogger(globals?.logLevel);
    const maxParallelism = globals?.maxParallelism ?? DEFAULT_MAX_PARALLELISM;
    const state = await getOrCreateState(ctx);
    if (args.generation !== state.generation) {
      throw new Error(
        `generation mismatch: ${args.generation} !== ${state.generation}`
      );
    }

    console.time("[updateRunStatus] outstandingCancellations");
    const thisSegment = currentSegment();
    const outstandingCancellations = await getNextUp(
      ctx,
      "pendingCancelation",
      { start: state.segmentCursors.cancelation, end: thisSegment }
    );
    console.timeEnd("[updateRunStatus] outstandingCancellations");
    if (outstandingCancellations) {
      await ctx.scheduler.runAfter(0, internal.loop.mainLoop, {
        generation: args.generation,
        segment: thisSegment,
      });
    }

    console.time("[updateRunStatus] nextSegmentIsActionable");
    const nextIsActionable = await nextSegmentIsActionable(
      ctx,
      state,
      maxParallelism
    );
    console.timeEnd("[updateRunStatus] nextSegmentIsActionable");

    const start = nextSegment();
    if (nextIsActionable) {
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
) {
  // First, try with our cursor range, up to next segment.
  const end = nextSegment();
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
  // Next, we look for out-of-order additions we may have missed.
  if (
    await getNextUp(ctx, "pendingCompletion", {
      end: state.segmentCursors.completion,
    })
  ) {
    return true;
  }
  if (
    await getNextUp(ctx, "pendingCancelation", {
      end: state.segmentCursors.cancelation,
    })
  ) {
    return true;
  }
  if (state.running.length < maxParallelism) {
    if (
      await getNextUp(ctx, "pendingStart", {
        end: state.segmentCursors.incoming,
      })
    ) {
      return true;
    }
  }
  return false;
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
          ? q.gte("segment", range.start).lte("segment", range.end)
          : q.gt("segment", range.start)
        : range.end
          ? q.lt("segment", range.end)
          : q
    )
    .first();
}

async function beginWork(
  ctx: MutationCtx,
  workId: Id<"work">,
  logLevel: LogLevel | undefined
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

async function getOrCreateState(ctx: MutationCtx) {
  const state = await ctx.db.query("internalState").unique();
  if (state) return state;
  const globals = await ctx.db.query("globals").unique();
  const console = createLogger(globals?.logLevel);
  console.error("No internalState in running loop! Re-creating empty one...");
  return (await ctx.db.get(
    await ctx.db.insert("internalState", INITIAL_STATE)
  ))!;
}

async function getOrCreateRunningStatus(ctx: MutationCtx) {
  const runStatus = await ctx.db.query("runStatus").unique();
  if (runStatus) return runStatus;
  const globals = await ctx.db.query("globals").unique();
  const console = createLogger(globals?.logLevel);
  console.error("No runStatus in running loop! Re-creating one...");
  return (await ctx.db.get(
    await ctx.db.insert("runStatus", { state: { kind: "running" } })
  ))!;
}
// eslint-disable-next-line @typescript-eslint/no-unused-vars
const console = "THIS IS A REMINDER TO USE createLogger";
