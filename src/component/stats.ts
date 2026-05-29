import { v } from "convex/values";
import type { Doc, Id } from "./_generated/dataModel.js";
import {
  internalMutation,
  internalQuery,
  type MutationCtx,
} from "./_generated/server.js";
import {
  type Config,
  DEFAULT_MAX_PARALLELISM,
  fnType,
  getCurrentSegment,
  retryBehavior,
  vOnCompleteFnContext,
} from "./shared.js";
import { createLogger, type Logger, logLevel, shouldLog } from "./logging.js";
import { internal } from "./_generated/api.js";
import schema from "./schema.js";
import { paginator } from "convex-helpers/server/pagination";

/**
 * Record stats about work execution. Intended to be queried by Axiom or Datadog.
 * See the [README](https://github.com/get-convex/workpool) for example queries.
 */

export function recordEnqueued(
  console: Logger,
  data: {
    workId: Id<"work">;
    fnName: string;
    runAt: number;
  },
) {
  console.event("enqueued", {
    ...data,
    enqueuedAt: Date.now(),
  });
}

export function recordStarted(
  console: Logger,
  work: Doc<"work">,
  lagMs: number,
  scheduledFunctionId: Id<"_scheduled_functions">,
) {
  console.event("started", {
    workId: work._id,
    fnName: work.fnName,
    enqueuedAt: work._creationTime,
    scheduledFunctionId,
    startedAt: Date.now(),
    startLag: lagMs,
  });
}

export function recordCompleted(
  console: Logger,
  work: Doc<"work">,
  status: "success" | "failed" | "canceled" | "retrying",
  onCompleteScheduledFunctionId: Id<"_scheduled_functions"> | undefined,
) {
  console.event("completed", {
    workId: work._id,
    fnName: work.fnName,
    completedAt: Date.now(),
    onCompleteScheduledFunctionId,
    attempts: work.attempts,
    status,
  });
}

export async function generateReport(
  ctx: MutationCtx,
  console: Logger,
  state: Doc<"internalState">,
  { maxParallelism, logLevel }: Config,
) {
  if (!shouldLog(logLevel, "REPORT")) {
    // Don't waste time if we're not going to log.
    return;
  }
  const currentSegment = getCurrentSegment();
  const pendingStart = await paginator(ctx.db, schema)
    .query("pendingStart")
    .withIndex("segment", (q) =>
      q
        .gte("segment", state.segmentCursors.incoming)
        .lt("segment", currentSegment),
    )
    .paginate({
      numItems: Math.max(maxParallelism, 10),
      cursor: null,
    });
  if (pendingStart.isDone) {
    recordReport(console, {
      ...state.report,
      running: state.running.length,
      backlog: pendingStart.page.length,
    });
  } else {
    await ctx.scheduler.runAfter(0, internal.stats.calculateBacklogAndReport, {
      startSegment: 0n,
      endSegment: currentSegment,
      cursor: pendingStart.continueCursor,
      report: state.report,
      running: state.running.length,
      logLevel,
    });
  }
}

export const calculateBacklogAndReport = internalMutation({
  args: {
    startSegment: v.int64(),
    endSegment: v.int64(),
    cursor: v.string(),
    report: schema.tables.internalState.validator.fields.report,
    running: v.number(),
    logLevel,
  },
  handler: async (ctx, args) => {
    const pendingStart = await (ctx.db.query("pendingStart") as any).count();

    const console = createLogger(args.logLevel);
    recordReport(console, {
      ...args.report,
      running: args.running,
      backlog: pendingStart,
    });
  },
});

function recordReport(
  console: Logger,
  report: Doc<"internalState">["report"] & { running: number; backlog: number },
) {
  const { completed, failed, retries } = report;
  const withoutRetries = completed - retries;
  const failureRate = completed ? (failed + retries) / completed : 0;
  const permanentFailureRate = withoutRetries ? failed / withoutRetries : 0;
  console.event("report", {
    ...report,
    failureRate: Number(failureRate.toFixed(4)),
    permanentFailureRate: Number(permanentFailureRate.toFixed(4)),
  });
}

/**
 * Returns the currently running work items with summary details.
 * Warning: this should not be used from a mutation, as it will cause conflicts.
 */
export const running = internalQuery({
  args: {},
  returns: v.array(
    v.object({
      workId: v.id("work"),
      scheduledId: v.id("_scheduled_functions"),
      fnName: v.string(),
      started: v.number(),
      attempts: v.number(),
    }),
  ),
  handler: async (ctx) => {
    const internalState = await ctx.db.query("internalState").unique();
    if (!internalState) return [];
    return Promise.all(
      internalState.running.map(async ({ workId, scheduledId, started }) => {
        const work = await ctx.db.get(workId);
        return {
          workId,
          scheduledId,
          fnName: work?.fnName ?? "<unknown>",
          started,
          attempts: work?.attempts ?? 0,
        };
      }),
    );
  },
});

/**
 * Returns full details for a specific work item, including args.
 * Args may be stored inline on the work doc or in a separate payload doc.
 */
export const workItem = internalQuery({
  args: { workId: v.id("work") },
  returns: v.union(
    v.null(),
    v.object({
      workId: v.id("work"),
      fnName: v.string(),
      fnType,
      fnHandle: v.string(),
      fnArgs: v.any(),
      attempts: v.number(),
      enqueuedAt: v.number(),
      canceled: v.optional(v.boolean()),
      retryBehavior: v.optional(retryBehavior),
      onComplete: v.optional(vOnCompleteFnContext),
      payloadSize: v.optional(v.number()),
      running: v.optional(
        v.object({
          scheduledId: v.id("_scheduled_functions"),
          started: v.number(),
        }),
      ),
    }),
  ),
  handler: async (ctx, { workId }) => {
    const work = await ctx.db.get(workId);
    if (!work) return null;
    let fnArgs = work.fnArgs;
    if (fnArgs === undefined && work.payloadId) {
      const payload = await ctx.db.get(work.payloadId);
      fnArgs = payload?.args;
    }
    const internalState = await ctx.db.query("internalState").unique();
    const runningEntry = internalState?.running.find(
      (r) => r.workId === workId,
    );
    return {
      workId: work._id,
      fnName: work.fnName,
      fnType: work.fnType,
      fnHandle: work.fnHandle,
      fnArgs,
      attempts: work.attempts,
      enqueuedAt: work._creationTime,
      canceled: work.canceled,
      retryBehavior: work.retryBehavior,
      onComplete: work.onComplete,
      payloadSize: work.payloadSize,
      running: runningEntry
        ? {
            scheduledId: runningEntry.scheduledId,
            started: runningEntry.started,
          }
        : undefined,
    };
  },
});

/**
 * Warning: this should not be used from a mutation, as it will cause conflicts.
 * Use this while developing to see the state of the queue.
 */
export const diagnostics = internalQuery({
  args: {},
  returns: v.any(),
  handler: async (ctx) => {
    const global = await ctx.db.query("globals").unique();
    const internalState = await ctx.db.query("internalState").unique();
    const inProgressWork = internalState?.running.length ?? 0;
    const maxParallelism = global?.maxParallelism ?? DEFAULT_MAX_PARALLELISM;
    const pendingStart = await (ctx.db.query("pendingStart") as any).count();
    const pendingCompletion = await (
      ctx.db.query("pendingCompletion") as any
    ).count();
    const pendingCancelation = await (
      ctx.db.query("pendingCancelation") as any
    ).count();
    const runStatus = await ctx.db.query("runStatus").unique();
    return {
      canceling: pendingCancelation,
      waiting: pendingStart,
      running: inProgressWork - pendingCompletion,
      completing: pendingCompletion,
      spareCapacity: maxParallelism - inProgressWork,
      runStatus: runStatus?.state.kind,
      generation: internalState?.generation,
    };
  },
});
