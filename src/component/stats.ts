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
  getCurrentSegment,
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
) {
  console.event("started", {
    workId: work._id,
    fnName: work.fnName,
    enqueuedAt: work._creationTime,
    startedAt: Date.now(),
    startLag: lagMs,
  });
}

export function recordCompleted(
  console: Logger,
  work: Doc<"work">,
  status: "success" | "failed" | "canceled" | "retrying",
) {
  console.event("completed", {
    workId: work._id,
    fnName: work.fnName,
    completedAt: Date.now(),
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
      numItems: maxParallelism,
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
