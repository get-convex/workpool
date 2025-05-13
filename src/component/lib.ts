import { v } from "convex/values";
import { api } from "./_generated/api.js";
import { fnType } from "./shared.js";
import { Id } from "./_generated/dataModel.js";
import { mutation, MutationCtx, query } from "./_generated/server.js";
import { kickMainLoop } from "./kick.js";
import { createLogger, LogLevel, logLevel } from "./logging.js";
import {
  boundScheduledTime,
  config,
  getNextSegment,
  max,
  onComplete,
  retryBehavior,
  status as statusValidator,
  toSegment,
} from "./shared.js";
import { recordEnqueued } from "./stats.js";

const MAX_POSSIBLE_PARALLELISM = 100;
const MAX_PARALLELISM_SOFT_LIMIT = 50;

export const enqueue = mutation({
  args: {
    fnHandle: v.string(),
    fnName: v.string(),
    fnArgs: v.any(),
    fnType,
    runAt: v.number(),
    // TODO: annotation?
    onComplete: v.optional(onComplete),
    retryBehavior: v.optional(retryBehavior),
    config,
  },
  returns: v.id("work"),
  handler: async (ctx, { config, runAt, ...workArgs }) => {
    const console = createLogger(config.logLevel);
    if (config.maxParallelism > MAX_POSSIBLE_PARALLELISM) {
      throw new Error(
        `maxParallelism must be <= ${MAX_PARALLELISM_SOFT_LIMIT}`
      );
    } else if (config.maxParallelism > MAX_PARALLELISM_SOFT_LIMIT) {
      console.warn(
        `maxParallelism should be <= ${MAX_PARALLELISM_SOFT_LIMIT}, but is set to ${config.maxParallelism}. This will be an error in a future version.`
      );
    } else if (config.maxParallelism < 1) {
      throw new Error("maxParallelism must be >= 1");
    }
    runAt = boundScheduledTime(runAt, console);
    const workId = await ctx.db.insert("work", {
      ...workArgs,
      attempts: 0,
    });
    const limit = await kickMainLoop(ctx, "enqueue", config);
    await ctx.db.insert("pendingStart", {
      workId,
      segment: max(toSegment(runAt), limit),
    });
    recordEnqueued(console, { workId, fnName: workArgs.fnName, runAt });
    return workId;
  },
});

export const cancel = mutation({
  args: {
    id: v.id("work"),
    logLevel,
  },
  handler: async (ctx, { id, logLevel }) => {
    const shouldCancel = await shouldCancelWorkItem(ctx, id, logLevel);
    if (shouldCancel) {
      const segment = await kickMainLoop(ctx, "cancel", { logLevel });
      await ctx.db.insert("pendingCancelation", {
        workId: id,
        segment,
      });
    }
  },
});

const PAGE_SIZE = 64;
export const cancelAll = mutation({
  args: { logLevel, before: v.optional(v.number()) },
  handler: async (ctx, { logLevel, before }) => {
    const beforeTime = before ?? Date.now();
    const pageOfWork = await ctx.db
      .query("work")
      .withIndex("by_creation_time", (q) => q.lte("_creationTime", beforeTime))
      .order("desc")
      .take(PAGE_SIZE);
    const shouldCancel = await Promise.all(
      pageOfWork.map(async ({ _id }) =>
        shouldCancelWorkItem(ctx, _id, logLevel)
      )
    );
    let segment = getNextSegment();
    if (shouldCancel.some((c) => c)) {
      segment = await kickMainLoop(ctx, "cancel", { logLevel });
    }
    await Promise.all(
      pageOfWork.map(({ _id }, index) => {
        if (shouldCancel[index]) {
          return ctx.db.insert("pendingCancelation", {
            workId: _id,
            segment,
          });
        }
      })
    );
    if (pageOfWork.length === PAGE_SIZE) {
      await ctx.scheduler.runAfter(0, api.lib.cancelAll, {
        logLevel,
        before: pageOfWork[pageOfWork.length - 1]._creationTime,
      });
    }
  },
});

export const status = query({
  args: { id: v.id("work") },
  returns: statusValidator,
  handler: async (ctx, { id }) => {
    const work = await ctx.db.get(id);
    if (!work) {
      return { state: "finished" } as const;
    }
    const pendingStart = await ctx.db
      .query("pendingStart")
      .withIndex("workId", (q) => q.eq("workId", id))
      .unique();
    if (pendingStart) {
      return { state: "pending", previousAttempts: work.attempts } as const;
    }
    const pendingCompletion = await ctx.db
      .query("pendingCompletion")
      .withIndex("workId", (q) => q.eq("workId", id))
      .unique();
    if (pendingCompletion?.retry) {
      return { state: "pending", previousAttempts: work.attempts } as const;
    }
    // Assume it's in progress. It could be pending cancelation
    return { state: "running", previousAttempts: work.attempts } as const;
  },
});

async function shouldCancelWorkItem(
  ctx: MutationCtx,
  workId: Id<"work">,
  logLevel: LogLevel
) {
  const console = createLogger(logLevel);
  // No-op if the work doesn't exist or has completed.
  const work = await ctx.db.get(workId);
  if (!work) {
    console.warn(`[cancel] work ${workId} doesn't exist`);
    return false;
  }
  const pendingCancelation = await ctx.db
    .query("pendingCancelation")
    .withIndex("workId", (q) => q.eq("workId", workId))
    .unique();
  if (pendingCancelation) {
    console.warn(`[cancel] work ${workId} has already been canceled`);
    return false;
  }
  return true;
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
const console = "THIS IS A REMINDER TO USE createLogger";
