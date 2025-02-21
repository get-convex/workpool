import { v } from "convex/values";
import { mutation, query } from "./_generated/server.js";
import {
  nextSegment,
  logLevel,
  onComplete,
  retryBehavior,
  config,
  status as statusValidator,
  toSegment,
} from "./shared.js";
import { kickMainLoop } from "./kick.js";
import { api } from "./_generated/api.js";

const MAX_POSSIBLE_PARALLELISM = 100;

export const enqueue = mutation({
  args: {
    fnHandle: v.string(),
    fnName: v.string(),
    fnArgs: v.any(),
    fnType: v.union(v.literal("action"), v.literal("mutation")),
    runAt: v.number(),
    // TODO: annotation?
    onComplete: v.optional(onComplete),
    retryBehavior: v.optional(retryBehavior),
    config,
  },
  returns: v.id("work"),
  handler: async (ctx, { config, runAt, ...workArgs }) => {
    if (config.maxParallelism > MAX_POSSIBLE_PARALLELISM) {
      throw new Error(`maxParallelism must be <= ${MAX_POSSIBLE_PARALLELISM}`);
    }
    if (config.maxParallelism < 1) {
      throw new Error("maxParallelism must be >= 1");
    }
    const workId = await ctx.db.insert("work", {
      ...workArgs,
      attempts: 0,
    });
    await ctx.db.insert("pendingStart", {
      workId,
      segment: toSegment(runAt),
    });
    await kickMainLoop(ctx, "enqueue", config);
    // TODO: stats event
    return workId;
  },
});

export const cancel = mutation({
  args: {
    id: v.id("work"),
    logLevel,
  },
  handler: async (ctx, { id, logLevel }) => {
    await ctx.db.insert("pendingCancelation", {
      workId: id,
      segment: nextSegment(),
    });
    await kickMainLoop(ctx, "cancel", { logLevel });
    // TODO: stats event
  },
});

const PAGE_SIZE = 64;
export const cancelAll = mutation({
  args: { logLevel, before: v.optional(v.number()) },
  handler: async (ctx, { logLevel, before }) => {
    const beforeTime = before ?? Date.now();
    const segment = nextSegment();
    const pageOfWork = await ctx.db
      .query("work")
      .withIndex("by_creation_time", (q) => q.lte("_creationTime", beforeTime))
      .order("desc")
      .take(PAGE_SIZE);
    await Promise.all(
      pageOfWork.map(async ({ _id }) => {
        if (
          await ctx.db
            .query("pendingCancelation")
            .withIndex("workId", (q) => q.eq("workId", _id))
            .first()
        ) {
          return;
        }
        await ctx.db.insert("pendingCancelation", { workId: _id, segment });
      })
    );
    if (pageOfWork.length === PAGE_SIZE) {
      await ctx.scheduler.runAfter(0, api.lib.cancelAll, {
        logLevel,
        before: pageOfWork[pageOfWork.length - 1]._creationTime,
      });
    }
    await kickMainLoop(ctx, "cancel", { logLevel });
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
      return { state: "pending", attempt: work.attempts } as const;
    }
    // Assume it's in progress. It could be pending cancellation
    return { state: "running", attempt: work.attempts } as const;
  },
});

// eslint-disable-next-line @typescript-eslint/no-unused-vars
const console = "THIS IS A REMINDER TO USE createLogger";
