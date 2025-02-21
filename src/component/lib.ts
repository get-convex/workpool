import { v } from "convex/values";
import { mutation, query } from "./_generated/server.js";
import {
  nextWheelSegment,
  logLevel,
  onComplete,
  retryBehavior,
  config,
  status as statusValidator,
  toWheelSegment,
} from "./shared.js";
import { kickMainLoop } from "./loop.js";

const MAX_POSSIBLE_PARALLELISM = 100;

/* TODO
 * Support scheduling & retries
 */
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
      config,
      segment: toWheelSegment(runAt),
    });
    await kickMainLoop(ctx, "enqueue", config);
    // TODO: stats event
    return workId;
  },
});

export const cancel = mutation({
  args: {
    id: v.id("work"),
    logLevel: v.optional(logLevel),
  },
  handler: async (ctx, { id, logLevel }) => {
    await ctx.db.insert("pendingCancelation", {
      workId: id,
      segment: nextWheelSegment(),
    });
    await kickMainLoop(ctx, "cancel", { logLevel });
    // TODO: stats event
  },
});

export const status = query({
  args: { id: v.id("work") },
  returns: statusValidator,
  handler: async (ctx, { id }) => {
    const work = await ctx.db.get(id);
    if (!work) {
      return { state: "done" } as const;
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
