import { v } from "convex/values";
import { internal } from "./_generated/api.js";
import { internalMutation } from "./_generated/server.js";
import { paginator } from "convex-helpers/server/pagination";
import schema from "./schema.js";

const DEFAULT_OLDER_THAN = 1000 * 60 * 60 * 24;

export const clearPending = internalMutation({
  args: {
    olderThan: v.optional(v.number()),
    before: v.optional(v.number()),
    cursor: v.optional(v.string()),
  },
  handler: async (ctx, args) => {
    const time =
      args.before ?? Date.now() - (args.olderThan ?? DEFAULT_OLDER_THAN);
    const entries = await paginator(ctx.db, schema)
      .query("pendingStart")
      .withIndex("by_creation_time", (q) => q.lt("_creationTime", time))
      .paginate({
        cursor: args.cursor ?? null,
        numItems: 100,
      });
    await Promise.all(
      entries.page.map(async (entry) => {
        await ctx.db.delete(entry._id);
        const work = await ctx.db.get(entry.workId);
        if (work) {
          await ctx.db.delete(work._id);
        }
      }),
    );
    if (!entries.isDone) {
      await ctx.scheduler.runAfter(0, internal.danger.clearPending, {
        before: time,
        cursor: entries.continueCursor,
      });
    }
  },
});

export const clearOldWork = internalMutation({
  args: {
    olderThan: v.optional(v.number()),
    before: v.optional(v.number()),
    cursor: v.optional(v.string()),
  },
  handler: async (ctx, args) => {
    const time =
      args.before ?? Date.now() - (args.olderThan ?? DEFAULT_OLDER_THAN);
    const entries = await paginator(ctx.db, schema)
      .query("work")
      .withIndex("by_creation_time", (q) => q.lt("_creationTime", time))
      .paginate({
        cursor: args.cursor ?? null,
        numItems: 100,
      });
    await Promise.all(
      entries.page.map(async (entry) => {
        const pendingStart = await ctx.db
          .query("pendingStart")
          .withIndex("workId", (q) => q.eq("workId", entry._id))
          .unique();
        if (pendingStart) {
          await ctx.db.delete(pendingStart._id);
        }
        const pendingCompletion = await ctx.db
          .query("pendingCompletion")
          .withIndex("workId", (q) => q.eq("workId", entry._id))
          .unique();
        if (pendingCompletion) {
          await ctx.db.delete(pendingCompletion._id);
        }
        const pendingCancelation = await ctx.db
          .query("pendingCancelation")
          .withIndex("workId", (q) => q.eq("workId", entry._id))
          .unique();
        if (pendingCancelation) {
          await ctx.db.delete(pendingCancelation._id);
        }
        console.debug(
          `cleared ${entry.fnName}: ${entry.fnArgs} (${Object.entries({
            pendingStart,
            pendingCompletion,
            pendingCancelation,
          })
            .filter(([_, v]) => v !== null)
            .map(([name]) => name)
            .join(", ")})`,
        );
        await ctx.db.delete(entry._id);
      }),
    );
    if (!entries.isDone) {
      await ctx.scheduler.runAfter(0, internal.danger.clearOldWork, {
        before: time,
        cursor: entries.continueCursor,
      });
    }
  },
});
