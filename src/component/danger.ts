import { v } from "convex/values";
import { api, internal } from "./_generated/api.js";
import type { Doc, Id } from "./_generated/dataModel.js";
import {
  action,
  internalAction,
  internalMutation,
  internalQuery,
  mutation,
  query,
} from "./_generated/server.js";

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
    const entries = await ctx.db
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
      })
    );
    if (!entries.isDone) {
      await ctx.scheduler.runAfter(0, internal.danger.clearPending, {
        before: time,
        cursor: entries.continueCursor,
      });
    }
  },
});
