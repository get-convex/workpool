import { v } from "convex/values";
import { kickMainLoop } from "./kick.js";
import { internal } from "./_generated/api.js";
import { internalMutation, type MutationCtx } from "./_generated/server.js";
import {
  findPendingStart,
  memberIds,
  removeFromPendingStart,
} from "./pendingStart.js";

const DEFAULT_OLDER_THAN = 1000 * 60 * 60 * 24;

export const clearPending = internalMutation({
  args: {
    olderThan: v.optional(v.number()),
    before: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    const time =
      args.before ?? Date.now() - (args.olderThan ?? DEFAULT_OLDER_THAN);
    console.log("Clearing pending before", new Date(time).toUTCString());
    for await (const entry of ctx.db
      .query("pendingStart")
      .withIndex("by_creation_time", (q) => q.lte("_creationTime", time))
      .order("desc")) {
      const removed = [];
      let shouldYield = false;
      for (const workId of memberIds(entry)) {
        const work = await ctx.db.get("work", workId);
        if (work) {
          // Clean up any large data stored separately
          if (work.payloadId) {
            await ctx.db.delete("payload", work.payloadId);
          }
          await ctx.db.delete("work", work._id);
        }
        removed.push(workId);
        shouldYield = await usedHalfTransactionBudget(ctx);
        if (shouldYield) break;
      }
      await removeFromPendingStart(ctx, entry, removed);
      if (shouldYield || (await usedHalfTransactionBudget(ctx))) {
        // The inclusive boundary revisits any members left in this document.
        await ctx.scheduler.runAfter(0, internal.danger.clearPending, {
          before: entry._creationTime,
        });
        return;
      }
    }
    console.log("Done clearing pending entries.");
  },
});

export const clearOldWork = internalMutation({
  args: {
    olderThan: v.optional(v.number()),
    before: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    const time =
      args.before ?? Date.now() - (args.olderThan ?? DEFAULT_OLDER_THAN);
    console.log("Clearing old work before", new Date(time).toUTCString());
    for await (const entry of ctx.db
      .query("work")
      .withIndex("by_creation_time", (q) => q.lte("_creationTime", time))
      .order("desc")) {
      if (entry.pendingStartId === undefined) {
        // The upgrade pass removes orphaned legacy queue entries.
        await kickMainLoop(ctx, "kick");
      }
      const pendingStart = await findPendingStart(ctx, entry);
      const pendingCompletion = await ctx.db
        .query("pendingCompletion")
        .withIndex("workId", (q) => q.eq("workId", entry._id))
        .unique();
      const pendingCancelation = await ctx.db
        .query("pendingCancelation")
        .withIndex("workId", (q) => q.eq("workId", entry._id))
        .unique();
      if (pendingStart) {
        await removeFromPendingStart(ctx, pendingStart, [entry._id]);
      }
      if (pendingCompletion) {
        await ctx.db.delete("pendingCompletion", pendingCompletion._id);
      }
      if (pendingCancelation) {
        await ctx.db.delete("pendingCancelation", pendingCancelation._id);
      }
      // Clean up any large data stored separately
      if (entry.payloadId) {
        await ctx.db.delete("payload", entry.payloadId);
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
      await ctx.db.delete("work", entry._id);
      if (await usedHalfTransactionBudget(ctx)) {
        await ctx.scheduler.runAfter(0, internal.danger.clearOldWork, {
          before: entry._creationTime,
        });
        return;
      }
    }
    console.log("Done clearing old work.");
  },
});

/** Leave headroom to finish the current document and schedule another batch. */
async function usedHalfTransactionBudget(ctx: MutationCtx) {
  const metrics = await ctx.meta.getTransactionMetrics();
  return Object.values(metrics).some(
    ({ used, remaining }) => used >= remaining,
  );
}
