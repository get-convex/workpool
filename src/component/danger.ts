import { v, getConvexSize } from "convex/values";
import { internal } from "./_generated/api.js";
import { internalMutation } from "./_generated/server.js";

const DEFAULT_OLDER_THAN = 1000 * 60 * 60 * 24;
const MAX_ROWS_READ = 100;
const MAX_BYTES_READ = 4_000_000;

export const clearPending = internalMutation({
  args: {
    olderThan: v.optional(v.number()),
    before: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    const time =
      args.before ?? Date.now() - (args.olderThan ?? DEFAULT_OLDER_THAN);
    let i = 0,
      totalBytes = 0,
      hasMore = false,
      nextTime;
    console.log("Clearing pending before", new Date(time).toUTCString());
    for await (const entry of ctx.db
      .query("pendingStart")
      .withIndex("by_creation_time", (q) => q.lte("_creationTime", time))
      .order("desc")) {
      i++;
      const work = await ctx.db.get(entry.workId);
      totalBytes +=
        getConvexSize(entry) + getConvexSize(work) + (work?.payloadSize ?? 0);
      if (i > MAX_ROWS_READ || totalBytes > MAX_BYTES_READ) {
        hasMore = true;
        nextTime = entry._creationTime;
        console.log(`Continuing after ${i} entries, ${totalBytes} bytes`);
        break;
      }
      await ctx.db.delete(entry._id);
      if (work) {
        // Clean up any large data stored separately
        if (work.payloadId) {
          await ctx.db.delete("payload", work.payloadId);
        }
        await ctx.db.delete("work", work._id);
      }
    }
    if (hasMore) {
      await ctx.scheduler.runAfter(0, internal.danger.clearPending, {
        before: nextTime,
      });
    } else {
      console.log(`Done clearing pending entries. ${i} in the last batch.`);
    }
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
    let i = 0,
      totalBytes = 0,
      hasMore = false,
      nextTime;
    console.log("Clearing old work before", new Date(time).toUTCString());
    for await (const entry of ctx.db
      .query("work")
      .withIndex("by_creation_time", (q) => q.lte("_creationTime", time))
      .order("desc")) {
      i++;
      const pendingStart = await ctx.db
        .query("pendingStart")
        .withIndex("workId", (q) => q.eq("workId", entry._id))
        .unique();
      const pendingCompletion = await ctx.db
        .query("pendingCompletion")
        .withIndex("workId", (q) => q.eq("workId", entry._id))
        .unique();
      const pendingCancelation = await ctx.db
        .query("pendingCancelation")
        .withIndex("workId", (q) => q.eq("workId", entry._id))
        .unique();
      totalBytes +=
        getConvexSize(entry) +
        getConvexSize(pendingStart) +
        getConvexSize(pendingCompletion) +
        getConvexSize(pendingCancelation) +
        (entry.payloadSize ?? 0);
      if (i > MAX_ROWS_READ || totalBytes > MAX_BYTES_READ) {
        hasMore = true;
        nextTime = entry._creationTime;
        console.log(`Continuing after ${i} entries, ${totalBytes} bytes`);
        break;
      }
      if (pendingStart) {
        await ctx.db.delete(pendingStart._id);
      }
      if (pendingCompletion) {
        await ctx.db.delete(pendingCompletion._id);
      }
      if (pendingCancelation) {
        await ctx.db.delete(pendingCancelation._id);
      }
      // Clean up any large data stored separately
      if (entry.payloadId) {
        await ctx.db.delete(entry.payloadId);
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
    }
    if (hasMore) {
      await ctx.scheduler.runAfter(0, internal.danger.clearOldWork, {
        before: nextTime,
      });
    } else {
      console.log(`Done clearing old work. ${i} in the last batch.`);
    }
  },
});
