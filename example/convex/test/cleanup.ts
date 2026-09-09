import { v } from "convex/values";
import { internalMutation, internalQuery } from "../_generated/server";
import { internal } from "../_generated/api";

const TABLES = [
  "tasks",
  "latencyTasks",
  "runs",
  "schedulingProbes",
  "data",
] as const;

/** Clear bookkeeping between runs; advance the cursor to skip tombstones. */
export const step = internalMutation({
  args: { index: v.number(), limit: v.number(), after: v.number() },
  returns: v.null(),
  handler: async (ctx, { index, limit, after }) => {
    validateLimit(limit);
    if (index >= TABLES.length) return null;
    const table = TABLES[index];
    const docs = await ctx.db
      .query(table)
      .withIndex("by_creation_time", (q) => q.gt("_creationTime", after))
      .take(limit);
    for (const doc of docs) await ctx.db.delete(table, doc._id);
    const done = docs.length < limit;
    await ctx.scheduler.runAfter(0, internal.test.cleanup.step, {
      index: done ? index + 1 : index,
      limit,
      after: done ? 0 : docs[docs.length - 1]._creationTime,
    });
    return null;
  },
});

export const start = internalMutation({
  args: { limit: v.optional(v.number()) },
  returns: v.null(),
  handler: async (ctx, { limit = 1000 }) => {
    validateLimit(limit);
    await ctx.scheduler.runAfter(0, internal.test.cleanup.step, {
      index: 0,
      limit,
      after: 0,
    });
    return null;
  },
});

function validateLimit(limit: number) {
  if (!Number.isInteger(limit) || limit < 1 || limit > 1000) {
    throw new Error("limit must be an integer between 1 and 1000");
  }
}

export const counts = internalQuery({
  args: {},
  returns: v.any(),
  handler: async (ctx) => {
    const out: Record<string, number> = {};
    for (const t of TABLES) out[t] = await (ctx.db.query(t) as any).count();
    return out;
  },
});
