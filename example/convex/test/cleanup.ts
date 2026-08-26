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

/**
 * Empty the benchmark bookkeeping tables, a batch per transaction, rescheduling
 * until done.
 *
 * These grow by one row per task and are never read across runs, but they are
 * written by every measured task — so letting them accumulate puts a slowly
 * changing cost inside the thing being measured. Run this between experiments.
 *
 *   npx convex run test/cleanup:start
 *   npx convex run test/cleanup:counts    # to watch it drain
 */
/**
 * Deletes forward from a `_creationTime` cursor rather than repeatedly taking
 * from the front. Without the cursor each batch rescans the tombstones of every
 * row already deleted, and the drain rate collapses — 300k rows/min at the start
 * down to under 1k/min — which is the same effect the workpool's own cursors
 * exist to avoid.
 */
export const step = internalMutation({
  args: { index: v.number(), limit: v.number(), after: v.number() },
  returns: v.null(),
  handler: async (ctx, { index, limit, after }) => {
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
    await ctx.scheduler.runAfter(0, internal.test.cleanup.step, {
      index: 0,
      limit,
      after: 0,
    });
    return null;
  },
});

export const counts = internalQuery({
  args: {},
  returns: v.any(),
  handler: async (ctx) => {
    const out: Record<string, number> = {};
    for (const t of TABLES) out[t] = await (ctx.db.query(t) as any).count();
    return out;
  },
});
