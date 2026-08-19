import { v } from "convex/values";
import {
  internalAction,
  internalMutation,
  internalQuery,
} from "../_generated/server";
import { internal } from "../_generated/api";
import { makePool, vPoolKind, type PoolKind } from "./pool";

/**
 * Scheduling latency and ordering harness.
 *
 * The throughput scenarios only enqueue ready-now work and only observe
 * completions, which can't answer anything about *when* a task started
 * relative to when it was asked to. Every task here records its own start
 * clock, so start lateness and start ordering are measured directly.
 *
 * One run enqueues a set of `groups` — each a count of tasks scheduled the same
 * distance out — waits for them all to start, and returns a row per task. The
 * caller does the statistics, so the analysis can change without a redeploy.
 *
 *   npx convex run test/latency:default '{
 *     "cell": "demo", "pool": "new",
 *     "groups": [{"delayMs": 3000, "count": 600}, {"delayMs": 0, "count": 2000}]
 *   }'
 *
 * `cell` names the run and is cleared first, so results from different
 * parameters never mix. `pool` picks this branch ("new") or the published
 * baseline ("old"). See `.context/scheduling-experiments.md` for the full
 * method and the questions this was built to answer.
 *
 * Two traps worth knowing before trusting a number:
 *
 * - Every entry in one enqueuing transaction shares a commit stamp *and* a
 *   start time, so a transaction is one sample, not `count` of them. Use
 *   `chunkSize` to get independent trials.
 * - Lateness at large `count` is mostly enqueue backlog, not scheduler
 *   precision. Keep bursts small to measure the latter.
 */

const MAX_PER_MUTATION = 200;

/** The task itself: records when it actually began. */
export const probe = internalMutation({
  args: { taskId: v.id("latencyTasks") },
  returns: v.null(),
  handler: async (ctx, { taskId }) => {
    await ctx.db.patch("latencyTasks", taskId, { startedAt: Date.now() });
    return null;
  },
});

// ~1KB, so a hold op costs commit bytes as well as a round of work.
const HOLD_BLOB = "x".repeat(1024);

/** Trivial callee: nesting a mutation costs wall time without costing reads. */
export const nestedNoop = internalMutation({
  args: {},
  returns: v.null(),
  handler: async () => null,
});

/**
 * Enqueue one chunk. `holdOps` extends this transaction *after* the enqueue so
 * its commit lands later than the start time it wrote — the condition under
 * which a scheduled entry can commit behind the loop's cursor.
 *
 * The hold is counted in operations, not milliseconds: `Date.now()` is frozen
 * inside a mutation, so a wall-clock spin never terminates. The caller times
 * the mutation from an action (where the clock is real) to learn what a given
 * op count actually cost.
 */
export const enqueueChunk = internalMutation({
  args: {
    cell: v.string(),
    pool: vPoolKind,
    delayMs: v.number(),
    count: v.number(),
    seqFrom: v.number(),
    holdOps: v.optional(v.number()),
    // Nested mutation calls: the cheapest way to stretch a transaction, since
    // unlike read/write ops they don't count against the per-function limits.
    nestedCalls: v.optional(v.number()),
    maxParallelism: v.number(),
  },
  returns: v.number(),
  handler: async (ctx, args) => {
    const pool = makePool(args.pool as PoolKind, {
      maxParallelism: args.maxParallelism,
    });
    const enqueuedAt = Date.now();
    const runAt = enqueuedAt + args.delayMs;

    for (let i = 0; i < args.count; i++) {
      const taskId = await ctx.db.insert("latencyTasks", {
        cell: args.cell,
        pool: args.pool,
        delayMs: args.delayMs,
        seq: args.seqFrom + i,
        enqueuedAt,
        runAt,
      });
      await pool.enqueueMutation(
        ctx,
        internal.test.latency.probe,
        { taskId },
        args.delayMs > 0 ? { runAfter: args.delayMs } : {},
      );
    }

    // Extend the transaction so it commits after `runAt`.
    let ops = 0;
    for (let i = 0; i < (args.holdOps ?? 0); i++) {
      const id = await ctx.db.insert("data", { misc: HOLD_BLOB });
      await ctx.db.delete("data", id);
      ops++;
    }
    for (let i = 0; i < (args.nestedCalls ?? 0); i++) {
      await ctx.runMutation(internal.test.latency.nestedNoop, {});
      ops++;
    }
    return ops;
  },
});

/** Clear a cell so it can be re-run. */
export const resetCell = internalMutation({
  args: { cell: v.string() },
  returns: v.null(),
  handler: async (ctx, { cell }) => {
    for (const doc of await ctx.db
      .query("latencyTasks")
      .withIndex("cell", (q) => q.eq("cell", cell))
      .collect()) {
      await ctx.db.delete("latencyTasks", doc._id);
    }
    return null;
  },
});

export const cellRows = internalQuery({
  args: { cell: v.string() },
  returns: v.any(),
  handler: async (ctx, { cell }) => {
    const rows = await ctx.db
      .query("latencyTasks")
      .withIndex("cell", (q) => q.eq("cell", cell))
      .collect();
    return rows.map((r) => ({
      pool: r.pool,
      delayMs: r.delayMs,
      seq: r.seq,
      enqueuedAt: r.enqueuedAt,
      runAt: r.runAt,
      startedAt: r.startedAt,
    }));
  },
});

export type Row = {
  pool: PoolKind;
  delayMs: number;
  seq: number;
  enqueuedAt: number;
  runAt: number;
  startedAt?: number;
};

/**
 * Enqueue a cell and wait for it to finish. Returns the raw rows; the caller
 * does the statistics so the analysis can change without a redeploy.
 */
export default internalAction({
  args: {
    cell: v.string(),
    pool: vPoolKind,
    // Each group is `count` tasks all scheduled `delayMs` out.
    groups: v.array(v.object({ delayMs: v.number(), count: v.number() })),
    // Entries per enqueuing transaction. Every entry in one transaction shares
    // a commit stamp and a start time, so it is one trial as far as
    // out-of-order landing is concerned — cap this to get independent samples.
    chunkSize: v.optional(v.number()),
    holdOps: v.optional(v.number()),
    nestedCalls: v.optional(v.number()),
    maxParallelism: v.optional(v.number()),
    // Gap between chunks, to spread enqueues out rather than one burst.
    interChunkMs: v.optional(v.number()),
    settleMs: v.optional(v.number()),
  },
  returns: v.any(),
  handler: async (ctx, args): Promise<any> => {
    const maxParallelism = args.maxParallelism ?? 50;
    const chunkSize = Math.min(
      args.chunkSize ?? MAX_PER_MUTATION,
      MAX_PER_MUTATION,
    );
    await ctx.runMutation(internal.test.latency.resetCell, { cell: args.cell });

    const total = args.groups.reduce((n, g) => n + g.count, 0);
    const startedEnqueue = Date.now();
    const chunkMs: number[] = [];
    let seq = 0;
    for (const group of args.groups) {
      for (let sent = 0; sent < group.count; sent += chunkSize) {
        const count = Math.min(chunkSize, group.count - sent);
        const t0 = Date.now();
        await ctx.runMutation(internal.test.latency.enqueueChunk, {
          cell: args.cell,
          pool: args.pool,
          delayMs: group.delayMs,
          count,
          seqFrom: seq,
          holdOps: args.holdOps,
          nestedCalls: args.nestedCalls,
          maxParallelism,
        });
        chunkMs.push(Date.now() - t0);
        seq += count;
        if (args.interChunkMs) {
          await new Promise((r) => setTimeout(r, args.interChunkMs));
        }
      }
    }
    const enqueueMs = Date.now() - startedEnqueue;

    // Wait for every task to have started, or give up.
    const maxDelay = Math.max(...args.groups.map((g) => g.delayMs), 0);
    const deadline = Date.now() + maxDelay + (args.settleMs ?? 120_000);
    let rows: Row[] = [];
    while (Date.now() < deadline) {
      rows = await ctx.runQuery(internal.test.latency.cellRows, {
        cell: args.cell,
      });
      if (
        rows.length >= total &&
        rows.every((r) => r.startedAt !== undefined)
      ) {
        break;
      }
      await new Promise((r) => setTimeout(r, 250));
    }
    const missing = rows.filter((r) => r.startedAt === undefined).length;
    const sorted = [...chunkMs].sort((a, b) => a - b);
    return {
      cell: args.cell,
      pool: args.pool,
      total,
      enqueueMs,
      // What the hold actually cost, measured where the clock is real.
      chunkMs: {
        n: sorted.length,
        min: sorted[0],
        median: sorted[Math.floor(sorted.length / 2)],
        max: sorted[sorted.length - 1],
      },
      missing,
      rows,
    };
  },
});
