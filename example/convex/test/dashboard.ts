import { v } from "convex/values";
import { query, action } from "../_generated/server";
import { internal } from "../_generated/api";
import { runStatus } from "./run";

function percentile(sorted: number[], p: number): number {
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}

// Just the list of run docs — no per-row tasks aggregation. Each row
// subscribes to `getRun` separately so we don't .collect() the entire
// tasks table on every history poll.
export const listRuns = query({
  args: { limit: v.optional(v.number()) },
  handler: async (ctx, { limit = 50 }) => {
    const runs = await ctx.db.query("runs").order("desc").take(limit);
    return runs.map((run) => ({
      _id: run._id,
      scenario: run.scenario,
      pool: run.pool,
      startTime: run.startTime,
      taskCount: run.taskCount,
    }));
  },
});

export const getRun = query({
  args: { runId: v.id("runs") },
  handler: async (ctx, { runId }) => {
    const run = await ctx.db.get("runs", runId);
    if (!run) return null;
    const tasks = await ctx.db
      .query("tasks")
      .withIndex("runId", (q) => q.eq("runId", run._id))
      .collect();
    const status = await runStatus(ctx, run);
    const latencies = tasks
      .filter((t) => t.enqueuedAt !== undefined)
      .map((t) => t.endTime - t.enqueuedAt!)
      .sort((a, b) => a - b);
    const endTimes = tasks.map((t) => t.endTime);
    const lastEnd = endTimes.length ? Math.max(...endTimes) : undefined;
    return {
      _id: run._id,
      scenario: run.scenario,
      parameters: run.parameters,
      pool: run.pool,
      startTime: run.startTime,
      taskCount: run.taskCount,
      completedCount: tasks.length,
      status,
      totalDurationMs:
        lastEnd !== undefined ? lastEnd - run.startTime : undefined,
      latency:
        latencies.length > 0
          ? {
              p50: percentile(latencies, 50),
              p95: percentile(latencies, 95),
              p99: percentile(latencies, 99),
              max: latencies[latencies.length - 1],
            }
          : undefined,
    };
  },
});

// Time-bucketed throughput. Returns one point per `bucketMs` window from
// the run's start: completed (count finishing in that window) and inFlight
// (enqueued - completed at that t).
export const throughputOverTime = query({
  args: { runId: v.id("runs"), bucketMs: v.optional(v.number()) },
  handler: async (ctx, { runId, bucketMs = 500 }) => {
    const run = await ctx.db.get("runs", runId);
    if (!run) return null;
    const tasks = await ctx.db
      .query("tasks")
      .withIndex("runId", (q) => q.eq("runId", runId))
      .collect();
    if (tasks.length === 0) return { bucketMs, points: [] };

    const start = run.startTime;
    const lastEnd = Math.max(...tasks.map((t) => t.endTime));
    const totalDurationMs = lastEnd - start;
    const numBuckets = Math.max(1, Math.ceil(totalDurationMs / bucketMs) + 1);

    const completedPerBucket = new Array<number>(numBuckets).fill(0);
    const enqueuedPerBucket = new Array<number>(numBuckets).fill(0);
    for (const t of tasks) {
      const cIdx = Math.min(
        numBuckets - 1,
        Math.floor((t.endTime - start) / bucketMs),
      );
      completedPerBucket[cIdx]++;
      if (t.enqueuedAt !== undefined) {
        const eIdx = Math.max(
          0,
          Math.min(
            numBuckets - 1,
            Math.floor((t.enqueuedAt - start) / bucketMs),
          ),
        );
        enqueuedPerBucket[eIdx]++;
      }
    }
    const points: Array<{
      tMs: number;
      completed: number;
      enqueued: number;
      inFlight: number;
    }> = [];
    let cumEnqueued = 0;
    let cumCompleted = 0;
    for (let i = 0; i < numBuckets; i++) {
      cumEnqueued += enqueuedPerBucket[i];
      cumCompleted += completedPerBucket[i];
      points.push({
        tMs: i * bucketMs,
        completed: completedPerBucket[i],
        enqueued: enqueuedPerBucket[i],
        inFlight: Math.max(0, cumEnqueued - cumCompleted),
      });
    }
    return { bucketMs, points };
  },
});

// Sorted latency array, thinned to ~200 points for CDF plotting.
export const latencyCdf = query({
  args: { runId: v.id("runs"), points: v.optional(v.number()) },
  handler: async (ctx, { runId, points = 200 }) => {
    const tasks = await ctx.db
      .query("tasks")
      .withIndex("runId", (q) => q.eq("runId", runId))
      .collect();
    const latencies = tasks
      .filter((t) => t.enqueuedAt !== undefined)
      .map((t) => t.endTime - t.enqueuedAt!)
      .sort((a, b) => a - b);
    if (latencies.length === 0) return [];
    const stride = Math.max(1, Math.floor(latencies.length / points));
    const out: Array<{ pct: number; ms: number }> = [];
    for (let i = 0; i < latencies.length; i += stride) {
      out.push({
        pct: ((i + 1) / latencies.length) * 100,
        ms: latencies[i],
      });
    }
    // ensure the max latency point is present
    out.push({ pct: 100, ms: latencies[latencies.length - 1] });
    return out;
  },
});

// Live status of the currently-running scenario, if any.
export const latestRunStatus = query({
  args: {},
  handler: async (ctx) => {
    const run = await ctx.db.query("runs").order("desc").first();
    if (!run) return null;
    const status = await runStatus(ctx, run);
    return { runId: run._id, scenario: run.scenario, status };
  },
});

const scenarioName = v.union(
  v.literal("burstyBatches"),
  v.literal("throughput"),
  v.literal("overhead"),
  v.literal("sustained"),
  v.literal("bigArgs"),
  v.literal("bigContext"),
  v.literal("bigReturnTypes"),
);

/**
 * Public action so the dashboard can trigger one or more scenario runs by
 * name. Multi-launch is sequenced server-side: each non-final entry is
 * awaited via `ctx.runAction` (so we know its tasks are done and the
 * scenario's poll loop has returned), then we sleep past the 5s
 * "previous run started too recently" guard in `run.start` before the
 * next launch. The final entry is also awaited so that any `run.start`
 * failure surfaces back to the dashboard instead of disappearing into a
 * scheduled action's logs.
 */
const GUARD_BUFFER_MS = 5_500;
/**
 * Concurrent benchmark: fires the same scenario at both pools simultaneously
 * and waits for both runs to fully complete. Useful for testing whether
 * scheduler thrash from a competing pool makes individual pool throughput
 * worse than running it alone.
 */
export const runConcurrent = action({
  args: { scenario: scenarioName, args: v.any() },
  handler: async (ctx, { scenario, args }) => {
    const fn = internal.test.scenarios[scenario].default;
    const [,] = await Promise.all([
      ctx.runAction(fn, { ...args, pool: "old" }),
      ctx.runAction(fn, { ...args, pool: "new" }),
    ]);
  },
});

export const runScenarios = action({
  args: { scenario: scenarioName, argsList: v.array(v.any()) },
  handler: async (ctx, { scenario, argsList }) => {
    const fn = internal.test.scenarios[scenario].default;
    for (let i = 0; i < argsList.length; i++) {
      if (i > 0) {
        await new Promise((r) => setTimeout(r, GUARD_BUFFER_MS));
      }
      await ctx.runAction(fn, argsList[i]);
    }
  },
});
