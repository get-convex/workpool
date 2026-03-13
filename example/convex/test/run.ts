import {
  internalMutation,
  QueryCtx,
  internalQuery,
} from "../_generated/server";
import { v } from "convex/values";
import { Id } from "../_generated/dataModel";
import { assert } from "convex-helpers";
import { components } from "../_generated/api";

export async function runStatus(
  ctx: QueryCtx,
  run: { _id: Id<"runs">; taskCount?: number },
) {
  const completedTasks = await ctx.db
    .query("tasks")
    .withIndex("runId", (q) => q.eq("runId", run._id))
    .collect();
  const completedCount = completedTasks.length;
  if (run.taskCount === undefined) {
    return "pending";
  }
  if (completedCount < run.taskCount) {
    return "running";
  }
  return "completed";
}

// Start a new test run
export const start = internalMutation({
  args: {
    scenario: v.string(),
    parameters: v.any(),
  },
  handler: async (ctx, args) => {
    // Check for in-flight tasks from the latest run
    const latestRun = await ctx.db.query("runs").order("desc").first();

    if (latestRun) {
      // Check if there are any in-flight tasks
      const status = await runStatus(ctx, latestRun);

      if (["running", "pending"].includes(status)) {
        throw new Error(
          `Cannot start new run: previous run ${latestRun.scenario} is ${status} (started at ${new Date(latestRun.startTime).toISOString()})`,
        );
      }
      if (latestRun.startTime + 5_000 > Date.now()) {
        throw new Error(
          `Cannot start new run: previous run ${latestRun.scenario} was started less than 5 seconds ago (started at ${new Date(latestRun.startTime).toISOString()})`,
        );
      }
    }
    if (args.parameters.maxParallelism !== undefined) {
      await ctx.runMutation(components.testWorkpool.config.update, {
        maxParallelism: args.parameters.maxParallelism,
      });
    }

    // Create new run
    const runId = await ctx.db.insert("runs", {
      startTime: Date.now(),
      scenario: args.scenario,
      parameters: args.parameters,
      taskCount: args.parameters.taskCount,
    });

    return runId;
  },
});

export const cancel = internalMutation({
  args: {
    runId: v.optional(v.id("runs")),
  },
  handler: async (ctx, args) => {
    const runId =
      args.runId ?? (await ctx.db.query("runs").order("desc").first())?._id;
    if (!runId) throw new Error("No run found");
    const run = await ctx.db.get("runs", runId);
    assert(run);
    console.log(`Deleting run ${runId}: ${JSON.stringify(run)}`);
    await ctx.db.delete("runs", runId);
  },
});

// Get the status of the latest run
export const status = internalQuery({
  handler: async (ctx) => {
    const latestRun = await ctx.db.query("runs").order("desc").first();

    if (!latestRun) {
      return null;
    }
    const status = await runStatus(ctx, latestRun);

    return { run: latestRun, status };
  },
});

function percentile(sorted: number[], p: number): number {
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}

// Get metrics for the latest run
export const metrics = internalQuery({
  handler: async (ctx) => {
    const run = await ctx.db.query("runs").order("desc").first();
    if (!run) return null;

    const tasks = await ctx.db
      .query("tasks")
      .withIndex("runId", (q) => q.eq("runId", run._id))
      .collect();

    const status = await runStatus(ctx, run);
    const completedCount = tasks.length;
    const taskCount = run.taskCount ?? 0;

    // Per-task latency (enqueue → onComplete)
    const latencies = tasks
      .filter((t) => t.enqueuedAt !== undefined)
      .map((t) => t.endTime - t.enqueuedAt!)
      .sort((a, b) => a - b);

    // Per-wave breakdown
    const waveMap = new Map<number, { count: number; latencies: number[] }>();
    for (const t of tasks) {
      if (t.wave === undefined) continue;
      let entry = waveMap.get(t.wave);
      if (!entry) {
        entry = { count: 0, latencies: [] };
        waveMap.set(t.wave, entry);
      }
      entry.count++;
      if (t.enqueuedAt !== undefined) {
        entry.latencies.push(t.endTime - t.enqueuedAt);
      }
    }
    const waves = [...waveMap.entries()]
      .sort(([a], [b]) => a - b)
      .map(([wave, { count, latencies: lats }]) => {
        lats.sort((a, b) => a - b);
        return {
          wave,
          count,
          ...(lats.length > 0 && {
            p50: percentile(lats, 50),
            p99: percentile(lats, 99),
          }),
        };
      });

    // Total duration
    const endTimes = tasks.map((t) => t.endTime);
    const lastEndTime = endTimes.length ? Math.max(...endTimes) : undefined;
    const totalDurationMs = lastEndTime
      ? lastEndTime - run.startTime
      : undefined;

    return {
      status,
      scenario: run.scenario,
      parameters: run.parameters,
      completedCount,
      taskCount,
      ...(totalDurationMs !== undefined && { totalDurationMs }),
      ...(latencies.length > 0 && {
        latency: {
          p50: percentile(latencies, 50),
          p95: percentile(latencies, 95),
          p99: percentile(latencies, 99),
          max: latencies[latencies.length - 1],
        },
      }),
      ...(waves.length > 0 && { waves }),
    };
  },
});
