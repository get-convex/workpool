import { internalAction, internalMutation } from "../../_generated/server";
import { v } from "convex/values";
import { internal } from "../../_generated/api";
import { Id } from "../../_generated/dataModel";
import { makePool, vPoolKind } from "../pool";

/**
 * Sustained, interleaved load scenario. Designed to exercise OCC paths
 * that the burst-then-drain `overhead` scenario cannot:
 *
 *   - Tasks arrive at a target rate, not in bursts.
 *   - Each worker takes a randomized duration so completions interleave
 *     with new arrivals (rather than landing in lockstep waves).
 *   - The run lasts long enough for the system to reach steady state.
 *
 * This means at any moment there's a mix of: tasks being enqueued, workers
 * running, completions arriving — exactly the scenario where main /
 * updateRunStatus / kickMainLoop reads can race with concurrent writes.
 *
 *   pool:        "new" | "old"
 *   onComplete:  if true, worker is a no-op and the recorder runs via the
 *                onComplete callback. If false, the worker itself records.
 *
 * Workers are actions (so they can actually sleep).
 */

// Worker: an action that sleeps for [minMs, maxMs] then records completion.
export const sleepingRecorder = internalAction({
  args: {
    runId: v.id("runs"),
    enqueuedAt: v.number(),
    minMs: v.number(),
    maxMs: v.number(),
  },
  handler: async (ctx, args) => {
    const ms = args.minMs + Math.random() * (args.maxMs - args.minMs);
    await new Promise((r) => setTimeout(r, ms));
    await ctx.runMutation(internal.test.scenarios.sustained.recordTask, {
      runId: args.runId,
      enqueuedAt: args.enqueuedAt,
    });
  },
});

export const recordTask = internalMutation({
  args: { runId: v.id("runs"), enqueuedAt: v.number() },
  handler: async (ctx, args) => {
    await ctx.db.insert("tasks", {
      runId: args.runId,
      workId: "sustained" as never,
      type: "action",
      endTime: Date.now(),
      enqueuedAt: args.enqueuedAt,
    });
  },
});

// onComplete callback variant
export const oncompleteRecord = internalMutation({
  args: {
    workId: v.string(),
    result: v.any(),
    context: v.object({
      runId: v.id("runs"),
      enqueuedAt: v.number(),
    }),
  },
  handler: async (ctx, args) => {
    await ctx.db.insert("tasks", {
      runId: args.context.runId,
      workId: args.workId as never,
      type: "action",
      endTime: Date.now(),
      enqueuedAt: args.context.enqueuedAt,
    });
  },
});

// A no-op action for the onComplete-mode runs (worker just sleeps).
export const sleepingNoop = internalAction({
  args: { minMs: v.number(), maxMs: v.number() },
  handler: async (_ctx, args) => {
    const ms = args.minMs + Math.random() * (args.maxMs - args.minMs);
    await new Promise((r) => setTimeout(r, ms));
  },
});

export default internalAction({
  args: {
    targetTps: v.optional(v.number()), // tasks per second
    durationSec: v.optional(v.number()), // how long to keep enqueuing
    workerMinMs: v.optional(v.number()),
    workerMaxMs: v.optional(v.number()),
    pool: v.optional(vPoolKind),
    onComplete: v.optional(v.boolean()),
    maxParallelism: v.optional(v.number()),
    pollTimeoutMs: v.optional(v.number()),
  },
  handler: async (
    ctx,
    {
      targetTps = 50,
      durationSec = 20,
      workerMinMs = 50,
      workerMaxMs = 500,
      pool: poolKind = "new",
      onComplete = false,
      maxParallelism = 100,
      pollTimeoutMs = 600_000,
    },
  ) => {
    const totalTasks = targetTps * durationSec;
    const scenarioLabel = `sustained-${poolKind}${onComplete ? "-oc" : "-bare"}`;
    const runId: Id<"runs"> = await ctx.runMutation(internal.test.run.start, {
      scenario: scenarioLabel,
      parameters: {
        taskCount: totalTasks,
        targetTps,
        durationSec,
        workerMinMs,
        workerMaxMs,
        onComplete,
        maxParallelism,
      },
      pool: poolKind,
    });

    // run.start already configured the right component's maxParallelism.
    const pool = makePool(poolKind, { maxParallelism });

    console.log(
      `${scenarioLabel}: ${totalTasks} tasks @ ${targetTps}/s for ${durationSec}s, ` +
        `worker=${workerMinMs}-${workerMaxMs}ms, max=${maxParallelism}`,
    );

    const startTime = Date.now();
    const deadline = startTime + durationSec * 1000;
    let enqueued = 0;

    // Trickle tasks at the target rate. Each iteration kicks off a single
    // enqueue but doesn't await it — that's the realistic pattern (every
    // wave of clients independently calls enqueue).
    const pending: Promise<unknown>[] = [];
    while (Date.now() < deadline) {
      const enqueuedAt = Date.now();
      const args = {
        runId,
        enqueuedAt,
        minMs: workerMinMs,
        maxMs: workerMaxMs,
      };
      let p: Promise<unknown>;
      if (onComplete) {
        p = pool.enqueueAction(
          ctx,
          internal.test.scenarios.sustained.sleepingNoop,
          { minMs: workerMinMs, maxMs: workerMaxMs },
          {
            onComplete: internal.test.scenarios.sustained.oncompleteRecord,
            context: { runId, enqueuedAt },
          },
        );
      } else {
        p = pool.enqueueAction(
          ctx,
          internal.test.scenarios.sustained.sleepingRecorder,
          args,
        );
      }
      pending.push(p);
      enqueued++;
      // Pace
      const elapsed = Date.now() - startTime;
      const targetElapsed = (enqueued / targetTps) * 1000;
      const sleep = targetElapsed - elapsed;
      if (sleep > 0) await new Promise((r) => setTimeout(r, sleep));
    }
    await Promise.allSettled(pending);
    const enqueueTotal = Date.now() - startTime;
    console.log(
      `Enqueued ${enqueued} in ${enqueueTotal}ms (target ${totalTasks}). Waiting...`,
    );

    // Poll until all `enqueued` tasks have completed (we may have overshot
    // or undershot the target count slightly).
    // Update the run's taskCount to the actual number enqueued so the
    // metrics query knows when we're "done".
    await ctx.runMutation(internal.test.scenarios.sustained.setTaskCount, {
      runId,
      taskCount: enqueued,
    });

    const pollStart = Date.now();
    let metrics: Record<string, unknown> | null = null;
    while (Date.now() - pollStart < pollTimeoutMs) {
      metrics = (await ctx.runQuery(internal.test.run.metrics, {
        runId,
      })) as Record<string, unknown> | null;
      if (metrics && metrics.status === "completed") break;
      await new Promise((r) => setTimeout(r, 200));
    }

    if (!metrics || metrics.status !== "completed") {
      console.log(`Timed out after ${pollTimeoutMs}ms.`);
      return { metrics, enqueueTotal, timedOut: true };
    }

    const total = metrics.totalDurationMs as number;
    const completedCount = metrics.completedCount as number;
    const tps = (completedCount / total) * 1000;
    const msPerTask = total / completedCount;
    const latency = metrics.latency as
      | { p50: number; p95: number; p99: number; max: number }
      | undefined;

    console.log(`\n=== ${scenarioLabel} ===`);
    console.log(
      `${completedCount}/${enqueued} done in ${total}ms ` +
        `(${tps.toFixed(0)} tps, ${msPerTask.toFixed(1)} ms/task wall)`,
    );
    if (latency)
      console.log(
        `Latency p50=${latency.p50}ms p95=${latency.p95}ms ` +
          `p99=${latency.p99}ms max=${latency.max}ms`,
      );
    return {
      pool: poolKind,
      onComplete,
      taskCount: completedCount,
      enqueued,
      totalDurationMs: total,
      enqueueTotal,
      tasksPerSec: Math.round(tps),
      msPerTaskWallClock: Math.round(msPerTask * 10) / 10,
      latency,
    };
  },
});

// Helper to patch the actual taskCount after pacing is done (since pacing
// can over/undershoot the nominal target).
export const setTaskCount = internalMutation({
  args: { runId: v.id("runs"), taskCount: v.number() },
  handler: async (ctx, args) => {
    await ctx.db.patch("runs", args.runId, { taskCount: args.taskCount });
  },
});
