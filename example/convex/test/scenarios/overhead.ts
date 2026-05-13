import { internalAction, internalMutation } from "../../_generated/server";
import { v } from "convex/values";
import { internal } from "../../_generated/api";
import { Id } from "../../_generated/dataModel";
import { makePool, vPoolKind } from "../pool";

/**
 * Throughput / overhead measurement scenario.
 *
 *   mode:        "raw" (bare ctx.scheduler) or "pool" (use a workpool)
 *   pool:        "new" | "0.4.6" | "0.4.2"  (only meaningful when mode = "pool")
 *   onComplete:  if true, worker is a no-op and the recorder runs as the
 *                onComplete callback. If false, the worker itself records.
 *
 * Both pool variants test against the same Convex deployment, against the
 * same tasks table, with the same recorder. The only difference between
 * `pool=new` and `pool=old` is which workpool component is used.
 */

export const recorder = internalMutation({
  args: { runId: v.id("runs"), enqueuedAt: v.number() },
  handler: async (ctx, args) => {
    await ctx.db.insert("tasks", {
      runId: args.runId,
      workId: "overhead-test" as never,
      type: "mutation",
      endTime: Date.now(),
      enqueuedAt: args.enqueuedAt,
    });
  },
});

export const noop = internalMutation({
  args: {},
  handler: async () => {},
});

export const oncompleteRecorder = internalMutation({
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
      type: "mutation",
      endTime: Date.now(),
      enqueuedAt: args.context.enqueuedAt,
    });
  },
});

const Mode = v.union(v.literal("raw"), v.literal("pool"));

export default internalAction({
  args: {
    taskCount: v.optional(v.number()),
    batchSize: v.optional(v.number()),
    interBatchMs: v.optional(v.number()),
    mode: v.optional(Mode),
    pool: v.optional(vPoolKind),
    onComplete: v.optional(v.boolean()),
    maxParallelism: v.optional(v.number()),
    pollTimeoutMs: v.optional(v.number()),
  },
  handler: async (
    ctx,
    {
      taskCount = 1000,
      batchSize = 50,
      interBatchMs = 0,
      mode = "raw",
      pool: poolKind = "new",
      onComplete = false,
      maxParallelism = 50,
      pollTimeoutMs = 600_000,
    },
  ) => {
    const usePool = mode === "pool";
    const scenarioLabel = usePool
      ? `overhead-${poolKind}${onComplete ? "-oc" : "-bare"}`
      : "overhead-raw";
    const runId: Id<"runs"> = await ctx.runMutation(internal.test.run.start, {
      scenario: scenarioLabel,
      parameters: {
        taskCount,
        batchSize,
        mode,
        onComplete,
        maxParallelism,
        interBatchMs,
      },
      pool: usePool ? poolKind : undefined,
    });
    const scenarioStart = Date.now();
    // run.start already configured the right component's maxParallelism.
    const pool = usePool ? makePool(poolKind, { maxParallelism }) : null;

    console.log(
      `${scenarioLabel}: ${taskCount} tasks, batchSize=${batchSize}` +
        (pool ? `, max=${maxParallelism}` : ""),
    );

    const numBatches = Math.ceil(taskCount / batchSize);
    let enqueued = 0;
    for (let batch = 0; batch < numBatches; batch++) {
      if (batch > 0 && interBatchMs > 0) {
        await new Promise((r) => setTimeout(r, interBatchMs));
      }
      const thisBatch = Math.min(batchSize, taskCount - enqueued);
      const enqueuedAt = Date.now();
      const tasks = Array(thisBatch).fill(0);
      if (!usePool) {
        await Promise.all(
          tasks.map(() =>
            ctx.scheduler.runAfter(
              0,
              internal.test.scenarios.overhead.recorder,
              { runId, enqueuedAt },
            ),
          ),
        );
      } else if (!onComplete) {
        await Promise.all(
          tasks.map(() =>
            pool!.enqueueMutation(
              ctx,
              internal.test.scenarios.overhead.recorder,
              { runId, enqueuedAt },
            ),
          ),
        );
      } else {
        await Promise.all(
          tasks.map(() =>
            pool!.enqueueMutation(
              ctx,
              internal.test.scenarios.overhead.noop,
              {},
              {
                onComplete: internal.test.scenarios.overhead.oncompleteRecorder,
                context: { runId, enqueuedAt },
              },
            ),
          ),
        );
      }
      enqueued += thisBatch;
    }
    const enqueueTotal = Date.now() - scenarioStart;
    console.log(
      `Enqueued ${taskCount} in ${enqueueTotal}ms ` +
        `(${(taskCount / (enqueueTotal / 1000)).toFixed(0)}/s).`,
    );

    const pollStart = Date.now();
    let metrics: Record<string, unknown> | null = null;
    while (Date.now() - pollStart < pollTimeoutMs) {
      metrics = (await ctx.runQuery(internal.test.run.metrics)) as Record<
        string,
        unknown
      > | null;
      if (metrics && metrics.status === "completed") break;
      await new Promise((r) => setTimeout(r, 100));
    }

    if (!metrics || metrics.status !== "completed") {
      console.log(`Timed out after ${pollTimeoutMs}ms.`);
      return { metrics, enqueueTotal, timedOut: true };
    }

    const total = metrics.totalDurationMs as number;
    const completedCount = metrics.completedCount as number;
    const tps = (completedCount / total) * 1000;
    const msPerTask = total / completedCount;

    console.log(`\n=== ${scenarioLabel} ===`);
    console.log(
      `${completedCount}/${taskCount} done in ${total}ms ` +
        `(${tps.toFixed(0)} tps, ${msPerTask.toFixed(1)} ms/task)`,
    );
    return {
      mode,
      pool: usePool ? poolKind : undefined,
      onComplete,
      taskCount: completedCount,
      totalDurationMs: total,
      enqueueTotal,
      tasksPerSec: Math.round(tps),
      msPerTaskWallClock: Math.round(msPerTask * 10) / 10,
    };
  },
});
