import { internalAction } from "../../_generated/server";
import { v } from "convex/values";
import { internal } from "../../_generated/api";
import { enqueueTasks, TaskType } from "../work";
import { Id } from "../../_generated/dataModel";

/**
 * Throughput / saturation scenario.
 *
 * Enqueues `taskCount` tasks in `batchSize`-sized chunks via batch enqueue,
 * with `interBatchMs` gap between chunks. Designed for sustained-load
 * throughput measurement at high parallelism (e.g. 5000 tasks at max=200).
 *
 * Run:
 *   npx convex run test/scenarios/throughput:default \
 *     '{"taskCount":5000,"batchSize":100,"interBatchMs":50,"maxParallelism":200,"taskDurationMs":20}'
 */
const parameters = {
  taskCount: v.optional(v.number()),
  batchSize: v.optional(v.number()),
  interBatchMs: v.optional(v.number()),
  maxParallelism: v.optional(v.number()),
  taskDurationMs: v.optional(v.number()),
  taskType: v.optional(v.union(v.literal("mutation"), v.literal("action"))),
  pollTimeoutMs: v.optional(v.number()),
};

export default internalAction({
  args: parameters,
  handler: async (
    ctx,
    {
      taskCount = 5000,
      batchSize = 100,
      interBatchMs = 50,
      maxParallelism = 200,
      taskDurationMs = 20,
      taskType = "mutation",
      pollTimeoutMs = 600_000, // 10 minutes for big runs
    },
  ) => {
    const runId: Id<"runs"> = await ctx.runMutation(internal.test.run.start, {
      scenario: "throughput",
      parameters: {
        taskCount,
        batchSize,
        interBatchMs,
        maxParallelism,
        taskType,
        taskDurationMs,
      },
    });

    const fn =
      taskType === "action"
        ? internal.test.work.configurableAction
        : internal.test.work.configurableMutation;

    const scenarioStart = Date.now();
    console.log(
      `throughput: ${taskCount} tasks in batches of ${batchSize}, ` +
        `${interBatchMs}ms gap, max=${maxParallelism}, work=${taskDurationMs}ms`,
    );

    const baseArgs = {
      payload: "throughput",
      returnBytes: 10,
      runId,
      ...(taskType === "action" ? { durationMs: taskDurationMs } : {}),
      ...(taskType === "mutation" ? { readWriteData: 0 } : {}),
    };

    const numBatches = Math.ceil(taskCount / batchSize);
    let enqueued = 0;
    for (let batch = 0; batch < numBatches; batch++) {
      if (batch > 0 && interBatchMs > 0) {
        await new Promise((r) => setTimeout(r, interBatchMs));
      }
      const thisBatch = Math.min(batchSize, taskCount - enqueued);
      const enqueuedAt = Date.now();
      await enqueueTasks({
        ctx,
        taskArgs: Array(thisBatch).fill(baseArgs),
        taskType,
        fn,
        onCompleteOpts: {
          onComplete: internal.test.work.markTaskCompleted,
          context: { runId, type: taskType as TaskType, enqueuedAt },
        },
        batchEnqueue: true,
      });
      enqueued += thisBatch;
    }
    const enqueueTotal = Date.now() - scenarioStart;
    console.log(
      `Enqueued ${taskCount} in ${enqueueTotal}ms ` +
        `(${(taskCount / (enqueueTotal / 1000)).toFixed(0)}/s). Waiting...`,
    );

    // Poll for completion
    const pollStart = Date.now();
    let metrics: Record<string, unknown> | null = null;
    while (Date.now() - pollStart < pollTimeoutMs) {
      metrics = (await ctx.runQuery(internal.test.run.metrics)) as Record<
        string,
        unknown
      > | null;
      if (metrics && metrics.status === "completed") break;
      await new Promise((r) => setTimeout(r, 250));
    }

    const timedOut = !metrics || metrics.status !== "completed";
    if (timedOut) {
      console.log(`Timed out after ${pollTimeoutMs}ms.`);
      console.log(
        "Metrics:",
        JSON.stringify(
          { ...metrics, completedCount: metrics?.completedCount },
          null,
          2,
        ),
      );
      return { metrics, enqueueTotal, timedOut };
    }

    const totalDurationMs = metrics!.totalDurationMs as number;
    const completedCount = metrics!.completedCount as number;
    const latency = metrics!.latency as
      | { p50: number; p95: number; p99: number; max: number }
      | undefined;

    const tasksPerSec = ((completedCount / totalDurationMs) * 1000).toFixed(0);
    console.log(`\n=== throughput results ===`);
    console.log(`Completed: ${completedCount}/${taskCount}`);
    console.log(`Total duration: ${totalDurationMs}ms (${tasksPerSec} tasks/s)`);
    console.log(`Enqueue total: ${enqueueTotal}ms`);
    if (latency) {
      console.log(
        `Latency p50=${latency.p50}ms p95=${latency.p95}ms ` +
          `p99=${latency.p99}ms max=${latency.max}ms`,
      );
    }
    return {
      metrics,
      enqueueTotal,
      timedOut: false,
      tasksPerSec: Number(tasksPerSec),
    };
  },
});
