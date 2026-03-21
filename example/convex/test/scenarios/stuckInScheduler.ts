import { internalAction } from "../../_generated/server";
import { v } from "convex/values";
import { internal } from "../../_generated/api";
import { enqueueTasks, TaskType } from "../work";
import { Id } from "../../_generated/dataModel";

const parameters = {
  taskCount: v.optional(v.number()),
  maxParallelism: v.optional(v.number()),
  contentionConcurrency: v.optional(v.number()),
  dbOps: v.optional(v.number()),
};

/**
 * Scenario: trigger stuckInScheduler recovery by causing OCC contention.
 *
 * All workpool mutations write to the same shared document. Additionally,
 * an external action continuously writes to the same document to create
 * OCC contention that causes the scheduler to retry wrapper mutations
 * with exponential backoff until they become "stuck" (pending > threshold).
 *
 * Run:
 *   npx convex run --push test/scenarios/stuckInScheduler:default '{}'
 *
 * Poll metrics while running or after:
 *   npx convex run test/run:metrics
 */
export default internalAction({
  args: parameters,
  handler: async (
    ctx,
    {
      taskCount = 100,
      maxParallelism = 100,
      contentionConcurrency = 20,
      dbOps = 50,
    },
  ) => {
    const taskType: TaskType = "mutation";

    // Create the shared document that all mutations will contend on
    const conflictDocId: Id<"data"> = await ctx.runMutation(
      internal.test.work.createSharedDoc,
    );

    const runId: Id<"runs"> = await ctx.runMutation(internal.test.run.start, {
      scenario: "stuckInScheduler",
      parameters: {
        taskCount,
        maxParallelism,
        taskType,
        conflictDocId,
        contentionConcurrency,
        dbOps,
      },
    });

    const scenarioStart = Date.now();
    console.log(
      `stuckInScheduler: ${taskCount} mutations, maxParallelism=${maxParallelism}, ` +
        `contentionConcurrency=${contentionConcurrency}, dbOps=${dbOps}, conflictDocId=${conflictDocId}`,
    );

    const onCompleteOpts = {
      onComplete: internal.test.work.markTaskCompleted,
      context: {
        runId,
        type: taskType,
        enqueuedAt: Date.now(),
      },
    };

    const taskArgs = Array.from({ length: taskCount }, (_, i) => ({
      payload: `task-${i}`,
      returnBytes: 10,
      runId,
      readWriteData: 0,
      dbOps,
      conflictDocId,
    }));

    const enqueueStart = Date.now();
    await enqueueTasks({
      ctx,
      taskArgs,
      taskType,
      fn: internal.test.work.configurableMutation,
      onCompleteOpts,
      batchEnqueue: false,
    });
    const enqueueMs = Date.now() - enqueueStart;
    console.log(
      `Enqueued ${taskCount} tasks in ${enqueueMs}ms ` +
        `(${(enqueueMs / taskCount).toFixed(1)}ms/task). Starting external contention...`,
    );

    // Start external contention in the background — this action fires
    // concurrent mutations at the shared doc continuously, causing OCC
    // conflicts with the workpool wrapper mutations.
    const contentionDurationMs = 5 * 60_000; // run for up to 5 minutes
    const contentionPromise = ctx
      .runAction(internal.test.work.generateContention, {
        docId: conflictDocId,
        durationMs: contentionDurationMs,
        concurrency: contentionConcurrency,
      })
      .catch((e) => console.log(`Contention generator stopped: ${e}`));

    console.log("External contention started. Waiting for completion...");

    // Poll for completion
    const pollInterval = 2_000;
    const timeout = 5 * 60_000;
    const pollStart = Date.now();
    let metrics: Record<string, unknown> | null = null;
    while (Date.now() - pollStart < timeout) {
      metrics = (await ctx.runQuery(internal.test.run.metrics)) as Record<
        string,
        unknown
      > | null;
      if (metrics && metrics.status === "completed") break;
      await new Promise((resolve) => setTimeout(resolve, pollInterval));
    }

    // Don't wait for contention to finish — it'll time out on its own
    void contentionPromise;

    const timedOut = !metrics || metrics.status !== "completed";
    const totalDurationMs = Date.now() - scenarioStart;
    const result = {
      metrics,
      enqueueMs,
      totalDurationMs,
      timedOut,
    };

    if (timedOut) {
      console.log(`Timed out after ${timeout}ms.`);
      console.log(JSON.stringify(result, null, 2));
      return result;
    }

    const latency = metrics!.latency as
      | { p50: number; p95: number; p99: number; max: number }
      | undefined;

    console.log(`\n=== stuckInScheduler results ===`);
    console.log(`Total duration: ${totalDurationMs}ms`);
    console.log(`Enqueue: ${enqueueMs}ms`);
    if (latency) {
      console.log(
        `Latency p50=${latency.p50}ms p95=${latency.p95}ms ` +
          `p99=${latency.p99}ms max=${latency.max}ms`,
      );
    }
    console.log(
      `Check the workpool dashboard for 'conflicted' count and [recovery] log messages.`,
    );
    return result;
  },
});
