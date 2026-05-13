import { internalAction, internalMutation } from "../../_generated/server";
import { v } from "convex/values";
import { internal, components } from "../../_generated/api";
import { Workpool } from "@convex-dev/workpool";
import { Workpool as OldWorkpool } from "@convex-dev/workpool-old";
import { Id } from "../../_generated/dataModel";

/**
 * Throughput / overhead measurement scenario.
 *
 *   mode determines what does the enqueue:
 *     raw            — ctx.scheduler.runAfter(0, recorder). Bare-Convex floor.
 *     workpool-bare  — new workpool, no onComplete (worker is the recorder)
 *     workpool-oc    — new workpool with onComplete (worker is no-op)
 *     oldpool-bare   — old workpool (workpool-old), no onComplete
 *     oldpool-oc     — old workpool with onComplete
 *
 * Both pool variants test against the same Convex deployment, against the
 * same tasks table, with the same recorder. The only difference between
 * `workpool-*` and `oldpool-*` is which workpool component is used.
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

const Mode = v.union(
  v.literal("raw"),
  v.literal("workpool-bare"),
  v.literal("workpool-oc"),
  v.literal("oldpool-bare"),
  v.literal("oldpool-oc"),
);

export default internalAction({
  args: {
    taskCount: v.optional(v.number()),
    batchSize: v.optional(v.number()),
    interBatchMs: v.optional(v.number()),
    mode: v.optional(Mode),
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
      maxParallelism = 50,
      pollTimeoutMs = 600_000,
    },
  ) => {
    const runId: Id<"runs"> = await ctx.runMutation(internal.test.run.start, {
      scenario: `overhead-${mode}`,
      parameters: { taskCount, batchSize, mode, maxParallelism, interBatchMs },
    });
    const scenarioStart = Date.now();

    const isWorkpoolNew = mode === "workpool-bare" || mode === "workpool-oc";
    const isWorkpoolOld = mode === "oldpool-bare" || mode === "oldpool-oc";
    const useOnComplete = mode === "workpool-oc" || mode === "oldpool-oc";

    // Configure the right pool (separate components → no cross-contamination)
    if (isWorkpoolNew) {
      await ctx.runMutation(components.testWorkpool.config.update, {
        maxParallelism,
      });
    }
    if (isWorkpoolOld) {
      await ctx.runMutation(components.oldWorkpool.config.update, {
        maxParallelism,
      });
    }

    const newPool = isWorkpoolNew
      ? new Workpool(components.testWorkpool, { maxParallelism })
      : null;
    const oldPool = isWorkpoolOld
      ? new OldWorkpool(components.oldWorkpool, { maxParallelism })
      : null;

    console.log(
      `overhead[${mode}]: ${taskCount} tasks, batchSize=${batchSize}` +
        (newPool || oldPool ? `, max=${maxParallelism}` : ""),
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
      if (mode === "raw") {
        await Promise.all(
          tasks.map(() =>
            ctx.scheduler.runAfter(
              0,
              internal.test.scenarios.overhead.recorder,
              { runId, enqueuedAt },
            ),
          ),
        );
      } else if (!useOnComplete) {
        const pool = newPool ?? oldPool!;
        await Promise.all(
          tasks.map(() =>
            pool.enqueueMutation(
              ctx,
              internal.test.scenarios.overhead.recorder,
              { runId, enqueuedAt },
            ),
          ),
        );
      } else {
        const pool = newPool ?? oldPool!;
        await Promise.all(
          tasks.map(() =>
            pool.enqueueMutation(
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

    console.log(`\n=== overhead[${mode}] ===`);
    console.log(
      `${completedCount}/${taskCount} done in ${total}ms ` +
        `(${tps.toFixed(0)} tps, ${msPerTask.toFixed(1)} ms/task)`,
    );
    return {
      mode,
      taskCount: completedCount,
      totalDurationMs: total,
      enqueueTotal,
      tasksPerSec: Math.round(tps),
      msPerTaskWallClock: Math.round(msPerTask * 10) / 10,
    };
  },
});
