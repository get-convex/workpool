import { internalAction } from "../../_generated/server";
import { v } from "convex/values";
import { internal } from "../../_generated/api";
import { enqueueTasks, TaskType } from "../work";
import { Id } from "../../_generated/dataModel";

const parameters = {
  waveCount: v.optional(v.number()),
  tasksPerWave: v.optional(v.number()),
  delayBetweenWavesMs: v.optional(v.number()),
  taskType: v.optional(v.union(v.literal("mutation"), v.literal("action"))),
  maxParallelism: v.optional(v.number()),
  taskDurationMs: v.optional(v.number()),
};

/**
 * Scenario: rapid concurrent enqueues in waves, with short gaps.
 *
 * Tests the STATUS_COOLDOWN behavior and its effect on OCC conflicts.
 *
 * Each wave enqueues tasks individually via Promise.all, so every enqueue
 * runs kickMainLoop concurrently. kickMainLoop reads the `runStatus`
 * document and:
 *  - If "running": returns immediately (read-only, no conflict).
 *  - If "scheduled" or "idle": writes to `runStatus` to transition to
 *    "running" — concurrent writes cause OCC retries.
 *
 * Without the cooldown, the loop transitions to scheduled/idle between
 * waves, so the next wave's concurrent enqueues all race to write
 * runStatus → OCC conflicts and retries.
 *
 * With the cooldown, runStatus stays "running" between waves, so
 * kickMainLoop short-circuits on a read — no writes, no conflicts.
 *
 * Run:
 *   npx convex run --push test/scenarios/burstyBatches:default '{}'
 *
 * Poll metrics while running or after:
 *   npx convex run test/run:metrics
 */
export default internalAction({
  args: parameters,
  handler: async (
    ctx,
    {
      waveCount = 10,
      tasksPerWave = 20,
      delayBetweenWavesMs = 500,
      taskType = "mutation",
      maxParallelism = 50,
      taskDurationMs = 0,
    },
  ) => {
    const taskCount = waveCount * tasksPerWave;
    const runId: Id<"runs"> = await ctx.runMutation(internal.test.run.start, {
      scenario: "burstyBatches",
      parameters: {
        taskCount,
        waveCount,
        tasksPerWave,
        delayBetweenWavesMs,
        taskType,
        maxParallelism,
        taskDurationMs,
      },
    });

    const scenarioStart = Date.now();
    console.log(
      `burstyBatches: ${waveCount} waves × ${tasksPerWave} tasks, ` +
        `${delayBetweenWavesMs}ms gap, maxParallelism=${maxParallelism}`,
    );

    const fn =
      taskType === "action"
        ? internal.test.work.configurableAction
        : internal.test.work.configurableMutation;

    const waveTimings: { wave: number; enqueueMs: number }[] = [];

    for (let wave = 0; wave < waveCount; wave++) {
      if (wave > 0) {
        await new Promise((resolve) =>
          setTimeout(resolve, delayBetweenWavesMs),
        );
      }

      const enqueuedAt = Date.now();

      const onCompleteOpts = {
        onComplete: internal.test.work.markTaskCompleted,
        context: {
          runId,
          type: taskType as TaskType,
          enqueuedAt,
          wave,
        },
      };

      const baseArgs = {
        payload: `wave-${wave}`,
        returnBytes: 10,
        runId,
        ...(taskType === "action" ? { durationMs: taskDurationMs } : {}),
        ...(taskType === "mutation" ? { readWriteData: 0 } : {}),
      };

      const taskArgs = Array(tasksPerWave).fill(baseArgs);

      // Individual enqueues so each calls kickMainLoop concurrently.
      const waveStart = Date.now();
      await enqueueTasks({
        ctx,
        taskArgs,
        taskType,
        fn,
        onCompleteOpts,
        batchEnqueue: false,
      });
      const enqueueMs = Date.now() - waveStart;
      waveTimings.push({ wave, enqueueMs });
      console.log(
        `Wave ${wave + 1}/${waveCount}: enqueued ${tasksPerWave} in ` +
          `${enqueueMs}ms (${(enqueueMs / tasksPerWave).toFixed(1)}ms/task)`,
      );
    }

    const enqueueTotal = Date.now() - scenarioStart;
    console.log(
      `All ${taskCount} tasks enqueued in ${enqueueTotal}ms. Waiting for completion...`,
    );

    // Poll for completion
    const pollInterval = 500;
    const timeout = 60_000;
    const pollStart = Date.now();
    let metrics: Record<string, unknown> | null = null;
    while (Date.now() - pollStart < timeout) {
      metrics = await ctx.runQuery(internal.test.run.metrics) as Record<string, unknown> | null;
      if (metrics && metrics.status === "completed") break;
      await new Promise((resolve) => setTimeout(resolve, pollInterval));
    }

    const timedOut = !metrics || metrics.status !== "completed";
    const result = {
      metrics,
      enqueueTotal,
      waveTimings,
      timedOut,
    };

    if (timedOut) {
      console.log(`Timed out after ${timeout}ms.`);
      console.log(JSON.stringify(result, null, 2));
      return result;
    }

    // Log results
    const latency = metrics!.latency as { p50: number; p95: number; p99: number; max: number } | undefined;
    const waves = metrics!.waves as Array<{ wave: number; count: number; p50?: number; p99?: number }> | undefined;

    console.log(`\n=== burstyBatches results ===`);
    console.log(`Total duration: ${metrics!.totalDurationMs}ms`);
    if (latency) {
      console.log(
        `Latency p50=${latency.p50}ms p95=${latency.p95}ms ` +
          `p99=${latency.p99}ms max=${latency.max}ms`,
      );
    }
    if (waves) {
      for (const w of waves) {
        const timing = waveTimings.find((t) => t.wave === w.wave);
        console.log(
          `  wave ${w.wave}: ${w.count} done, ` +
            `enqueue=${timing?.enqueueMs ?? "?"}ms` +
            (w.p50 !== undefined ? ` latency p50=${w.p50}ms p99=${w.p99}ms` : ""),
        );
      }
    }
    console.log(`Enqueue total: ${enqueueTotal}ms`);
    return result;
  },
});
