/**
 * Scenario: noisy neighbors inside one batch.
 *
 * The batched worker starts up to 32 actions/queries in ONE scheduled
 * action. This scenario enqueues a mix of task classes (fast, slow,
 * failing, non-retryable, queries, big returns, OCC-heavy onComplete) with a
 * shared future `runAt`, interleaved round-robin, so they land in the same
 * loop iteration and the same chunk. It then measures per-class
 * latency to see whether one class's misbehavior delays the others.
 *
 * Run e.g.:
 *   npx convex run test/scenarios/noisyNeighbor:run '{"preset":"slowNeighbor"}'
 *   npx convex run test/scenarios/noisyNeighbor:run '{"classes":[...], "pool":"old"}'
 *
 * Worker-death blast radius (one item's failure rejecting the shared
 * action, batch-mates stuck until recovery) is covered by unit tests in
 * src/component — it needs hooks into component internals to simulate.
 */
import { v } from "convex/values";
import {
  ActionCtx,
  internalAction,
  internalQuery,
} from "../../_generated/server";
import { internal } from "../../_generated/api";
import { Id } from "../../_generated/dataModel";
import { vPoolKind, enqueueFor, PoolKind } from "../pool";
import { vTaskClass } from "../noisy";
import { generateData } from "../work";
import { Infer } from "convex/values";
import { WorkId } from "@convex-dev/workpool";

type TaskClass = Infer<typeof vTaskClass>;

const PRESETS: Record<string, TaskClass[]> = {
  // 32 equal actions: measures intra-batch concurrency (is Promise.all of 32
  // ctx.runAction actually parallel?).
  parallelism: [
    {
      label: "sleep5s",
      count: 32,
      fnType: "action",
      behavior: "succeed",
      durationMs: 5000,
    },
  ],
  // A couple of slow tasks sharing a batch with many fast ones.
  slowNeighbor: [
    {
      label: "fast",
      count: 28,
      fnType: "action",
      behavior: "succeed",
      durationMs: 50,
    },
    {
      label: "slow",
      count: 4,
      fnType: "action",
      behavior: "succeed",
      durationMs: 20000,
    },
  ],
  // Retryable failures next to fast successes.
  failRetry: [
    {
      label: "fast",
      count: 24,
      fnType: "action",
      behavior: "succeed",
      durationMs: 50,
    },
    {
      label: "fail",
      count: 8,
      fnType: "action",
      behavior: "fail",
      durationMs: 50,
      retries: 3,
    },
  ],
  // Non-retryable failures next to fast successes.
  failTerminal: [
    {
      label: "fast",
      count: 24,
      fnType: "action",
      behavior: "succeed",
      durationMs: 50,
    },
    {
      label: "terminal",
      count: 8,
      fnType: "action",
      behavior: "failNonRetryable",
      durationMs: 50,
      retries: 3,
    },
  ],
  // Queries (including failing ones) sharing a batch with actions.
  queryMix: [
    {
      label: "fast",
      count: 16,
      fnType: "action",
      behavior: "succeed",
      durationMs: 50,
    },
    { label: "query", count: 8, fnType: "query", behavior: "succeed" },
    { label: "failQuery", count: 8, fnType: "query", behavior: "fail" },
  ],
  // Large return values next to fast small ones.
  bigReturn: [
    {
      label: "fast",
      count: 24,
      fnType: "action",
      behavior: "succeed",
      durationMs: 50,
    },
    {
      label: "big",
      count: 8,
      fnType: "action",
      behavior: "succeed",
      durationMs: 50,
      returnBytes: 900_000,
    },
  ],
  // Every onComplete in the run contends on one shared counter doc.
  occOnComplete: [
    {
      label: "contended",
      count: 32,
      fnType: "action",
      behavior: "succeed",
      durationMs: 50,
      contendedOnComplete: true,
    },
  ],
};

async function enqueueClasses(
  ctx: ActionCtx,
  {
    classes,
    pool,
    runId,
    runAt,
  }: {
    classes: TaskClass[];
    pool: PoolKind;
    runId: Id<"runs">;
    runAt: number;
  },
): Promise<Map<string, WorkId[]>> {
  const { component, enqueueOne } = enqueueFor(pool);
  // Interleave classes round-robin so every chunk gets a mix.
  const items: { cls: TaskClass; index: number }[] = [];
  for (let i = 0; i < Math.max(...classes.map((c) => c.count)); i++) {
    for (const cls of classes) {
      if (i < cls.count) items.push({ cls, index: i });
    }
  }
  const byLabel = new Map<string, WorkId[]>();
  for (const { cls } of items) {
    if (!byLabel.has(cls.label)) byLabel.set(cls.label, []);
  }
  // Sequential enqueue preserves pendingStart order; the shared future runAt
  // means nothing starts until all are enqueued.
  for (const { cls } of items) {
    const onComplete = {
      onComplete: internal.test.noisy.noisyOnComplete,
      context: {
        runId,
        label: cls.label,
        type: cls.fnType,
        enqueuedAt: runAt,
        ...(cls.contendedOnComplete ? { contended: true } : {}),
      },
      runAt,
      ...(cls.retries
        ? {
            retryBehavior: {
              maxAttempts: cls.retries,
              initialBackoffMs: 250,
              base: 2,
            },
          }
        : {}),
    };
    const workId =
      cls.fnType === "action"
        ? await enqueueOne(
            component,
            ctx,
            "action",
            internal.test.noisy.noisyAction,
            {
              runId,
              label: cls.label,
              behavior: cls.behavior,
              ...(cls.durationMs !== undefined
                ? { durationMs: cls.durationMs }
                : {}),
              ...(cls.returnBytes !== undefined
                ? { returnBytes: cls.returnBytes }
                : {}),
              ...(cls.argBytes ? { filler: generateData(cls.argBytes) } : {}),
              ...(cls.marker ? { marker: true } : {}),
            },
            onComplete,
          )
        : await enqueueOne(
            component,
            ctx,
            "query",
            internal.test.noisy.noisyQuery,
            {
              runId,
              label: cls.label,
              behavior: cls.behavior,
              ...(cls.returnBytes !== undefined
                ? { returnBytes: cls.returnBytes }
                : {}),
            },
            onComplete,
          );
    byLabel.get(cls.label)!.push(workId);
  }
  return byLabel;
}

type LabelMetrics = {
  status: string;
  scenario: string;
  pool?: string;
  completedCount: number;
  taskCount?: number;
  labels: Array<{
    label: string;
    count: number;
    results: Record<string, number>;
    latencyMs?: { min: number; p50: number; p95: number; max: number };
  }>;
};

async function pollUntilDone(
  ctx: ActionCtx,
  runId: Id<"runs">,
  timeoutMs: number,
): Promise<LabelMetrics | null> {
  const pollStart = Date.now();
  let metrics: LabelMetrics | null = null;
  while (Date.now() - pollStart < timeoutMs) {
    metrics = (await ctx.runQuery(
      internal.test.scenarios.noisyNeighbor.metricsByLabel,
      { runId },
    )) as LabelMetrics | null;
    if (metrics && metrics.status === "completed") return metrics;
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  return metrics;
}

export const run = internalAction({
  args: {
    preset: v.optional(v.string()),
    classes: v.optional(v.array(vTaskClass)),
    pool: v.optional(vPoolKind),
    maxParallelism: v.optional(v.number()),
    timeoutMs: v.optional(v.number()),
  },
  handler: async (
    ctx,
    { preset, classes, pool = "new", maxParallelism = 40, timeoutMs = 120_000 },
  ) => {
    const taskClasses = classes ?? PRESETS[preset ?? "slowNeighbor"];
    if (!taskClasses) {
      throw new Error(
        `Unknown preset ${preset}; known: ${Object.keys(PRESETS).join(", ")}`,
      );
    }
    const taskCount = taskClasses.reduce((sum, c) => sum + c.count, 0);
    const runId: Id<"runs"> = await ctx.runMutation(internal.test.run.start, {
      scenario: `noisyNeighbor:${preset ?? "custom"}`,
      parameters: { taskCount, maxParallelism, classes: taskClasses },
      pool,
    });

    const runAt = Date.now() + 3000;
    console.log(
      `noisyNeighbor(${preset ?? "custom"}): ${taskCount} tasks on ${pool} pool, ` +
        `starting together at ${new Date(runAt).toISOString()}`,
    );
    await enqueueClasses(ctx, { classes: taskClasses, pool, runId, runAt });

    const metrics = await pollUntilDone(ctx, runId, timeoutMs);
    console.log(JSON.stringify(metrics, null, 2));
    return metrics;
  },
});

/** Per-label latency + outcome metrics for a run. */
export const metricsByLabel = internalQuery({
  args: { runId: v.id("runs") },
  handler: async (ctx, { runId }) => {
    const run = await ctx.db.get("runs", runId);
    if (!run) return null;
    const tasks = await ctx.db
      .query("tasks")
      .withIndex("runId", (q) => q.eq("runId", runId))
      .collect();
    const byLabel = new Map<
      string,
      { latencies: number[]; kinds: Map<string, number> }
    >();
    for (const t of tasks) {
      const label = t.label ?? "unlabeled";
      let entry = byLabel.get(label);
      if (!entry) {
        entry = { latencies: [], kinds: new Map() };
        byLabel.set(label, entry);
      }
      if (t.enqueuedAt !== undefined) {
        entry.latencies.push(t.endTime - t.enqueuedAt);
      }
      const kind = t.resultKind ?? "unknown";
      entry.kinds.set(kind, (entry.kinds.get(kind) ?? 0) + 1);
    }
    const percentile = (sorted: number[], p: number) =>
      sorted[Math.max(0, Math.ceil((p / 100) * sorted.length) - 1)];
    const labels = [...byLabel.entries()].map(([label, e]) => {
      e.latencies.sort((a, b) => a - b);
      return {
        label,
        count: e.latencies.length,
        results: Object.fromEntries(e.kinds),
        ...(e.latencies.length > 0 && {
          latencyMs: {
            min: e.latencies[0],
            p50: percentile(e.latencies, 50),
            p95: percentile(e.latencies, 95),
            max: e.latencies[e.latencies.length - 1],
          },
        }),
      };
    });
    return {
      status:
        run.taskCount !== undefined && tasks.length >= run.taskCount
          ? "completed"
          : "running",
      scenario: run.scenario,
      pool: run.pool,
      completedCount: tasks.length,
      taskCount: run.taskCount,
      labels: labels.sort((a, b) => a.label.localeCompare(b.label)),
    };
  },
});
