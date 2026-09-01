import { type WorkId, Workpool, vWorkId } from "@convex-dev/workpool";
import { v } from "convex/values";
import { components, internal } from "../../_generated/api";
import { internalAction, internalMutation } from "../../_generated/server";

const pool = new Workpool(components.testWorkpool, {
  maxParallelism: 1,
  logLevel: "WARN",
});

export const blocker = internalAction({
  args: { durationMs: v.number() },
  returns: v.null(),
  handler: async (_ctx, { durationMs }) => {
    if (
      !Number.isFinite(durationMs) ||
      durationMs < 1_000 ||
      durationMs > 120_000
    ) {
      throw new Error("durationMs must be between 1,000 and 120,000");
    }
    await new Promise((resolve) => setTimeout(resolve, durationMs));
    return null;
  },
});

export const queuedMutation = internalMutation({
  args: { payload: v.string() },
  returns: v.null(),
  handler: async (_ctx, _args) => null,
});

/**
 * Hold the root mutation open after Workpool's nested enqueue. In the
 * vulnerable implementation, that enqueue read Batch Worker's `workers`
 * document and this widened the interval in which the scheduled loop could
 * invalidate it. The saturation-aware implementation skips that read.
 */
export const slowEnqueue = internalMutation({
  args: {
    payload: v.string(),
    spinIterations: v.number(),
  },
  returns: vWorkId,
  handler: async (ctx, { payload, spinIterations }): Promise<WorkId> => {
    if (
      !Number.isInteger(spinIterations) ||
      spinIterations < 0 ||
      spinIterations > 50_000_000
    ) {
      throw new Error("spinIterations must be an integer from 0 to 50,000,000");
    }
    if (payload.length > 500_000) {
      throw new Error("payload must be at most 500,000 characters");
    }

    const workId: WorkId = await pool.enqueueMutation(
      ctx,
      internal.test.scenarios.enqueueOcc.queuedMutation,
      { payload },
    );

    // A deterministic CPU delay after enqueue. Keeping the checksum live
    // prevents the loop from being optimized away.
    let checksum = 0;
    for (let i = 0; i < spinIterations; i++) {
      checksum = (checksum + i) | 0;
    }
    if (checksum === Number.MAX_SAFE_INTEGER) {
      throw new Error("unreachable checksum");
    }
    return workId;
  },
});

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const index = Math.min(
    sorted.length - 1,
    Math.ceil((p / 100) * sorted.length) - 1,
  );
  return sorted[Math.max(0, index)];
}

/**
 * Sustained-load variant: keep `concurrency` enqueues in flight for
 * `durationMs` against a saturated pool. Unlike the one-shot burst below,
 * this spans many status-cooldown periods, so it can detect how often the
 * loop's idle/running transitions invalidate in-flight enqueues (as retry
 * latency inflation and, at the extreme, exhausted-retry failures).
 */
export const storm = internalAction({
  args: {
    durationMs: v.optional(v.number()),
    concurrency: v.optional(v.number()),
    payloadBytes: v.optional(v.number()),
    spinIterations: v.optional(v.number()),
    blockerMs: v.optional(v.number()),
  },
  returns: v.object({
    attempted: v.number(),
    blockerStarted: v.boolean(),
    succeeded: v.number(),
    failed: v.number(),
    workersFailures: v.number(),
    workerStateFailures: v.number(),
    otherFailures: v.number(),
    p50Ms: v.number(),
    p95Ms: v.number(),
    p99Ms: v.number(),
    maxMs: v.number(),
    errors: v.array(v.string()),
  }),
  handler: async (
    ctx,
    {
      durationMs = 30_000,
      concurrency = 32,
      payloadBytes = 10_000,
      spinIterations = 10_000_000,
      blockerMs = 60_000,
    },
  ) => {
    if (!Number.isInteger(durationMs) || durationMs < 1_000 || durationMs > 120_000) {
      throw new Error("durationMs must be an integer from 1,000 to 120,000");
    }
    if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > 100) {
      throw new Error("concurrency must be an integer from 1 to 100");
    }
    if (!Number.isInteger(payloadBytes) || payloadBytes < 0 || payloadBytes > 500_000) {
      throw new Error("payloadBytes must be an integer from 0 to 500,000");
    }

    const blockerId = await pool.enqueueAction(
      ctx,
      internal.test.scenarios.enqueueOcc.blocker,
      { durationMs: blockerMs },
    );
    let blockerRunning = false;
    const startDeadline = Date.now() + 10_000;
    while (Date.now() < startDeadline) {
      const status = await ctx.runQuery(components.testWorkpool.lib.status, {
        id: blockerId,
      });
      if (status.state === "running") {
        blockerRunning = true;
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    // Let the post-start cooldown expire so the storm begins in the
    // steady-state saturated regime rather than riding the startup window.
    await new Promise((resolve) => setTimeout(resolve, 2_500));

    const payload = "x".repeat(payloadBytes);
    const deadline = Date.now() + durationMs;
    const successLatencies: number[] = [];
    const errors: string[] = [];
    await Promise.all(
      Array.from({ length: concurrency }, async () => {
        while (Date.now() < deadline) {
          const start = Date.now();
          try {
            await ctx.runMutation(
              internal.test.scenarios.enqueueOcc.slowEnqueue,
              { payload, spinIterations },
            );
            successLatencies.push(Date.now() - start);
          } catch (error) {
            errors.push(String(error));
          }
        }
      }),
    );

    const workersFailures = errors.filter((error) =>
      error.includes('"workers" table'),
    ).length;
    const workerStateFailures = errors.filter((error) =>
      error.includes('"workerState" table'),
    ).length;
    successLatencies.sort((a, b) => a - b);
    return {
      attempted: successLatencies.length + errors.length,
      blockerStarted: blockerRunning,
      succeeded: successLatencies.length,
      failed: errors.length,
      workersFailures,
      workerStateFailures,
      otherFailures: errors.length - workersFailures - workerStateFailures,
      p50Ms: percentile(successLatencies, 50),
      p95Ms: percentile(successLatencies, 95),
      p99Ms: percentile(successLatencies, 99),
      maxMs: successLatencies[successLatencies.length - 1] ?? 0,
      errors: errors.slice(0, 5),
    };
  },
});

type RunResult = {
  attempted: number;
  blockerStarted: boolean;
  succeeded: number;
  failed: number;
  workersFailures: number;
  workerStateFailures: number;
  otherFailures: number;
  errors: string[];
};

export default internalAction({
  args: {
    count: v.optional(v.number()),
    payloadBytes: v.optional(v.number()),
    spinIterations: v.optional(v.number()),
    blockerMs: v.optional(v.number()),
  },
  returns: v.object({
    attempted: v.number(),
    blockerStarted: v.boolean(),
    succeeded: v.number(),
    failed: v.number(),
    workersFailures: v.number(),
    workerStateFailures: v.number(),
    otherFailures: v.number(),
    errors: v.array(v.string()),
  }),
  handler: async (
    ctx,
    {
      count = 100,
      payloadBytes = 100_000,
      spinIterations = 10_000_000,
      blockerMs = 30_000,
    },
  ): Promise<RunResult> => {
    if (!Number.isInteger(count) || count < 1 || count > 500) {
      throw new Error("count must be an integer from 1 to 500");
    }
    if (
      !Number.isInteger(payloadBytes) ||
      payloadBytes < 0 ||
      payloadBytes > 500_000
    ) {
      throw new Error("payloadBytes must be an integer from 0 to 500,000");
    }

    const blockerId = await pool.enqueueAction(
      ctx,
      internal.test.scenarios.enqueueOcc.blocker,
      { durationMs: blockerMs },
    );

    let blockerRunning = false;
    const startDeadline = Date.now() + 10_000;
    while (Date.now() < startDeadline) {
      const status = await ctx.runQuery(components.testWorkpool.lib.status, {
        id: blockerId,
      });
      if (status.state === "running") {
        blockerRunning = true;
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    // Wait past the normal two-second cooldown. A saturated pool now uses ten
    // seconds, so workers.status should still be "running" during the burst.
    await new Promise((resolve) => setTimeout(resolve, 2_500));

    const payload = "x".repeat(payloadBytes);
    const settled: PromiseSettledResult<WorkId>[] = await Promise.allSettled(
      Array.from({ length: count }, () =>
        ctx.runMutation(internal.test.scenarios.enqueueOcc.slowEnqueue, {
          payload,
          spinIterations,
        }),
      ),
    );
    const errors: string[] = settled
      .filter(
        (result): result is PromiseRejectedResult =>
          result.status === "rejected",
      )
      .map((result) => String(result.reason));
    const workersFailures = errors.filter((error) =>
      error.includes('"workers" table'),
    ).length;
    const workerStateFailures = errors.filter((error) =>
      error.includes('"workerState" table'),
    ).length;

    return {
      attempted: count,
      blockerStarted: blockerRunning,
      succeeded: count - errors.length,
      failed: errors.length,
      workersFailures,
      workerStateFailures,
      otherFailures: errors.length - workersFailures - workerStateFailures,
      errors: errors.slice(0, 10),
    };
  },
});
