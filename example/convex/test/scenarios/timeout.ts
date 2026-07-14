import { internalAction, internalMutation } from "../../_generated/server";
import { v } from "convex/values";
import { components, internal } from "../../_generated/api";
import { Workpool, vOnCompleteArgs, WorkId } from "@convex-dev/workpool";

/**
 * Timed-out mutation recovery scenario.
 *
 * A mutation that calls `ctx.runMutation` thousands of times back-to-back blows
 * past the mutation execution-time limit. When that happens the whole
 * transaction is killed: the workpool's `runMutationWrapper` never reaches its
 * try/catch, so it never writes a completion. The work just sits in
 * `internalState.running` until the main loop's recovery pass (every ~1min,
 * for jobs silent >5min) notices it, reads the scheduled function's terminal
 * "failed" state, and turns it into a completion.
 *
 * This scenario triggers a *real* timeout and verifies the work converges to a
 * terminal failed state instead of being retried forever. (Mutations are never
 * retried by the workpool, so the expected outcome is exactly one attempt that
 * recovers into a single terminal failure — onComplete fires once.)
 *
 * Run (expect it to take >5min — recovery only fires for jobs silent >5min):
 *   npx convex run test/scenarios/timeout:default '{"calls":10000}'
 *
 * Watch progress in another shell:
 *   npx convex run test/scenarios/timeout:report --watch
 */

const TIMEOUT_MARKER = "timeoutScenario";

const pool = new Workpool(components.testWorkpool, {
  maxParallelism: 5,
  logLevel: "INFO",
});

// A no-op mutation; the timing-out worker calls this thousands of times.
export const noop = internalMutation({
  args: {},
  handler: async () => {},
});

/**
 * The work that times out: `calls` sequential `ctx.runMutation` calls. With
 * calls=10000 this reliably exceeds the mutation time limit and the whole
 * transaction is killed mid-flight (uncatchable — recovery must handle it).
 */
export const timingOutMutation = internalMutation({
  args: { calls: v.number() },
  handler: async (ctx, { calls }) => {
    for (let i = 0; i < calls; i++) {
      await ctx.runMutation(internal.test.scenarios.timeout.noop, {});
    }
    // We never expect to reach here for large `calls`.
    return calls;
  },
});

export const scheduleTimeout = internalMutation({
  handler: async (ctx) => {
    await ctx.scheduler.runAfter(
      0,
      internal.test.scenarios.timeout.timingOutMutation,
      { calls: 10000 },
    );
  },
});

// Records the single terminal result. If the work were retried forever, this
// would never run.
export const onTimeoutComplete = internalMutation({
  args: vOnCompleteArgs(v.object({ enqueuedAt: v.number() })),
  handler: async (ctx, args) => {
    await ctx.db.insert("data", {
      misc: {
        kind: TIMEOUT_MARKER,
        resultKind: args.result.kind,
        error: args.result.kind === "failed" ? args.result.error : undefined,
        enqueuedAt: args.context.enqueuedAt,
        completedAt: Date.now(),
      },
    });
  },
});

// Snapshot of progress: the work's live status + whether onComplete has fired.
export const report = internalMutation({
  args: { workId: v.optional(v.string()) },
  handler: async (ctx, { workId }) => {
    const docs = await ctx.db.query("data").collect();
    const marker = docs
      .map((d) => d.misc)
      .find(
        (m) =>
          m &&
          typeof m === "object" &&
          (m as { kind?: string }).kind === TIMEOUT_MARKER,
      ) as
      | {
          resultKind: string;
          error?: string;
          enqueuedAt: number;
          completedAt: number;
        }
      | undefined;
    const status = workId ? await pool.status(ctx, workId as WorkId) : null;
    return {
      status,
      completed: marker
        ? {
            resultKind: marker.resultKind,
            error: marker.error,
            elapsedMs: marker.completedAt - marker.enqueuedAt,
          }
        : null,
    };
  },
});

/**
 * Enqueue the timing-out mutation and return immediately with its workId.
 * Recovery takes >5min, which is longer than an action can run, so we don't
 * poll inside the action — poll `report`/`status` from outside instead:
 *
 *   id=$(npx convex run test/scenarios/timeout:default '{"calls":10000}' | jq -r .workId)
 *   npx convex run test/scenarios/timeout:report "{\"workId\":\"$id\"}" --watch
 */
export default internalAction({
  args: { calls: v.optional(v.number()) },
  handler: async (ctx, { calls = 10000 }): Promise<{ workId: string }> => {
    // Clear any prior marker so we measure a fresh run.
    await ctx.runMutation(internal.test.scenarios.timeout.clearMarkers, {});

    const enqueuedAt = Date.now();
    const workId = await pool.enqueueMutation(
      ctx,
      internal.test.scenarios.timeout.timingOutMutation,
      { calls },
      {
        onComplete: internal.test.scenarios.timeout.onTimeoutComplete,
        context: { enqueuedAt },
      },
    );
    console.log(
      `Enqueued timing-out mutation ${workId} (${calls} runMutation calls). ` +
        `Recovery fires for jobs silent >5min, so expect a multi-minute wait.`,
    );
    return { workId };
  },
});

export const clearMarkers = internalMutation({
  args: {},
  handler: async (ctx) => {
    const docs = await ctx.db.query("data").collect();
    for (const d of docs) {
      const m = d.misc as { kind?: string } | undefined;
      if (m && m.kind === TIMEOUT_MARKER) {
        await ctx.db.delete("data", d._id);
      }
    }
  },
});
