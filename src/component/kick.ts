import { ping } from "@convex-dev/batch-worker";
import { components, internal } from "./_generated/api.js";
import { internalMutation, type MutationCtx } from "./_generated/server.js";
import { MINUTE, WORKER_NAME } from "./shared.js";

type KickSource = "enqueue" | "cancel" | "complete" | "retry" | "kick";

/**
 * Ensure the batch-worker loop is running, (re)registering our work query and
 * worker mutation. `ping` is cheap and idempotent — a no-op while the loop is
 * already running. We intentionally always ping instead of reading
 * internalState.running: that table changes constantly, so adding it as a
 * dependency here makes enqueue/complete/cancel much more likely to OCC.
 */
export async function kickMainLoop(
  ctx: MutationCtx,
  _source: KickSource = "kick",
): Promise<void> {
  await ping(ctx, components.batchWorker, {
    name: WORKER_NAME,
    workQuery: internal.loop.getBatch,
    workerMutation: internal.loop.run,
    // Restart the loop within ~2min if it ever dies between iterations.
    config: { monitorLagMs: 2 * MINUTE },
  });
}

/**
 * Hard reset: stop the worker, then start it again (reusing the stored
 * query/mutation/config). A manual escape hatch if the loop ever wedges.
 */
export const forceKick = internalMutation({
  args: {},
  handler: async (ctx) => {
    await ctx.runMutation(components.batchWorker.lib.stop, {
      name: WORKER_NAME,
    });
    await ctx.runMutation(components.batchWorker.lib.start, {
      name: WORKER_NAME,
    });
  },
});
