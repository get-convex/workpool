import { internal } from "./_generated/api.js";
import { internalMutation, type MutationCtx } from "./_generated/server.js";
import { getOrUpdateGlobals } from "./config.js";
import { createLogger } from "./logging.js";
import { INITIAL_STATE } from "./loop.js";
import {
  boundScheduledTime,
  type Config,
  fromSegment,
  getCurrentSegment,
  getNextSegment,
  SECOND,
  toSegment,
} from "./shared.js";

/**
 * Called from outside the loop.
 * Returns the soonest segment to enqueue work for the main loop.
 */
export async function kickMainLoop(
  ctx: MutationCtx,
  source: "enqueue" | "cancel" | "complete" | "kick",
  config?: Config,
): Promise<bigint> {
  const globals = config ?? (await getOrUpdateGlobals(ctx, config));
  const console = createLogger(globals.logLevel);
  const runStatus = await getOrCreateRunStatus(ctx);
  const next = getNextSegment();

  // Only kick to run now if we're scheduled or idle.
  if (runStatus.state.kind === "running") {
    console.debug(
      `[${source}] main is actively running, so we don't need to kick it`,
    );
    return next;
  }
  // main is scheduled to run later, so we should cancel it and reschedule.
  if (runStatus.state.kind === "scheduled") {
    if (source === "enqueue" && runStatus.state.saturated) {
      console.debug(
        `[${source}] main is saturated, so we don't need to kick it`,
      );
      return next;
    }
    if (runStatus.state.segment <= toSegment(Date.now() + SECOND)) {
      console.debug(
        `[${source}] main is scheduled to run soon enough, so we don't need to kick it`,
      );
      return next;
    }
    console.debug(
      `[${source}] main is scheduled to run later, so reschedule it to run now`,
    );
    const scheduled = await ctx.db.system.get(runStatus.state.scheduledId);
    if (scheduled && scheduled.state.kind === "pending") {
      await ctx.scheduler.cancel(runStatus.state.scheduledId);
    } else {
      console.warn(
        `[${source}] main is marked as scheduled, but it's status is ${scheduled?.state.kind}`,
      );
    }
  } else if (runStatus.state.kind === "idle") {
    console.debug(`[${source}] main was idle, so run it now`);
  }
  await ctx.db.patch(runStatus._id, { state: { kind: "running" } });
  const current = getCurrentSegment();
  const scheduledTime = boundScheduledTime(fromSegment(current), console);
  await ctx.scheduler.runAt(scheduledTime, internal.loop.main, {
    generation: runStatus.state.generation,
    segment: current,
  });
  return current;
}

export const forceKick = internalMutation({
  args: {},
  handler: async (ctx) => {
    const runStatus = await getOrCreateRunStatus(ctx);
    await ctx.db.delete(runStatus._id);
    await kickMainLoop(ctx, "kick");
  },
});

async function getOrCreateRunStatus(ctx: MutationCtx) {
  let runStatus = await ctx.db.query("runStatus").unique();
  if (!runStatus) {
    const state = await ctx.db.query("internalState").unique();
    const id = await ctx.db.insert("runStatus", {
      state: {
        kind: "idle",
        generation: state?.generation ?? INITIAL_STATE.generation,
      },
    });
    runStatus = (await ctx.db.get(id))!;
    if (!state) {
      await ctx.db.insert("internalState", INITIAL_STATE);
    }
  }
  return runStatus;
}
