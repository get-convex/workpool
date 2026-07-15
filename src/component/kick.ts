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
  SECOND,
  toSegment,
} from "./shared.js";

/**
 * Wakes the main loop if it isn't already running. No-ops when a wake-up
 * wouldn't be productive (e.g. enqueue while saturated).
 */
export async function kickMainLoop(
  ctx: MutationCtx,
  source: "enqueue" | "cancel" | "complete" | "retry" | "kick",
  config?: Config,
): Promise<void> {
  const globals = config ?? (await getOrUpdateGlobals(ctx, config));
  const console = createLogger(globals.logLevel);
  const runStatus = await getOrCreateRunStatus(ctx);

  // Only kick to run now if we're scheduled or idle.
  if (runStatus.state.kind === "running") {
    console.debug(
      `[${source}] main is actively running, so we don't need to kick it`,
    );
    return;
  }
  // main is scheduled to run later, so we should cancel it and reschedule.
  if (runStatus.state.kind === "scheduled") {
    if (source === "enqueue" && runStatus.state.saturated) {
      console.debug(
        `[${source}] main is saturated, so we don't need to kick it`,
      );
      return;
    }
    if (source === "complete" && !runStatus.state.saturated) {
      // A pure completion doesn't create new work to start, so the loop's
      // existing wake-up is fine. Retries (source "retry") do create new
      // pendingStart work and fall through to kick the loop promptly.
      console.debug(
        `[${source}] main is not saturated, so kicking for completion isn't necessary`,
      );
      return;
    }
    if (runStatus.state.segment <= toSegment(Date.now() + SECOND)) {
      console.debug(
        `[${source}] main is scheduled to run soon enough, so we don't need to kick it`,
      );
      return;
    }
    console.debug(
      `[${source}] main is scheduled to run later, so reschedule it to run now`,
    );
    const scheduled = await ctx.db.system.get(
      "_scheduled_functions",
      runStatus.state.scheduledId,
    );
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
  await ctx.db.patch("runStatus", runStatus._id, {
    state: { kind: "running" },
  });
  const scheduledTime = boundScheduledTime(
    fromSegment(getCurrentSegment()),
    console,
  );
  await ctx.scheduler.runAt(scheduledTime, internal.loop.main, {
    generation: runStatus.state.generation,
  });
}

export const forceKick = internalMutation({
  args: {},
  handler: async (ctx) => {
    const runStatus = await getOrCreateRunStatus(ctx);
    if (runStatus.state.kind === "scheduled") {
      const scheduled = await ctx.db.system.get(
        "_scheduled_functions",
        runStatus.state.scheduledId,
      );
      if (scheduled && scheduled.state.kind === "pending") {
        await ctx.scheduler.cancel(runStatus.state.scheduledId);
      }
    }
    await ctx.db.delete("runStatus", runStatus._id);
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
    runStatus = (await ctx.db.get("runStatus", id))!;
    if (!state) {
      await ctx.db.insert("internalState", INITIAL_STATE);
    }
  }
  return runStatus;
}
