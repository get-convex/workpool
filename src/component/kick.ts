import { internal } from "./_generated/api";
import { MutationCtx } from "./_generated/server";
import { createLogger } from "./logging";
import { INITIAL_STATE } from "./loop";
import { Config, nextSegment } from "./shared";

export const DEFAULT_MAX_PARALLELISM = 10;
/**
 * Called from outside the loop:
 */

export async function kickMainLoop(
  ctx: MutationCtx,
  source: "enqueue" | "cancel" | "saveResult" | "recovery",
  config?: Partial<Config>
): Promise<void> {
  const globals = await getOrUpdateGlobals(ctx, config);
  const console = createLogger(globals.logLevel);
  const runStatus = await getOrCreateRunStatus(ctx);

  // Only kick to run now if we're scheduled or idle.
  // TODO: is this a race?
  if (runStatus.state.kind === "running") {
    console.debug(
      `[${source}] mainLoop is actively running, so we don't need to kick it`
    );
    return;
  }
  const segment = nextSegment();
  // mainLoop is scheduled to run later, so we should cancel it and reschedule.
  if (runStatus.state.kind === "scheduled") {
    if (source === "enqueue" && runStatus.state.saturated) {
      console.debug(
        `[${source}] mainLoop is saturated, so we don't need to kick it`
      );
      return;
    }
    if (runStatus.state.segment <= segment) {
      console.debug(
        `[${source}] mainLoop is scheduled to run soon enough, so we don't need to kick it`
      );
      return;
    }
    // TODO: check that it's still pending first
    console.debug(
      `[${source}] mainLoop is scheduled to run later, so reschedule it to run now`
    );
    await ctx.scheduler.cancel(runStatus.state.scheduledId);
  }
  console.debug(
    `[${source}] mainLoop was scheduled later, so reschedule it to run now`
  );
  await ctx.db.patch(runStatus._id, { state: { kind: "running" } });
  await ctx.scheduler.runAfter(0, internal.loop.mainLoop, {
    generation: runStatus.state.generation,
    segment,
  });
}

async function getOrCreateRunStatus(ctx: MutationCtx) {
  let runStatus = await ctx.db.query("runStatus").unique();
  if (!runStatus) {
    const id = await ctx.db.insert("runStatus", {
      state: { kind: "idle", generation: 0n },
    });
    runStatus = (await ctx.db.get(id))!;
    const state = await ctx.db.query("internalState").unique();
    if (!state) {
      await ctx.db.insert("internalState", INITIAL_STATE);
    }
  }
  return runStatus;
}

async function getOrUpdateGlobals(ctx: MutationCtx, config?: Partial<Config>) {
  const globals = await ctx.db.query("globals").unique();
  if (!globals) {
    const id = await ctx.db.insert("globals", {
      maxParallelism: config?.maxParallelism ?? DEFAULT_MAX_PARALLELISM,
      logLevel: config?.logLevel,
    });
    return (await ctx.db.get(id))!;
  } else if (config) {
    let updated = false;
    if (
      config.maxParallelism &&
      config.maxParallelism !== globals.maxParallelism
    ) {
      globals.maxParallelism = config.maxParallelism;
      updated = true;
    }
    if (config.logLevel && config.logLevel !== globals.logLevel) {
      globals.logLevel = config.logLevel;
      updated = true;
    }
    if (updated) {
      await ctx.db.replace(globals._id, globals);
    }
  }
  return globals;
}
