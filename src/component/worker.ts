/**
 * Responsible for all the functions around doing the work.
 * Should not touch any of loop's tables other than writing to `pendingCompletion`.
 * It is not responsible for handling retries.
 */
import { v } from "convex/values";
import { internal } from "./_generated/api";
import { internalAction, internalMutation } from "./_generated/server";
import { createLogger } from "./logging.js";
import { nextSegment, runResult } from "./shared";
import { logLevel } from "./logging";
import { FunctionHandle } from "convex/server";
import { kickMainLoop } from "./kick";

export const runMutationWrapper = internalMutation({
  args: {
    workId: v.id("work"),
    fnHandle: v.string(),
    fnArgs: v.any(),
    logLevel,
  },
  handler: async (ctx, { workId, fnHandle: handleStr, fnArgs, logLevel }) => {
    const console = createLogger(logLevel);
    const fnHandle = handleStr as FunctionHandle<"mutation">;
    try {
      const returnValue = await ctx.runMutation(fnHandle, fnArgs);
      // NOTE: we could run the `saveResult` handler here, or call `ctx.runMutation`,
      // but we want the mutation to be a separate transaction to reduce the window for OCCs.
      await ctx.scheduler.runAfter(0, internal.worker.saveResult, {
        workId,
        runResult: { kind: "success", returnValue },
      });
    } catch (e: unknown) {
      console.error(e);
      await ctx.scheduler.runAfter(0, internal.worker.saveResult, {
        workId,
        runResult: { kind: "failed", error: formatError(e) },
      });
    }
  },
});

function formatError(e: unknown) {
  if (e instanceof Error) {
    return e.message;
  }
  return String(e);
}

export const runActionWrapper = internalAction({
  args: {
    workId: v.id("work"),
    fnHandle: v.string(),
    fnArgs: v.any(),
    logLevel,
  },
  handler: async (ctx, { workId, fnHandle: handleStr, fnArgs, logLevel }) => {
    const console = createLogger(logLevel);
    const fnHandle = handleStr as FunctionHandle<"action">;
    try {
      const returnValue = await ctx.runAction(fnHandle, fnArgs);
      // NOTE: we could run `ctx.runMutation`, but we want to guarantee execution,
      // and `ctx.scheduler.runAfter` won't OCC.
      await ctx.scheduler.runAfter(0, internal.worker.saveResult, {
        workId,
        runResult: { kind: "success", returnValue },
      });
    } catch (e: unknown) {
      console.error(e);
      // We let the main loop handle the retries.
      await ctx.scheduler.runAfter(0, internal.worker.saveResult, {
        workId,
        runResult: { kind: "failed", error: formatError(e) },
      });
    }
  },
});

export const saveResult = internalMutation({
  args: {
    workId: v.id("work"),
    runResult,
  },
  handler: async (ctx, { workId, runResult }) => {
    await ctx.db.insert("pendingCompletion", {
      runResult,
      workId,
      segment: nextSegment(),
    });
    await kickMainLoop(ctx, "saveResult");
  },
});

// eslint-disable-next-line @typescript-eslint/no-unused-vars
const console = "THIS IS A REMINDER TO USE createLogger";
