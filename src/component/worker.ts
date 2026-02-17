/**
 * Responsible for all the functions around doing the work.
 * Should not touch any of loop's tables other than writing to `pendingCompletion`.
 * It is not responsible for handling retries.
 */
import type { FunctionHandle } from "convex/server";
import { v } from "convex/values";
import { internal } from "./_generated/api.js";
import {
  internalAction,
  internalMutation,
  internalQuery,
} from "./_generated/server.js";
import { createLogger, logLevel } from "./logging.js";
import type { RunResult } from "./shared.js";
import { assert } from "convex-helpers";

export const runMutationWrapper = internalMutation({
  args: {
    workId: v.id("work"),
    fnHandle: v.string(),
    payloadId: v.optional(v.id("payload")),
    fnArgs: v.optional(v.record(v.string(), v.any())),
    fnType: v.union(v.literal("query"), v.literal("mutation")),
    logLevel,
    attempt: v.number(),
  },
  handler: async (ctx, { workId, attempt, ...args }) => {
    const console = createLogger(args.logLevel);

    let fnArgs = args.fnArgs;
    if (!fnArgs) {
      assert(args.payloadId);
      const payload = await ctx.db.get(args.payloadId);
      assert(payload?.args);
      fnArgs = payload.args;
    }

    try {
      const returnValue = await (args.fnType === "query"
        ? ctx.runQuery(args.fnHandle as FunctionHandle<"query">, fnArgs)
        : ctx.runMutation(args.fnHandle as FunctionHandle<"mutation">, fnArgs));
      // NOTE: we could run the `saveResult` handler here, or call `ctx.runMutation`,
      // but we want the mutation to be a separate transaction to reduce the window for OCCs.
      await ctx.scheduler.runAfter(0, internal.complete.complete, {
        jobs: [
          { workId, runResult: { kind: "success", returnValue }, attempt },
        ],
      });
    } catch (e: unknown) {
      console.error(e);
      const runResult = { kind: "failed" as const, error: formatError(e) };
      await ctx.scheduler.runAfter(0, internal.complete.complete, {
        jobs: [{ workId, runResult, attempt }],
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
    fnArgs: v.optional(v.record(v.string(), v.any())),
    payloadId: v.optional(v.id("payload")),
    logLevel,
    attempt: v.number(),
  },
  handler: async (ctx, { workId, attempt, ...args }) => {
    const console = createLogger(args.logLevel);

    // Fetch args from payload if stored separately
    let fnArgs = args.fnArgs;
    if (fnArgs === undefined) {
      assert(args.payloadId);
      fnArgs = await ctx.runQuery(internal.worker.getWorkArgs, {
        payloadId: args.payloadId,
      });
    }

    const fnHandle = args.fnHandle as FunctionHandle<"action">;
    try {
      const returnValue = await ctx.runAction(fnHandle, fnArgs);
      // NOTE: we could run `ctx.runMutation`, but we want to guarantee execution,
      // and `ctx.scheduler.runAfter` won't OCC.
      const runResult: RunResult = { kind: "success", returnValue };
      await ctx.scheduler.runAfter(0, internal.complete.complete, {
        jobs: [{ workId, runResult, attempt }],
      });
    } catch (e: unknown) {
      console.error(e);
      // We let the main loop handle the retries.
      const runResult: RunResult = { kind: "failed", error: formatError(e) };
      await ctx.scheduler.runAfter(0, internal.complete.complete, {
        jobs: [{ workId, runResult, attempt }],
      });
    }
  },
});

// Helper mutation for actions to fetch work args
export const getWorkArgs = internalQuery({
  args: {
    payloadId: v.id("payload"),
  },
  returns: v.record(v.string(), v.any()),
  handler: async (ctx, args) => {
    const payload = await ctx.db.get("payload", args.payloadId);
    assert(payload);
    assert(payload.args);
    return payload.args;
  },
});

// eslint-disable-next-line @typescript-eslint/no-unused-vars
const console = "THIS IS A REMINDER TO USE createLogger";
