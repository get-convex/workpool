/**
 * Responsible for all the functions around doing the work.
 * Should not touch any of loop's tables other than writing to `pendingCompletion`.
 * It is not responsible for handling retries.
 */
import type { FunctionHandle } from "convex/server";
import { v } from "convex/values";
import { internal } from "./_generated/api.js";
import type { Id } from "./_generated/dataModel.js";
import {
  internalAction,
  internalMutation,
  internalQuery,
  type ActionCtx,
} from "./_generated/server.js";
import { getNonRetryableErrorMessage, isNonRetryableError } from "./errors.js";
import { createLogger, type Logger, logLevel } from "./logging.js";
import type { RunResult } from "./shared.js";
import { assert } from "convex-helpers";

const commonRunArgs = {
  workId: v.id("work"),
  fnHandle: v.string(),
  fnArgs: v.optional(v.record(v.string(), v.any())),
  payloadId: v.optional(v.id("payload")),
  attempt: v.number(),
};

const actionOrQueryRunArgs = {
  ...commonRunArgs,
  fnType: v.union(v.literal("action"), v.literal("query")),
};

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
      const payload = await ctx.db.get("payload", args.payloadId);
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
        jobs: [
          {
            workId,
            runResult,
            attempt,
            nonRetryable: isNonRetryableError(e),
          },
        ],
      });
    }
  },
});

function formatError(e: unknown) {
  const nonRetryableMessage = getNonRetryableErrorMessage(e);
  if (nonRetryableMessage !== undefined) {
    return nonRetryableMessage;
  }
  if (e instanceof Error) {
    return e.message;
  }
  return String(e);
}

export const runActionWrapper = internalAction({
  args: {
    ...commonRunArgs,
    logLevel,
  },
  handler: async (ctx, { workId, attempt, ...args }) => {
    const console = createLogger(args.logLevel);

    await completeActionOrQueryJobs(ctx, console, [
      await runActionOrQueryJob(ctx, console, {
        workId,
        attempt,
        ...args,
        fnType: "action",
      }),
    ]);
  },
});

export const runWork = internalAction({
  args: {
    items: v.array(v.object(actionOrQueryRunArgs)),
    logLevel,
  },
  handler: async (ctx, { items, logLevel }) => {
    const console = createLogger(logLevel);
    const completionJobs = await Promise.all(
      items.map((item) => runActionOrQueryJob(ctx, console, item)),
    );
    await completeActionOrQueryJobs(ctx, console, completionJobs);
  },
});

async function runActionOrQueryJob(
  ctx: ActionCtx,
  console: Logger,
  args: {
    workId: Id<"work">;
    fnHandle: string;
    fnArgs?: Record<string, unknown>;
    payloadId?: Id<"payload">;
    attempt: number;
    fnType: "action" | "query";
  },
) {
  const { workId, attempt } = args;

  // Fetch args from payload if stored separately
  let fnArgs = args.fnArgs;
  if (fnArgs === undefined) {
    assert(args.payloadId);
    fnArgs = await ctx.runQuery(internal.worker.getWorkArgs, {
      payloadId: args.payloadId,
    });
  }

  try {
    const returnValue =
      args.fnType === "action"
        ? await ctx.runAction(args.fnHandle as FunctionHandle<"action">, fnArgs)
        : await ctx.runQuery(args.fnHandle as FunctionHandle<"query">, fnArgs);
    const runResult: RunResult = { kind: "success", returnValue };
    return { workId, runResult, attempt, runOnCompleteInline: true };
  } catch (e: unknown) {
    console.error(e);
    // We let the main loop handle the retries.
    return {
      workId,
      runResult: { kind: "failed", error: formatError(e) } satisfies RunResult,
      attempt,
      nonRetryable: isNonRetryableError(e),
    };
  }
}

async function completeActionOrQueryJobs(
  ctx: ActionCtx,
  console: Logger,
  jobs: Array<{
    workId: Id<"work">;
    runResult: RunResult;
    attempt: number;
    nonRetryable?: boolean;
    runOnCompleteInline?: boolean;
  }>,
) {
  try {
    // Attempt to run complete inline and onComplete inline for successful jobs.
    await ctx.runMutation(internal.complete.complete, { jobs });
    console.info("[runWork] onComplete succeeded");
  } catch (e) {
    console.error(
      `[runWork] caught error while attempting to run complete inline, scheduling instead: ${e}`,
    );
    // Fall through and schedule complete instead (without running onComplete inline)
    await ctx.scheduler.runAfter(0, internal.complete.complete, {
      jobs: jobs.map(
        ({ runOnCompleteInline: _runOnCompleteInline, ...job }) => job,
      ),
    });
  }
}

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
