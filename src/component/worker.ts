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
import type { CompleteJob } from "./complete.js";
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

/** Legacy entrypoint, keeping around for graceful upgrade for scheduled functions in flight */
export const runActionWrapper = internalAction({
  args: {
    ...commonRunArgs,
    logLevel,
  },
  handler: async (ctx, { workId, attempt, ...args }) => {
    const console = createLogger(args.logLevel);

    const status = await runOne(ctx, console, {
      ...args,
      fnType: "action",
    });
    await completeInline(ctx, console, {
      workId,
      attempt,
      ...status,
    });
  },
});

export const runBatch = internalAction({
  args: {
    items: v.array(v.object(actionOrQueryRunArgs)),
    logLevel,
  },
  handler: async (ctx, { items, logLevel }) => {
    const console = createLogger(logLevel);
    await Promise.all(
      items.map(async (item) => {
        const status = await runOne(ctx, console, item);
        await completeInline(ctx, console, {
          workId: item.workId,
          attempt: item.attempt,
          ...status,
        });
      }),
    );
  },
});

/**
 * Complete a single action/query. Runs the completion — including the
 * onComplete callback — inline in its own transaction (called directly from
 * this action). If that hits an OCC we durably schedule the completion as a
 * batch of one; the scheduler retries it (and its inline onComplete) on OCC
 * until it succeeds. This is safe even for a job whose function already ran:
 * `complete` marks the work done exactly once, guarded by the work deletion.
 */
async function completeInline(
  ctx: ActionCtx,
  console: Logger,
  job: CompleteJob,
) {
  try {
    await ctx.runMutation(internal.complete.complete, { jobs: [job] });
  } catch (e) {
    console.error(
      `[runBatch] completing ${job.workId} inline failed, scheduling a batch of one instead: ${e}`,
    );
    await ctx.scheduler.runAfter(0, internal.complete.complete, {
      jobs: [job],
    });
  }
}

async function runOne(
  ctx: ActionCtx,
  console: Logger,
  args: {
    fnHandle: string;
    fnArgs?: Record<string, unknown>;
    payloadId?: Id<"payload">;
    fnType: "action" | "query";
  },
) {
  // Run the query directly from the action (its own snapshot — no completion
  // transaction to conflict with, and usage attributes to the query itself),
  // or the action. A transient failure of this wrapping action re-runs the
  // whole thing, which is safe for a query (side-effect free) because
  // `complete` still marks the work done exactly once.
  try {
    // Fetch args from payload if stored separately. Inside the try so a
    // missing/corrupt payload fails this item rather than the whole batch.
    let fnArgs = args.fnArgs;
    if (fnArgs === undefined) {
      assert(args.payloadId);
      fnArgs = await ctx.runQuery(internal.worker.getWorkArgs, {
        payloadId: args.payloadId,
      });
    }
    const returnValue =
      args.fnType === "action"
        ? await ctx.runAction(args.fnHandle as FunctionHandle<"action">, fnArgs)
        : await ctx.runQuery(args.fnHandle as FunctionHandle<"query">, fnArgs);
    const runResult: RunResult = { kind: "success", returnValue };
    return { runResult, runOnCompleteInline: true };
  } catch (e: unknown) {
    console.error(e);
    // We let the main loop handle the retries.
    return {
      runResult: { kind: "failed", error: formatError(e) } satisfies RunResult,
      nonRetryable: isNonRetryableError(e),
    };
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
