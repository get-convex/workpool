import { createFunctionHandle } from "convex/server";
import { v } from "convex/values";
import {
  mutation,
  query,
  internalAction,
  internalMutation,
} from "../_generated/server";
import { components, internal } from "../_generated/api";
import {
  WorkId,
  NonRetryableError,
  Workpool,
  vOnCompleteArgs,
} from "@convex-dev/workpool";

const smallPool = new Workpool(components.smallPool, {
  maxParallelism: 3,
  retryActionsByDefault: true,
  logLevel: "INFO",
});

let terminalMutationAttempts = 0;

export const recordTerminalAttempt = internalMutation({
  args: {},
  handler: async (ctx) => {
    await ctx.db.insert("data", { data: 1 });
  },
});

export const terminalAction = internalAction({
  args: {},
  handler: async (ctx) => {
    await ctx.runMutation(internal.test.nonRetryable.recordTerminalAttempt, {});
    throw new NonRetryableError("terminal failure");
  },
});

export const completeTerminalAction = internalMutation({
  args: vOnCompleteArgs(v.null()),
  handler: async (ctx, args) => {
    await ctx.db.insert("data", {
      data: args.result.kind === "failed" ? 999 : -1,
      misc: args.result,
    });
  },
});

export const enqueueTerminalAction = mutation({
  args: {},
  handler: async (ctx): Promise<WorkId> => {
    return await smallPool.enqueueAction(
      ctx,
      internal.test.nonRetryable.terminalAction,
      {},
      {
        retry: { maxAttempts: 3, initialBackoffMs: 100, base: 2 },
        onComplete: internal.test.nonRetryable.completeTerminalAction,
        context: null,
      },
    );
  },
});

export const resetTerminalMutationAttempts = mutation({
  args: {},
  handler: async () => {
    terminalMutationAttempts = 0;
  },
});

export const terminalMutationAttemptCount = query({
  args: {},
  handler: async () => terminalMutationAttempts,
});

export const terminalCompletionResults = query({
  args: {},
  handler: async (ctx) => {
    const docs = await ctx.db.query("data").collect();
    return docs.flatMap((doc) => (doc.misc === undefined ? [] : [doc.misc]));
  },
});

export const terminalMutation = internalMutation({
  args: {},
  handler: async () => {
    terminalMutationAttempts++;
    throw new NonRetryableError("terminal mutation failure");
  },
});

export const enqueueTerminalMutationWithRetry = mutation({
  args: {},
  handler: async (ctx): Promise<WorkId> => {
    const [fnHandle, onCompleteHandle] = await Promise.all([
      createFunctionHandle(internal.test.nonRetryable.terminalMutation),
      createFunctionHandle(internal.test.nonRetryable.completeTerminalAction),
    ]);

    return (await ctx.runMutation(components.smallPool.lib.enqueue, {
      fnHandle,
      fnName: "test/nonRetryable:terminalMutation",
      fnArgs: {},
      fnType: "mutation",
      runAt: Date.now(),
      onComplete: { fnHandle: onCompleteHandle, context: null },
      retryBehavior: { maxAttempts: 3, initialBackoffMs: 100, base: 2 },
      config: { logLevel: "INFO" },
    })) as WorkId;
  },
});
