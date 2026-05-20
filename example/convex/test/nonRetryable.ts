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

const ATTEMPT_EVENT = "nonRetryableAttempt";
const COMPLETION_EVENT = "nonRetryableCompletion";

function isEvent(
  value: unknown,
  kind: string,
): value is Record<string, unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    (value as { kind?: unknown }).kind === kind
  );
}

export const recordTerminalAttempt = internalMutation({
  args: {},
  handler: async (ctx) => {
    await ctx.db.insert("data", {
      misc: { kind: ATTEMPT_EVENT },
    });
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
      misc:
        args.result.kind === "failed"
          ? { kind: COMPLETION_EVENT, error: args.result.error }
          : { kind: COMPLETION_EVENT },
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

export const terminalAttemptCount = query({
  args: {},
  handler: async (ctx) => {
    const docs = await ctx.db.query("data").collect();
    return docs.filter((doc) => isEvent(doc.misc, ATTEMPT_EVENT)).length;
  },
});

export const terminalCompletionErrors = query({
  args: {},
  handler: async (ctx) => {
    const docs = await ctx.db.query("data").collect();
    return docs.flatMap((doc) => {
      if (!isEvent(doc.misc, COMPLETION_EVENT)) {
        return [];
      }
      const error = doc.misc.error;
      return typeof error === "string" ? [error] : [];
    });
  },
});

export const terminalMutation = internalMutation({
  args: {},
  handler: async () => {
    throw new NonRetryableError("terminal mutation failure");
  },
});

export const enqueueTerminalMutationWithRetry = mutation({
  args: {},
  handler: async (ctx): Promise<WorkId> => {
    return await smallPool.enqueueMutation(
      ctx,
      internal.test.nonRetryable.terminalMutation,
      {},
      {
        onComplete: internal.test.nonRetryable.completeTerminalAction,
        context: null,
      },
    );
  },
});
