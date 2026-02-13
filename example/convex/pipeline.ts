import { internalMutation } from "./_generated/server";
import { internal } from "./_generated/api";
import { standard, batch } from "./setup";
import { vOnCompleteArgs } from "@convex-dev/workpool";
import { v } from "convex/values";

const vContext = v.object({ jobId: v.id("jobs") });

// ─── Standard mode pipeline (onComplete chaining) ───────────────────────────

export const standardAfterSpanish = internalMutation({
  args: vOnCompleteArgs(vContext),
  handler: async (ctx, { result, context }) => {
    if (result.kind !== "success") {
      await ctx.db.patch(context.jobId, { status: "failed" });
      return;
    }
    await ctx.db.patch(context.jobId, {
      spanish: result.returnValue,
      status: "step1",
    });
    await standard.enqueueAction(
      ctx,
      internal.standardActions.translateToEnglish,
      { sentence: result.returnValue },
      {
        onComplete: internal.pipeline.standardAfterEnglish,
        context: { jobId: context.jobId },
      },
    );
  },
});

export const standardAfterEnglish = internalMutation({
  args: vOnCompleteArgs(vContext),
  handler: async (ctx, { result, context }) => {
    if (result.kind !== "success") {
      await ctx.db.patch(context.jobId, { status: "failed" });
      return;
    }
    await ctx.db.patch(context.jobId, {
      backToEnglish: result.returnValue,
      status: "step2",
    });
    await standard.enqueueAction(
      ctx,
      internal.standardActions.countLetters,
      { text: result.returnValue },
      {
        onComplete: internal.pipeline.standardAfterCount,
        context: { jobId: context.jobId },
      },
    );
  },
});

export const standardAfterCount = internalMutation({
  args: vOnCompleteArgs(vContext),
  handler: async (ctx, { result, context }) => {
    if (result.kind !== "success") {
      await ctx.db.patch(context.jobId, { status: "failed" });
      return;
    }
    await ctx.db.patch(context.jobId, {
      letterCount: result.returnValue,
      status: "completed",
      completedAt: Date.now(),
    });
  },
});

// ─── Batch mode pipeline (onComplete chaining) ──────────────────────────────

export const batchAfterSpanish = internalMutation({
  args: vOnCompleteArgs(vContext),
  handler: async (ctx, { result, context }) => {
    if (result.kind !== "success") {
      await ctx.db.patch(context.jobId, { status: "failed" });
      return;
    }
    const { text, fetchStart, fetchEnd } = result.returnValue as {
      text: string;
      fetchStart: number;
      fetchEnd: number;
    };
    await ctx.db.patch(context.jobId, {
      spanish: text,
      status: "step1",
      fetch1Start: fetchStart,
      fetch1End: fetchEnd,
    });
    await batch.enqueue(
      ctx,
      "translateToEnglish",
      { sentence: text },
      {
        onComplete: internal.pipeline.batchAfterEnglish,
        context: { jobId: context.jobId },
      },
    );
  },
});

export const batchAfterEnglish = internalMutation({
  args: vOnCompleteArgs(vContext),
  handler: async (ctx, { result, context }) => {
    if (result.kind !== "success") {
      await ctx.db.patch(context.jobId, { status: "failed" });
      return;
    }
    const { text, fetchStart, fetchEnd } = result.returnValue as {
      text: string;
      fetchStart: number;
      fetchEnd: number;
    };
    await ctx.db.patch(context.jobId, {
      backToEnglish: text,
      status: "step2",
      fetch2Start: fetchStart,
      fetch2End: fetchEnd,
    });
    await batch.enqueue(
      ctx,
      "countLetters",
      { text },
      {
        onComplete: internal.pipeline.batchAfterCount,
        context: { jobId: context.jobId },
      },
    );
  },
});

export const batchAfterCount = internalMutation({
  args: vOnCompleteArgs(vContext),
  handler: async (ctx, { result, context }) => {
    if (result.kind !== "success") {
      await ctx.db.patch(context.jobId, { status: "failed" });
      return;
    }
    await ctx.db.patch(context.jobId, {
      letterCount: result.returnValue,
      status: "completed",
      completedAt: Date.now(),
    });
  },
});
