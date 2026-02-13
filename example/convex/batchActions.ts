import { batch } from "./setup";
import { internal } from "./_generated/api";
import { v } from "convex/values";

export const translateToSpanish = batch.action("translateToSpanish", {
  args: { sentence: v.string() },
  handler: async (_ctx, { sentence }: { sentence: string }) => {
    // Mock: simulate LLM latency without hitting OpenAI
    await new Promise((r) => setTimeout(r, 3000 + Math.random() * 2000));
    return `[ES] ${sentence}`;
  },
});

export const translateToEnglish = batch.action("translateToEnglish", {
  args: { sentence: v.string() },
  handler: async (_ctx, { sentence }: { sentence: string }) => {
    // Mock: simulate LLM latency without hitting OpenAI
    await new Promise((r) => setTimeout(r, 3000 + Math.random() * 2000));
    return `[EN] ${sentence}`;
  },
});

export const countLetters = batch.action("countLetters", {
  args: { text: v.string() },
  handler: async (_ctx, { text }: { text: string }) => {
    return text.replace(/\s/g, "").length;
  },
});

// Export the executor — this is the long-lived action that runs many
// handlers concurrently inside a single Convex action.
export const executor = batch.executor();

// Tell the batch workpool about its own executor reference for self-scheduling.
batch.setExecutorRef(internal.batchActions.executor);
