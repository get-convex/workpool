import { internalAction } from "./_generated/server";
import { v } from "convex/values";

export const translateToSpanish = internalAction({
  args: { sentence: v.string() },
  handler: async (_ctx, { sentence }) => {
    // Mock: simulate LLM latency without hitting OpenAI
    await new Promise((r) => setTimeout(r, 3000 + Math.random() * 2000));
    return `[ES] ${sentence}`;
  },
});

export const translateToEnglish = internalAction({
  args: { sentence: v.string() },
  handler: async (_ctx, { sentence }) => {
    // Mock: simulate LLM latency without hitting OpenAI
    await new Promise((r) => setTimeout(r, 3000 + Math.random() * 2000));
    return `[EN] ${sentence}`;
  },
});

export const countLetters = internalAction({
  args: { text: v.string() },
  handler: async (_ctx, { text }) => {
    return text.replace(/\s/g, "").length;
  },
});
