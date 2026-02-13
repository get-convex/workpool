import { batch } from "./setup";
import { internal } from "./_generated/api";
import { v } from "convex/values";

export const translateToSpanish = batch.action("translateToSpanish", {
  args: { sentence: v.string() },
  handler: async (_ctx, { sentence }: { sentence: string }) => {
    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages: [
          {
            role: "system",
            content:
              "Translate the following to Spanish. Return only the translation.",
          },
          { role: "user", content: sentence },
        ],
      }),
    });
    const data = await response.json();
    return data.choices[0].message.content;
  },
});

export const translateToEnglish = batch.action("translateToEnglish", {
  args: { sentence: v.string() },
  handler: async (_ctx, { sentence }: { sentence: string }) => {
    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages: [
          {
            role: "system",
            content:
              "Translate the following to English. Return only the translation.",
          },
          { role: "user", content: sentence },
        ],
      }),
    });
    const data = await response.json();
    return data.choices[0].message.content;
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
