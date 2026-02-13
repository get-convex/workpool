import { internalAction } from "./_generated/server";
import { v } from "convex/values";

export const translateToSpanish = internalAction({
  args: { sentence: v.string() },
  handler: async (_ctx, { sentence }) => {
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

export const translateToEnglish = internalAction({
  args: { sentence: v.string() },
  handler: async (_ctx, { sentence }) => {
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

export const countLetters = internalAction({
  args: { text: v.string() },
  handler: async (_ctx, { text }) => {
    return text.replace(/\s/g, "").length;
  },
});
