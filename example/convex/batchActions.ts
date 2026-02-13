"use node";

import { batch } from "./setup";
import { internal } from "./_generated/api";
import { v } from "convex/values";

// ─── API helper ──────────────────────────────────────────────────────────────

type FetchResult = { text: string; fetchStart: number; fetchEnd: number };

async function callSonnet(prompt: string): Promise<FetchResult> {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) throw new Error("ANTHROPIC_API_KEY not set");
  const fetchStart = Date.now();
  const maxRetries = 8;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    const resp = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "x-api-key": apiKey,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
      },
      body: JSON.stringify({
        model: "claude-sonnet-4-20250514",
        max_tokens: 256,
        messages: [{ role: "user", content: prompt }],
      }),
    });
    if (resp.ok) {
      const data = (await resp.json()) as {
        content: Array<{ type: string; text: string }>;
      };
      return { text: data.content[0].text, fetchStart, fetchEnd: Date.now() };
    }
    const body = await resp.text();
    if (resp.status === 429 || resp.status === 529 || resp.status >= 500) {
      if (attempt < maxRetries) {
        // Respect Retry-After header if present, otherwise exponential backoff
        const retryAfter = resp.headers.get("retry-after");
        const delay = retryAfter
          ? parseFloat(retryAfter) * 1000
          : Math.min(1000 * Math.pow(2, attempt), 30000);
        await new Promise((r) => setTimeout(r, delay + Math.random() * 500));
        continue;
      }
    }
    console.error(`[callSonnet] FATAL ${resp.status}: ${body.slice(0, 500)}`);
    throw new Error(`Anthropic API ${resp.status}: ${body}`);
  }
  throw new Error("unreachable");
}

// ─── Batch actions ───────────────────────────────────────────────────────────

export const translateToSpanish = batch.action("translateToSpanish", {
  args: { sentence: v.string() },
  handler: async (_ctx, { sentence }: { sentence: string }) => {
    return await callSonnet(
      `Translate this sentence to Spanish. Reply with ONLY the translation, nothing else:\n${sentence}`,
    );
  },
}) as any; // Return type is FetchResult, not string

export const translateToEnglish = batch.action("translateToEnglish", {
  args: { sentence: v.string() },
  handler: async (_ctx, { sentence }: { sentence: string }) => {
    return await callSonnet(
      `Translate this sentence to English. Reply with ONLY the translation, nothing else:\n${sentence}`,
    );
  },
}) as any; // Return type is FetchResult, not string

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
