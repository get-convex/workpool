import {
  mutation,
  action,
  query,
  internalMutation,
  internalAction,
} from "./_generated/server";
import { api, components, internal } from "./_generated/api";
import { WorkId, WorkPool } from "@convex-dev/workpool";
import { v } from "convex/values";

const pool = new WorkPool(components.workpool, {
  maxParallelism: 3,
  // For tests, disable completed work cleanup.
  ttl: Number.POSITIVE_INFINITY,
});
const lowpriPool = new WorkPool(components.lowpriWorkpool, {
  maxParallelism: 1,
  // For tests, disable completed work cleanup.
  ttl: Number.POSITIVE_INFINITY,
});

export const addNow = mutation({
  args: { data: v.optional(v.number()) },
  handler: async (ctx, { data }) => {
    const d = data ?? Math.random();
    await ctx.db.insert("data", { data: d });
  },
});

export const addLater = mutation({
  args: { data: v.optional(v.number()) },
  handler: async (ctx, { data }) => {
    // XXX make work with actions too
    await pool.enqueueMutation(ctx, api.example.addNow, { data });
  },
});

export const list = query({
  args: {},
  handler: async (ctx, {}) => {
    return ctx.db.query("data").collect();
  },
});

export const status = query({
  args: { id: v.string() },
  handler: async (ctx, { id }) => {
    return await pool.status(ctx, id as WorkId<null>);
  },
});

export const enqueueABunchOfMutations = mutation({
  args: {},
  handler: async (ctx, _args) => {
    for (let i = 0; i < 30; i++) {
      await pool.enqueueMutation(ctx, api.example.addNow, {});
    }
  },
});

export const addLowPri = mutation({
  args: { data: v.optional(v.number()) },
  handler: async (ctx, { data }) => {
    const d = -(data ?? Math.random());
    await ctx.db.insert("data", { data: d });
    return d;
  },
});

export const enqueueLowPriMutations = mutation({
  args: {},
  handler: async (ctx, _args) => {
    for (let i = 0; i < 30; i++) {
      await lowpriPool.enqueueMutation(ctx, api.example.addLowPri, {});
    }
  },
});

async function sampleWork() {
  const index = Math.floor(Math.random() * 3000) + 1;
  const url = `https://xkcd.com/${index}`;
  const response = await fetch(url);
  const text = await response.text();
  const titleMatch = text.match(/<title>(.*)<\/title>/);
  console.log(`xkcd ${index} title: ${titleMatch?.[1]}`);
}

// Example background work: scraping from a website.
export const backgroundWork = internalAction({
  args: {},
  handler: async () => {
    await sampleWork();
  },
});

export const startBackgroundWork = internalMutation({
  args: {},
  handler: async (ctx, _args) => {
    for (let i = 0; i < 20; i++) {
      await lowpriPool.enqueueAction(ctx, internal.example.backgroundWork, {});
    }
  },
});

// Example foreground work: calling an API on behalf of a user.
export const foregroundWork = internalAction({
  args: {},
  handler: async () => {
    await sampleWork();
  },
});

export const startForegroundWork = internalMutation({
  args: {},
  handler: async (ctx, _args) => {
    await pool.enqueueAction(ctx, internal.example.foregroundWork, {});
  },
});
