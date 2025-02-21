import { mutation, action, query, internalAction } from "./_generated/server";
import { api, components, internal } from "./_generated/api";
import { Workpool } from "@convex-dev/workpool";
import { v } from "convex/values";

const bigPool = new Workpool(components.bigPool, {
  maxParallelism: 20,
  defaultRetryBehavior: {
    maxAttempts: 3,
    initialBackoffMs: 100,
    base: 2,
  },
  retryActionsByDefault: true,
  logLevel: "INFO",
});
const smallPool = new Workpool(components.smallPool, {
  maxParallelism: 3,
  retryActionsByDefault: true,
  logLevel: "INFO",
});
const serializedPool = new Workpool(components.serializedPool, {
  maxParallelism: 1,
  retryActionsByDefault: true,
  logLevel: "INFO",
});

export const addMutation = mutation({
  args: { data: v.optional(v.number()) },
  handler: async (ctx, { data }) => {
    const d = data ?? Math.random();
    await ctx.db.insert("data", { data: d });
    return d;
  },
});

export const addAction = action({
  args: { data: v.optional(v.number()) },
  handler: async (ctx, { data }): Promise<number> => {
    return await ctx.runMutation(api.example.addMutation, { data });
  },
});

export const queryData = query({
  args: {},
  handler: async (ctx) => {
    const dataDocs = await ctx.db.query("data").collect();
    return dataDocs.map((doc) => doc.data);
  },
});

export const enqueueOneMutation = mutation({
  args: { data: v.number() },
  handler: async (ctx, { data }): Promise<string> => {
    return await smallPool.enqueueMutation(ctx, api.example.addMutation, {
      data,
    });
  },
});

export const cancelMutation = mutation({
  args: { id: v.string() },
  handler: async (ctx, { id }) => {
    await smallPool.cancel(ctx, id);
  },
});

export const status = query({
  args: { id: v.string() },
  handler: async (ctx, { id }) => {
    return await smallPool.status(ctx, id);
  },
});

export const enqueueABunchOfMutations = action({
  args: {},
  handler: async (ctx, _args) => {
    await Promise.all(
      Array.from({ length: 30 }, () =>
        smallPool.enqueueMutation(ctx, api.example.addMutation, {})
      )
    );
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

export const enqueueLowPriMutations = action({
  args: {},
  handler: async (ctx, _args) => {
    await Promise.all(
      Array.from({ length: 30 }, () =>
        serializedPool.enqueueMutation(ctx, api.example.addLowPri, {})
      )
    );
  },
});

export const highPriMutation = mutation({
  args: { data: v.number() },
  handler: async (ctx, { data }) => {
    await bigPool.enqueueMutation(ctx, api.example.addMutation, { data });
  },
});

export const enqueueABunchOfActions = action({
  args: {},
  handler: async (ctx, _args) => {
    await Promise.all(
      Array.from({ length: 30 }, () =>
        bigPool.enqueueAction(ctx, api.example.addAction, {})
      )
    );
  },
});

export const enqueueAnAction = mutation({
  args: {},
  handler: async (ctx, _args): Promise<void> => {
    await bigPool.enqueueAction(ctx, api.example.addAction, {});
  },
});

export const echo = query({
  args: { num: v.number() },
  handler: async (ctx, { num }) => {
    return num;
  },
});

async function sampleWork() {
  const index = Math.floor(Math.random() * 1000) + 1;
  await new Promise((resolve) => setTimeout(resolve, Math.random() * index));
}

// Example background work: scraping from a website.
export const backgroundWork = internalAction({
  args: {},
  handler: async () => {
    await sampleWork();
  },
});

export const startBackgroundWork = internalAction({
  args: {},
  handler: async (ctx, _args) => {
    await Promise.all(
      Array.from({ length: 20 }, () =>
        serializedPool.enqueueAction(ctx, internal.example.backgroundWork, {})
      )
    );
  },
});

// Example foreground work: calling an API on behalf of a user.
export const foregroundWork = internalAction({
  args: {},
  handler: async () => {
    await sampleWork();
  },
});

export const startForegroundWork = internalAction({
  args: {},
  handler: async (ctx, _args) => {
    await Promise.all(
      Array.from({ length: 100 }, () =>
        bigPool.enqueueAction(ctx, internal.example.foregroundWork, {})
      )
    );
  },
});
