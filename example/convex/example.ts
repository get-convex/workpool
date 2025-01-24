import {
  mutation,
  action,
  query,
  internalMutation,
  internalAction,
} from "./_generated/server";
import { api, components, internal } from "./_generated/api";
import { Workpool } from "@convex-dev/workpool";
import { v } from "convex/values";

const highPriPool = new Workpool(components.highPriWorkpool, {
  maxParallelism: 20,
  // For tests, disable completed work cleanup.
  statusTtl: Number.POSITIVE_INFINITY,
  logLevel: "INFO",
});
const pool = new Workpool(components.workpool, {
  maxParallelism: 3,
  // For tests, disable completed work cleanup.
  statusTtl: Number.POSITIVE_INFINITY,
  logLevel: "INFO",
});
const lowpriPool = new Workpool(components.lowpriWorkpool, {
  maxParallelism: 1,
  // For tests, disable completed work cleanup.
  statusTtl: Number.POSITIVE_INFINITY,
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
    return await pool.enqueueMutation(ctx, api.example.addMutation, { data });
  },
});

export const cancelMutation = mutation({
  args: { id: v.string() },
  handler: async (ctx, { id }) => {
    await pool.cancel(ctx, id);
  },
});

export const status = query({
  args: { id: v.string() },
  handler: async (ctx, { id }) => {
    return await pool.status(ctx, id);
  },
});

export const enqueueABunchOfMutations = action({
  args: {},
  handler: async (ctx, _args) => {
    await Promise.all(
      Array.from({ length: 30 }, () =>
        pool.enqueueMutation(ctx, api.example.addMutation, {})
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
        lowpriPool.enqueueMutation(ctx, api.example.addLowPri, {})
      )
    );
  },
});

export const highPriMutation = mutation({
  args: { data: v.number() },
  handler: async (ctx, { data }) => {
    await highPriPool.enqueueMutation(ctx, api.example.addMutation, { data });
  },
});

export const enqueueABunchOfActions = action({
  args: {},
  handler: async (ctx, _args) => {
    await Promise.all(
      Array.from({ length: 30 }, () =>
        pool.enqueueAction(ctx, api.example.addAction, {})
      )
    );
  },
});

export const enqueueAnAction = action({
  args: {},
  handler: async (ctx, _args): Promise<void> => {
    await pool.enqueueAction(ctx, api.example.addAction, {});
  },
});

export const echo = query({
  args: { num: v.number() },
  handler: async (ctx, { num }) => {
    return num;
  },
});

async function sampleWork() {
  const index = Math.floor(Math.random() * 3000) + 1;
  const url = `${process.env.CONVEX_CLOUD_URL}/api/query`;
  const start = Date.now();
  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      path: "example:echo",
      args: { num: index },
    }),
  });
  const data = await response.json();
  console.log(data.value === index, Date.now() - start);
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
        lowpriPool.enqueueAction(ctx, internal.example.backgroundWork, {})
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

export const startForegroundWork = internalMutation({
  args: {},
  handler: async (ctx, _args) => {
    await pool.enqueueAction(ctx, internal.example.foregroundWork, {});
  },
});
