import { mutation, action, query, ActionCtx } from "./_generated/server";
import { api, components } from "./_generated/api";
import { WorkId, WorkPool } from "@convex-dev/workpool";
import { v } from "convex/values";

const pool = new WorkPool(components.workpool, {
  maxParallelism: 3,
  // For tests, disable completed work cleanup.
  ttl: Number.POSITIVE_INFINITY,
  logLevel: "DEBUG",
});
const lowpriPool = new WorkPool(components.lowpriWorkpool, {
  maxParallelism: 1,
  // For tests, disable completed work cleanup.
  ttl: Number.POSITIVE_INFINITY,
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

export const enqueueOneMutation = mutation({
  args: { data: v.number() },
  handler: async (ctx, { data }): Promise<string> => {
    return await pool.enqueueMutation(ctx, api.example.addMutation, { data });
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
      await pool.enqueueMutation(ctx, api.example.addMutation, {});
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

export const enqueueABunchOfActions = mutation({
  args: {},
  handler: async (ctx, _args) => {
    for (let i = 0; i < 30; i++) {
      await pool.enqueueAction(ctx, api.example.addAction, {});
    }
  },
});

export const enqueueAndWait = action({
  args: {},
  handler: async (ctx, _args): Promise<number> => {
    const work = await pool.enqueueAction(ctx, api.example.addAction, {});
    const result = await pool.pollResult(ctx, work, 30 * 1000);
    return result;
  },
});

export const doSomethingInPool = action({
  args: {},
  handler: async (ctx, _args): Promise<number> => {
    // poolCtx is a drop-in replacement for ctx that does all work in the pool.
    const poolCtx = pool.ctx(ctx);
    return await doSomething(poolCtx);
  },
});

async function doSomething(ctx: ActionCtx): Promise<number> {
  const data1 = await ctx.runMutation(api.example.addMutation, { data: 1 });
  const data2 = await ctx.runAction(api.example.addAction, { data: 2 });
  return data1 + data2;
}
