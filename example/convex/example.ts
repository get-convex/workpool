import { internalMutation, query, mutation, action } from "./_generated/server";
import { api, components } from "./_generated/api";
import { WorkPool } from "@convex-dev/workpool";

const pool = new WorkPool(components.workpool, { maxParallelism: 3 });

export const addMutation = mutation({
  args: {},
  handler: async (ctx, _args) => {
    const data = Math.random();
    await ctx.db.insert("data", {data});
    return data;
  },
});

export const addAction = action({
  args: {},
  handler: async (ctx, _args): Promise<number> => {
    return await ctx.runMutation(api.example.addMutation, {});
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
    const result = await pool.pollResult(ctx, work, 30*1000);
    return result;
  },
});
