import { mutation, query } from "./_generated/server";
import { api, components } from "./_generated/api";
import { WorkPool } from "@convex-dev/workpool";
import { v } from "convex/values";

const pool = new WorkPool(components.workpool, 3);

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
    await pool.enqueue(ctx, api.example.addNow, { data });
  },
});

export const list = query({
  args: {},
  handler: async (ctx, {}) => {
    return ctx.db.query("data").collect();
  },
});

export const state = query({
  args: { id: v.string() },
  handler: async (ctx, { id }) => {
    return await pool.state(ctx, id);
  },
});
