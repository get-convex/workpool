import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

export default defineSchema({
  // XXX rename to config or whatever
  pools: defineTable({
    workers: v.number(),
  }),

  jobs: defineTable({
    fnHandle: v.string(),
    fnName: v.string(),
    fnType: v.union(v.literal("action"), v.literal("mutation")),
    fnArgs: v.any(),
    state: v.union(
      v.literal("pending"),
      v.literal("inProgress"),
      v.literal("success"),
      v.literal("failed"),
      v.literal("canceled")
    ),
    scheduledJob: v.optional(v.id("_scheduled_functions")),
    error: v.optional(v.string()),
  }).index("state", ["state"]),
});
