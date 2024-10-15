import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

export default defineSchema({
  // Statically configured.
  pools: defineTable({
    name: v.string(),
    maxParallelism: v.optional(v.number()),
    priority: v.union(v.literal("low"), v.literal("normal"), v.literal("high")),
    parent: v.optional(v.id("pools")),
  }).index("name", ["name"]),
  // State across all pools.
  mainLoop: defineTable({
    fn: v.id("_scheduled_functions"),
  }),
  // State for the whole pool.
  // We could use a sharded counter, but that doesn't help much because we have
  // to accumulate the total count anyway, which serializes all the writes.
  poolState: defineTable({
    pool: v.id("pools"),
    countInProgress: v.number(),
  }).index("pool", ["pool"]),
  pendingWork: defineTable({
    pool: v.id("pools"),
    // Between 0 and 100.
    priority: v.number(),
    handle: v.string(),
    fnArgs: v.any(),
  }).index("priority", ["priority"]),
  inProgressWork: defineTable({
    pool: v.id("pools"),
    running: v.id("_scheduled_functions"),
    handle: v.string(),
  }).index("pool", ["pool"]),
  completedWork: defineTable({
    pool: v.id("pools"),
    result: v.any(),
  }).index("pool", ["pool"]),
});
