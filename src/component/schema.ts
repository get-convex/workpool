import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

export default defineSchema({
  // Statically configured.
  pools: defineTable({
    maxParallelism: v.number(),
  }),
  // State across all pools.
  mainLoop: defineTable({
    fn: v.id("_scheduled_functions"),
  }),
  // State for the whole pool.
  // We could use a sharded counter, but that doesn't help much because we have
  // to accumulate the total count anyway, which serializes all the writes.
  poolState: defineTable({
    countInProgress: v.number(),
  }),

  // Client appends to this table to enqueue work.
  // The main loop pops from the front (low _creationTime) to process.
  pendingWork: defineTable({
    fnType: v.union(v.literal("action"), v.literal("mutation")),
    handle: v.string(),
    fnArgs: v.any(),
  }),
  // Completely read and written by the main loop.
  inProgressWork: defineTable({
    running: v.id("_scheduled_functions"),
    workId: v.id("pendingWork"),
  }).index("workId", ["workId"]),
  // Appended by processing threads.
  // The main loop pops from the front (low _creationTime) to remove from
  // inProgress and mark as completed.
  pendingCompletion: defineTable({
    result: v.optional(v.any()),
    error: v.optional(v.string()),
    workId: v.id("pendingWork"),
  }).index("workId", ["workId"]),
  // Inserted here by the main loop.
  completedWork: defineTable({
    result: v.optional(v.any()),
    error: v.optional(v.string()),
    workId: v.id("pendingWork"),
  }).index("workId", ["workId"]),
});
