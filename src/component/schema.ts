import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

export default defineSchema({
  // Statically configured.
  pools: defineTable({
    maxParallelism: v.number(),
    actionTimeoutMs: v.number(),
    mutationTimeoutMs: v.number(),
    unknownTimeoutMs: v.number(),
    debounceMs: v.number(),
    fastHeartbeatMs: v.number(),
    slowHeartbeatMs: v.number(),
  }),
  // State across all pools.
  mainLoop: defineTable({
    fn: v.optional(v.id("_scheduled_functions")),
    generation: v.number(),
  }),

  // Client appends to this table to enqueue work.
  // The main loop pops from the front (low _creationTime) to process.
  pendingWork: defineTable({
    fnType: v.union(v.literal("action"), v.literal("mutation"), v.literal("unknown")),
    handle: v.string(),
    fnArgs: v.any(),
    runAtTime: v.number(),
  }).index("runAtTime", ["runAtTime"]),
  // Completely read and written by the main loop.
  inProgressWork: defineTable({
    running: v.id("_scheduled_functions"),
    timeoutMs: v.number(),
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
  pendingCancelation: defineTable({
    workId: v.id("pendingWork"),
  }),
  // Inserted here by the main loop.
  completedWork: defineTable({
    result: v.optional(v.any()),
    error: v.optional(v.string()),
    workId: v.id("pendingWork"),
  }).index("workId", ["workId"]),
});
