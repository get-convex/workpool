import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";
import { vWorkId } from "@convex-dev/workpool";

export default defineSchema({
  data: defineTable({
    data: v.optional(v.number()),
    misc: v.optional(v.any()),
  }),
  runs: defineTable({
    startTime: v.number(),
    scenario: v.string(),
    parameters: v.any(),
    taskCount: v.optional(v.number()),
    endTime: v.optional(v.number()),
    pool: v.optional(v.string()),
  }),
  tasks: defineTable({
    runId: v.id("runs"),
    workId: vWorkId,
    type: v.union(
      v.literal("mutation"),
      v.literal("action"),
      v.literal("query"),
    ),
    endTime: v.number(),
    enqueuedAt: v.optional(v.number()),
    wave: v.optional(v.number()),
    // Class label for noisy-neighbor scenarios (e.g. "fast", "slow", "fail").
    label: v.optional(v.string()),
    // Terminal result kind reported to onComplete: success | failed | canceled.
    resultKind: v.optional(v.string()),
  }).index("runId", ["runId"]),
  // Single shared doc used to force OCC contention between onComplete
  // handlers in the noisyNeighbor scenario.
  counters: defineTable({
    name: v.string(),
    value: v.number(),
  }).index("name", ["name"]),
});
