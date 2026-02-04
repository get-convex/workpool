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
    endTime: v.optional(v.number()),
  }),
  tasks: defineTable({
    runId: v.id("runs"),
    taskNum: v.number(),
    workId: vWorkId,
    type: v.union(v.literal("mutation"), v.literal("action")),
    startTime: v.optional(v.number()),
    endTime: v.optional(v.number()),
    status: v.union(
      // The "last" task sorted by status (A-Z) will be the run status
      v.literal("completed"),
      v.literal("failed"),
      v.literal("pending"),
      v.literal("running"),
    ),
    hasOnComplete: v.boolean(),
  }).index("runId_status_taskNum", ["runId", "status", "taskNum"]),
});
