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
  }),
  tasks: defineTable({
    runId: v.id("runs"),
    workId: vWorkId,
    type: v.union(v.literal("mutation"), v.literal("action")),
    endTime: v.number(),
  }).index("runId", ["runId"]),
});
