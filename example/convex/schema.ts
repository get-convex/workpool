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
    pool: v.optional(
      v.union(
        v.literal("0.4.7"),
        v.literal("0.4.6"),
        v.literal("0.4.2"),
        // Legacy values kept so historical runs still validate. New runs
        // should write the version-string values above.
        v.literal("new"),
        v.literal("old"),
      ),
    ),
  }),
  tasks: defineTable({
    runId: v.id("runs"),
    workId: vWorkId,
    type: v.union(v.literal("mutation"), v.literal("action")),
    endTime: v.number(),
    enqueuedAt: v.optional(v.number()),
    wave: v.optional(v.number()),
  }).index("runId", ["runId"]),
});
