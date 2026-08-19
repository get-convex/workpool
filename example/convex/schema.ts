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
    scheduledFunctions: v.optional(v.number()),
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
  // When each probe in test/scheduling.ts ran, to check delayed and retried
  // work against a real deployment.
  schedulingProbes: defineTable({
    label: v.string(),
    at: v.number(),
    attempt: v.optional(v.number()),
  }),
  // One row per task in test/latency.ts, carrying when it was meant to run and
  // when it actually did, so ordering and lateness can be measured directly
  // rather than inferred from completion times.
  latencyTasks: defineTable({
    cell: v.string(), // which experiment cell enqueued it
    pool: v.union(v.literal("new"), v.literal("old")),
    delayMs: v.number(), // 0 for immediate
    seq: v.number(), // enqueue order within the cell
    enqueuedAt: v.number(), // clock at the enqueuing mutation
    runAt: v.number(), // when it was asked to run
    startedAt: v.optional(v.number()), // clock inside the task itself
  }).index("cell", ["cell"]),
});
