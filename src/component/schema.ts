import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";
import {
  fnType,
  vConfig,
  vOnCompleteFnContext,
  retryBehavior,
  vResult,
} from "./shared.js";

// Represents a slice of time to process work.
const segment = v.int64();

export default defineSchema({
  // ----- Batch execution mode tables -----

  // Batch task queue. Each row is one logical task to be executed by an executor.
  batchTasks: defineTable({
    name: v.string(), // handler name in the registry
    args: v.any(), // serialized args for the handler
    slot: v.number(), // executor slot (0..maxWorkers-1) for claim partitioning
    status: v.union(
      v.literal("pending"),
      v.literal("claimed"),
      v.literal("completed"),
      v.literal("failed"),
      v.literal("canceled"),
    ),
    claimedAt: v.optional(v.number()), // when the executor claimed this task
    readyAt: v.number(), // earliest time this task can be claimed (for retry backoff)
    result: v.optional(v.any()),
    error: v.optional(v.string()),
    attempt: v.number(), // completed attempts so far
    onComplete: v.optional(vOnCompleteFnContext),
    retryBehavior: v.optional(retryBehavior),
  })
    .index("by_slot_status_readyAt", ["slot", "status", "readyAt"])
    .index("by_status_claimedAt", ["status", "claimedAt"]),

  // Singleton configuration for the batch executor pool.
  batchConfig: defineTable({
    executorHandle: v.string(), // function handle for the user's executor action
    maxWorkers: v.number(),
    activeSlots: v.array(v.number()), // which executor slots are currently running
    claimTimeoutMs: v.number(),
    watchdogScheduledAt: v.optional(v.number()), // when the last watchdog was scheduled
  }),

  // ----- Standard workpool tables -----

  // Written from kickLoop, read everywhere.
  globals: defineTable(vConfig),
  // Singleton, only read & written by `main`.
  internalState: defineTable({
    // Ensure that only one main is running at a time.
    generation: v.int64(),
    segmentCursors: v.object({
      incoming: segment,
      completion: segment,
      cancelation: segment,
    }),
    lastRecovery: segment,
    report: v.object({
      completed: v.number(), // finished running, counts retries & failures
      succeeded: v.number(), // finished successfully, regardless of retries
      failed: v.number(), // failed after all retries
      retries: v.number(), // failure that turned into a retry
      canceled: v.number(), // cancelations processed
      lastReportTs: v.number(),
    }),
    running: v.array(
      v.object({
        workId: v.id("work"),
        scheduledId: v.id("_scheduled_functions"),
        started: v.number(),
      }),
    ),
  }),

  // Singleton, written by `updateRunStatus` when running, by client or worker otherwise.
  // Safe to read from kickLoop, since it should update infrequently.
  runStatus: defineTable({
    state: v.union(
      v.object({ kind: v.literal("running") }),
      v.object({
        kind: v.literal("scheduled"),
        segment,
        scheduledId: v.id("_scheduled_functions"),
        saturated: v.boolean(),
        generation: v.int64(),
      }),
      v.object({ kind: v.literal("idle"), generation: v.int64() }),
    ),
  }),

  // Written on enqueue. Deleted by `complete` for success, failure, canceled.
  work: defineTable({
    fnType,
    fnHandle: v.string(),
    fnName: v.string(),
    fnArgs: v.any(),
    attempts: v.number(), // number of completed attempts
    onComplete: v.optional(vOnCompleteFnContext),
    retryBehavior: v.optional(retryBehavior),
    canceled: v.optional(v.boolean()),
  }),

  // Written on enqueue & rescheduled for retry, read & deleted by `main`.
  pendingStart: defineTable({
    workId: v.id("work"),
    segment,
  })
    .index("workId", ["workId"])
    .index("segment", ["segment"]),

  // Written by complete, read & deleted by `main`.
  pendingCompletion: defineTable({
    segment,
    runResult: vResult,
    workId: v.id("work"),
    retry: v.boolean(),
  })
    .index("workId", ["workId"])
    .index("segment", ["segment"]),

  // Written on cancelation, read & deleted by `main`.
  pendingCancelation: defineTable({
    segment,
    workId: v.id("work"),
  })
    .index("workId", ["workId"])
    .index("segment", ["segment"]),
});
