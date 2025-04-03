import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";
import {
  fnType,
  config,
  onComplete,
  retryBehavior,
  runResult,
} from "./shared.js";

// Represents a slice of time to process work.
const segment = v.int64();

export default defineSchema({
  // Written from kickLoop, read everywhere.
  globals: defineTable(config),
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
      })
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
      v.object({ kind: v.literal("idle"), generation: v.int64() })
    ),
  }),

  // Written on enqueue. Deleted by `complete` for success, failure, canceled.
  work: defineTable({
    fnType,
    fnHandle: v.string(),
    fnName: v.string(),
    fnArgs: v.any(),
    attempts: v.number(), // number of completed attempts
    onComplete: v.optional(onComplete),
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
    runResult,
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
