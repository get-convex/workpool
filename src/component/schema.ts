import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";
import {
  completionStatus,
  config,
  onComplete,
  retryBehavior,
} from "./shared.js";

// Represents a slice of time to process work.
const segment = v.int64();
/**
Data flow:

- The mutation `mainLoop` runs periodically and serially.
- Several tables act as queues (pending*), with client-driven mutations enqueueing in
  future segments, and `mainLoop` reading from past segments.
- State machine:
  {start} --client--> pendingStart (and writes to work)
  pendingStart --mainLoop--> inProgressWork // to start work
   --worker--> pendingCompletion // when work succeeds or fails
  inProgressWork, pendingCompletion --mainLoop--> {end} // when work reports success or failure.
  inProgressWork, pendingCompletion --mainLoop--> pendingStart // when retry is needed.
  inProgressWork --mainLoop--> {end} // fails due to timeout / internal failure
  inProgressWork --mainLoop--> pendingStart // retries due to timeout / internal failure
   --client--> pendingCancelation // cancel requested
  pendingCancelation, pendingStart --mainLoop--> {end} // when canceled before work starts.
  pendingCancelation, inProgressWork --mainLoop--> {end} // attempts to cancel
  pendingCancelation, pendingCompletion --mainLoop--> {end} // no-op, work is alreadydone.
  {end}: calls onComplete, then deletes work in the same transaction.
 */

export default defineSchema({
  // Singleton, only read & written by `mainLoop`.
  internalState: defineTable({
    config,
    segmentCursors: v.object({
      incoming: segment,
      completion: segment,
      cancelation: segment,
    }),
    numRunning: v.number(),
    lastRecoveryTs: v.number(),
    report: v.object({
      completed: v.int64(),
      failed: v.int64(),
      canceled: v.int64(),
      timedOut: v.int64(),
      lastReportTs: v.number(),
    }),
  }),

  // Singleton, written by `mainLoop` when running, by client or worker otherwise.
  // Safe to read from kickLoop, since it should update infrequently.
  runStatus: defineTable({
    state: v.union(
      v.object({ kind: v.literal("running") }),
      // Only when scheduled >1 segment in the future.
      v.object({
        kind: v.literal("scheduled"),
        segment,
        fn: v.id("_scheduled_functions"),
      }),
      v.object({ kind: v.literal("idle") })
    ),
  }),

  // Written on enqueue. Safe to read. Deleted after onComplete is called.
  work: defineTable({
    fnType: v.union(v.literal("action"), v.literal("mutation")),
    fnHandle: v.string(),
    fnName: v.string(),
    fnArgs: v.any(),
    attempts: v.number(),
    onComplete: v.optional(onComplete),
    retryBehavior: v.optional(retryBehavior),
  }),

  // Written on enqueue, read & deleted by `mainLoop`.
  pendingStart: defineTable({
    workId: v.id("work"),
    config,
    segment,
  })
    .index("workId", ["workId"])
    .index("segment", ["segment"]),

  // Written by job, read & deleted by `mainLoop`.
  pendingCompletion: defineTable({
    segment,
    completionStatus,
    workId: v.id("work"),
  })
    .index("workId", ["workId"])
    .index("segment", ["segment"]),

  // Written on cancellation, read & deleted by `mainLoop`.
  pendingCancelation: defineTable({
    segment,
    workId: v.id("work"),
  })
    .index("workId", ["workId"])
    .index("segment", ["segment"]),

  // Read, written, and deleted by `mainLoop`.
  inProgressWork: defineTable({
    running: v.id("_scheduled_functions"),
    started: v.number(),
    workId: v.id("work"),
  }).index("workId", ["workId"]),
});
