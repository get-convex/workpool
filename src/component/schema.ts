import { defineSchema, defineTable } from "convex/server";
import { v, type Infer } from "convex/values";
import {
  fnType,
  vConfig,
  vOnCompleteFnContext,
  retryBehavior,
  vResult,
} from "./shared.js";

// Represents a slice of time to process work.
const segment = v.int64();

// Internal-only RunResult kinds, used by recovery → complete → main loop.
// External callers (worker, onComplete) only ever see vResult.
export const vResultInternal = v.union(
  vResult,
  // Mutation was canceled by recovery because it sat in the scheduler
  // beyond the stuck threshold; main loop must re-enqueue it.
  v.object({ kind: v.literal("stuckInScheduler") }),
);
export type RunResultInternal = Infer<typeof vResultInternal>;

export default defineSchema({
  // Written from kickLoop, read everywhere.
  globals: defineTable(vConfig),
  // Singleton, only read & written by `run`.
  internalState: defineTable({
    // @deprecated batch-worker now owns the generation guard. We keep writing
    // `0n` for rollback compatibility with older workpool versions.
    generation: v.optional(v.int64()),
    // Track where we've scanned to, so we skip tombstones on re-scan.
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
      conflicted: v.optional(v.number()),
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

  // Written on enqueue. Deleted by `complete` for success, failure, canceled.
  work: defineTable({
    fnType,
    fnHandle: v.string(),
    fnName: v.string(),
    fnArgs: v.optional(v.any()),
    // Reference to large args/onComplete context if stored separately
    payloadId: v.optional(v.id("payload")),
    payloadSize: v.optional(v.number()),
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
    runResult: vResultInternal,
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

  // Store large data separately to avoid document size limits
  payload: defineTable({
    args: v.optional(v.record(v.string(), v.any())),
    context: v.optional(v.any()),
  }),
});
