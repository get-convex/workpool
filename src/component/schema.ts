import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";
import {
  fnType,
  vConfig,
  vOnCompleteFnContext,
  retryBehavior,
  vResult,
} from "./shared.js";

// When a queue entry becomes eligible, in nanoseconds since the epoch: the
// commit timestamp of the enqueue for ready work, or the start time for
// scheduled work. See cursor-strategy.md.
const segment = v.commitTs();
// A time on the same nanosecond scale (commit-timestamp fields read back as
// plain int64s).
const timestamp = v.int64();

export default defineSchema({
  // Written from kickLoop, read everywhere.
  globals: defineTable(vConfig),
  // Singleton, only read & written by `run`.
  internalState: defineTable({
    // @deprecated batch-worker now owns the generation guard. We keep writing
    // `0n` for rollback compatibility with older workpool versions.
    generation: v.optional(v.int64()),
    // Track where we've scanned to, so we skip tombstones on re-scan. Same
    // field as older versions used for its wall-clock cursors, so their data
    // still validates — the values are now commit timestamps, and an older
    // version's much smaller ones just mean "scan from the beginning".
    segmentCursors: v.object({
      incoming: timestamp,
      completion: timestamp,
      cancelation: timestamp,
      // Cursor into pendingStart's `scanTs` index; absent means "from the
      // beginning".
      sweep: v.optional(timestamp),
    }),
    // The commit timestamp of the previous `run`. Every transaction stamped
    // at or below it is visible to any reader of this document, so it is a
    // safe lower bound on the next run's snapshot.
    lastCommitTs: v.optional(v.commitTs()),
    // Unlike the cursors, this stays a 100ms "segment": it only paces how often
    // the loop checks for stuck jobs.
    lastRecovery: v.int64(),
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
    // The queue entry waiting to start this work, if any — replacing a
    // `workId` index on pendingStart. May be stale (the entry started and was
    // deleted, or predates this pointer), so readers check the entry still
    // exists. A pendingStart unreachable through this pointer is dropped
    // reactively: the loop reads it and finds its work gone or canceled.
    pendingStartId: v.optional(v.id("pendingStart")),
  }),

  // Work waiting to start: one document per transaction and `segment` value,
  // holding up to a cap of entries.
  pendingStart: defineTable({
    // The work waiting to start at `segment`. Entries leave as they start or
    // cancel; the document is deleted when none remain.
    workIds: v.optional(v.array(v.id("work"))),
    // @deprecated The single entry of a document written by version ≤ 0.4.9.
    workId: v.optional(v.id("work")),
    segment,
    // Present iff `segment` is a wall-clock start time rather than a commit
    // timestamp.
    scheduled: v.optional(v.boolean()),
    // The enqueue's commit timestamp, present iff the document could have
    // committed out of order (a scheduled start within five minutes).
    scanTs: v.optional(v.commitTs()),
  })
    .index("segment", ["segment"])
    .index("scanTs", ["scanTs", "segment"]),

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

  // Store large data separately to avoid document size limits
  payload: defineTable({
    args: v.optional(v.record(v.string(), v.any())),
    context: v.optional(v.any()),
  }),
});
