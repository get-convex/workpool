import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";
import { queueTable } from "../queue/index.js";
import {
  fnType,
  vConfig,
  vOnCompleteFnContext,
  retryBehavior,
  vResult,
} from "./shared.js";

// A cursor into a queue lane's `segment` index (see `queueTable` for why the
// lanes are ordered by commit timestamp). Reads of the field come back as a
// plain int64.
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
    }),
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
  }),

  // Written on enqueue & rescheduled for retry, read & deleted by `main`.
  // `runAt` is only set when the work shouldn't start yet: `segment` already
  // holds that time whenever we could safely write it there; when we couldn't —
  // a caller-supplied `runAt` too near to be sure it lands ahead of the loop's
  // cursor — `segment` is the commit timestamp and `runAt` is what tells the
  // loop to move the entry forward instead of starting it.
  pendingStart: queueTable({ workId: v.id("work") }).index("workId", [
    "workId",
  ]),

  // Written by complete, read & deleted by `main`.
  pendingCompletion: queueTable({
    runResult: vResult,
    workId: v.id("work"),
    retry: v.boolean(),
  }).index("workId", ["workId"]),

  // Written on cancelation, read & deleted by `main`.
  pendingCancelation: queueTable({ workId: v.id("work") }).index("workId", [
    "workId",
  ]),

  // Store large data separately to avoid document size limits
  payload: defineTable({
    args: v.optional(v.record(v.string(), v.any())),
    context: v.optional(v.any()),
  }),
});
