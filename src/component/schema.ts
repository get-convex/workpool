import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";
import {
  fnType,
  vConfig,
  vOnCompleteFnContext,
  retryBehavior,
  vResult,
} from "./shared.js";

// When a queue entry becomes eligible to process, in nanoseconds since the
// epoch — the scale a commit timestamp uses.
//
// Entries that are ready as soon as they land store `db.vars.commitTs`, which
// resolves at commit time. That's what makes an index on this field scannable
// with a cursor that never has to be rewound: an entry can't appear behind a
// commit timestamp we've already read past. (`_creationTime` can't do this — it
// is assigned when the mutation *starts*, so a slow mutation's row can land
// behind rows that already committed and were read.)
//
// Entries that shouldn't be processed until later store that time directly.
// `v.commitTs()` accepts any int64, so both clocks live in one field and one
// index, and scheduled work sorts above the loop's read bound until it's due.
const segment = v.commitTs();
// A cursor into an index on a commit-timestamp field. Reads of such a field
// come back as a plain int64.
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
      // Cursor into pendingStart's `scanTs` index — the out-of-order sweep.
      // Inclusive: a transaction can write several documents with one stamp,
      // so the boundary stamp's few documents are re-read until they retire.
      // Optional because it didn't always exist; absent means "from the
      // beginning".
      sweep: v.optional(timestamp),
    }),
    // The commit timestamp of the previous `run`. It says nothing about what
    // that run had read (commits can land between its snapshot and its
    // commit) — but the *next* run reads this document, so its snapshot
    // includes every transaction stamped at or below this value, making it a
    // safe addition to that run's cursor ceiling (see `run`). It only bounds
    // cursor advancement. Without it the ceiling would come only from commit
    // stamps observed in the batch itself, which still converges — every
    // start eventually produces a commit-stamped completion — but a pool
    // running only scheduled (wall-clock-keyed) work would leave its cursor
    // behind each entry it starts, re-reading those tombstones until the
    // completion arrives. This closes that gap by the very next iteration.
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

  // Written on enqueue & rescheduled for retry, read, shrunk & deleted by
  // `main`. One document carries every entry of a single transaction that
  // shares a `segment` (up to a cap), so a batch enqueue writes a few
  // documents rather than hundreds, and starting or canceling one entry
  // patches it out of `workIds` (found via the work doc's `pendingStartId`).
  pendingStart: defineTable({
    // The work waiting to start at `segment`. Entries leave as they start or
    // cancel; the document is deleted when none remain.
    workIds: v.optional(v.array(v.id("work"))),
    // @deprecated The single entry of a document written by version ≤ 0.4.9.
    workId: v.optional(v.id("work")),
    segment,
    // Present iff `segment` is a wall-clock start time rather than a commit
    // timestamp — so the cursor ceiling knows not to count it as an observed
    // commit stamp. See `run`.
    scheduled: v.optional(v.boolean()),
    // The enqueue's commit timestamp, present only on documents that could
    // commit out of order — scheduled enqueues, where commit latency can
    // exceed the delay and land the document behind the loop's `segment`
    // cursor. The sweep walks this index in commit order — which nothing can
    // land behind — and starts anything the cursor passed over. Loop-written
    // documents (retries, re-keyed legacy entries) can't be out of order and
    // omit it. See `queryPending`.
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
