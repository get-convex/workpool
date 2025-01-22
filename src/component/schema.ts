import { defineSchema, defineTable } from "convex/server";
import { Infer, v } from "convex/values";
import { logLevel } from "./logging";

export const completionStatus = v.union(
  v.literal("success"),
  v.literal("error"),
  v.literal("canceled"),
  v.literal("timeout")
);
export type CompletionStatus = Infer<typeof completionStatus>;

/**
Data flow:

- The mutation `mainLoop` runs periodically and serially.
- Several tables act as queues, with client-driven mutations enqueueing at high
  timestamps and `mainLoop` popping at low timestamps:
  pendingStart, pendingCompletion, and pendingCancelation.
  - The `enqueue` mutation writes to pendingStart.
  - The `cancel` mutation writes to pendingCancelation.
  - The `saveResult` mutation, run as part of scheduled work, writes to pendingCompletion.
- mainLoop processes the queues:
  - pendingStart => inProgressWork.
  - pendingCompletion and pendingCancelation => completedWork.
  - inProgressWork that finishes uncleanly (timeout or system failure) => completedWork.
- `mainLoop` schedules itself to run.
- `enqueue`, `cancel`, and `saveResult` mutations check when `mainLoop` is scheduled to run,
  and if it's too far in the future, they schedule it to run sooner.
- `status` query reads from pendingWork and completedWork.
- `cleanup` mutation deletes old rows from completedWork.

To avoid OCCs, we restrict which mutations can read and write from each table:
- pools: read by all, written only when static Workpool options change.
- mainLoop (table): read by all, written mostly by `mainLoop`.
  If `mainLoop` will not run for a while, mainLoop table is written by `enqueue`, `cancel`, or `saveResult`.
- pendingWork: `enqueue` inserts at high timestamps, `mainLoop` pops at low timestamps. `status` query does point-reads.
- pendingCompletion: `saveResult` inserts at high timestamps, `mainLoop` pops at low timestamps.
- pendingCancelation: `cancel` inserts at high timestamps, `mainLoop` pops at low timestamps.
- inProgressWork: `mainLoop` inserts, reads all, and deletes.
- completedWork: `mainLoop` inserts at hight timestamps, `status` query reads, `cleanup` deletes at low timestamps.

 */

export default defineSchema({
  // Statically configured, singleton.
  pools: defineTable({
    maxParallelism: v.number(),
    ttl: v.number(),
    logLevel,
  }),

  mainLoop: defineTable({
    state: v.union(
      v.object({ kind: v.literal("running") }),
      v.object({
        kind: v.literal("scheduled"),
        runAtTime: v.number(),
        fn: v.id("_scheduled_functions"),
      }),
      v.object({ kind: v.literal("idle") })
    ),
  }),

  work: defineTable({
    fnType: v.union(v.literal("action"), v.literal("mutation")),
    fnHandle: v.string(),
    fnName: v.string(),
    fnArgs: v.any(),
  }),

  pendingStart: defineTable({
    workId: v.id("work"),
  }).index("workId", ["workId"]),
  pendingCompletion: defineTable({
    completionStatus,
    workId: v.id("work"),
  }).index("workId", ["workId"]),
  pendingCancelation: defineTable({
    workId: v.id("work"),
  }),

  inProgressWork: defineTable({
    running: v.id("_scheduled_functions"),
    timeoutMs: v.union(v.number(), v.null()),
    workId: v.id("work"),
  }).index("workId", ["workId"]),

  completedWork: defineTable({
    completionStatus,
    workId: v.id("work"),
  }).index("workId", ["workId"]),
});
