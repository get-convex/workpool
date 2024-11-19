import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

/**
Data flow:

- The mutation `mainLoop` runs periodically and serially.
- Several tables act as queues, with client-driven mutations enqueueing at high
  timestamps and `mainLoop` popping at low timestamps:
  pendingWork, pendingCompletion, and pendingCancelation.
  - The `enqueue` mutation writes to pendingWork.
  - The `cancel` mutation writes to pendingCancelation.
  - The `saveResult` mutation, run as part of scheduled work, writes to pendingCompletion.
- mainLoop processes the queues:
  - pendingWork => inProgressWork.
  - pendingCompletion and pendingCancelation => completedWork.
  - inProgressWork that finishes uncleanly (timeout or system failure) => completedWork.
- `mainLoop` schedules itself to run.
- `enqueue`, `cancel`, and `saveResult` mutations check when `mainLoop` is scheduled to run,
  and if it's too far in the future, they schedule it to run sooner.
- `status` query reads from pendingWork and completedWork.
- `cleanup` mutation deletes old rows from completedWork.

To avoid OCCs, we restrict which mutations can read and write from each table:
- pools: read by all, written only when static WorkPool options change.
- mainLoop (table): read by all, written mostly by `mainLoop`.
  If `mainLoop` will not run for a while, mainLoop table is written by `enqueue`, `cancel`, or `saveResult`.
- pendingWork: `enqueue` inserts at high timestamps, `mainLoop` pops at low timestamps. `status` query does point-reads.
- pendingCompletion: `saveResult` inserts at high timestamps, `mainLoop` pops at low timestamps.
- pendingCancelation: `cancel` inserts at high timestamps, `mainLoop` pops at low timestamps.
- inProgressWork: `mainLoop` inserts, reads all, and deletes.
- completedWork: `mainLoop` inserts at hight timestamps, `status` query reads, `cleanup` deletes at low timestamps.

 */

export default defineSchema({
  // Statically configured.
  pools: defineTable({
    workers: v.number(),
  }),

  // State across all pools.
  // TODO(emma) change this to use a boolean or enum of statuses, instead of using runAtTime.
  // Status like "running", "waitingForJobCompletion", "idle".
  // Currently there's a problem if enqueue is called from a mutation that takes longer than
  // debounceMs to complete, and a mainLoop finishes and restarts in that time window. Then the enqueue will OCC with the mainLoop.
  // But if we have fixed statuses, we don't need to write it so frequently so it won't OCC. Chat with @ian about details.
  mainLoop: defineTable({
    fn: v.optional(v.id("_scheduled_functions")),
    generation: v.number(),
    runAtTime: v.number(),
  }).index("runAtTime", ["runAtTime"]),

  pendingWork: defineTable({
    fnType: v.union(v.literal("action"), v.literal("mutation")),
    fnHandle: v.string(),
    fnName: v.string(),
    fnArgs: v.any(),
    runAtTime: v.number(),
  }).index("runAtTime", ["runAtTime"]),
  pendingCompletion: defineTable({
    error: v.optional(v.string()),
    workId: v.id("pendingWork"),
  }).index("workId", ["workId"]),
  pendingCancelation: defineTable({
    workId: v.id("pendingWork"),
  }),

  inProgressWork: defineTable({
    running: v.id("_scheduled_functions"),
    workId: v.id("pendingWork"),
  }).index("workId", ["workId"]),

  // XXX feels like these could all go in one table with an index and state machine
  completedWork: defineTable({
    error: v.optional(v.string()),
    workId: v.id("pendingWork"),
  }).index("workId", ["workId"]),
});
