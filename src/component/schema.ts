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
  // Statically configured, singleton.
  pools: defineTable({
    maxParallelism: v.number(),
    actionTimeoutMs: v.number(),
    fastHeartbeatMs: v.number(),
    slowHeartbeatMs: v.number(),
    ttl: v.number(),
    logLevel,
  }),

  // TODO(emma) change this to use a boolean or enum of statuses, instead of using runAtTime.
  // Status like "running", "waitingForJobCompletion", "idle".
  // Currently there's a problem if enqueue is called from a mutation that takes longer than
  // debounceMs to complete, and a mainLoop finishes and restarts in that time window. Then the enqueue will OCC with the mainLoop.
  // But if we have fixed statuses, we don't need to write it so frequently so it won't OCC. Chat with @ian about details.
  mainLoop: defineTable({
    // null means it's actively running.
    runAtTime: v.union(v.number(), v.null()),
    // Only set if it's not actively running -- so it's scheduled to run in the future, at runAtTime.
    fn: v.union(v.id("_scheduled_functions"), v.null()),
  }).index("runAtTime", ["runAtTime"]),

  pendingWork: defineTable({
    fnType: v.union(v.literal("action"), v.literal("mutation")),
    fnHandle: v.string(),
    fnName: v.string(),
    fnArgs: v.any(),
  }),
  pendingCompletion: defineTable({
    completionStatus,
    workId: v.id("pendingWork"),
  }).index("workId", ["workId"]),
  pendingCancelation: defineTable({
    workId: v.id("pendingWork"),
  }),

  inProgressWork: defineTable({
    running: v.id("_scheduled_functions"),
    timeoutMs: v.number(),
    workId: v.id("pendingWork"),
  }).index("workId", ["workId"]),

  completedWork: defineTable({
    completionStatus,
    workId: v.id("pendingWork"),
  }).index("workId", ["workId"]),
});
