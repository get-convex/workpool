import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";
import { config, onComplete, retryBehavior, runResult } from "./shared.js";

// Represents a slice of time to process work.
const segment = v.int64();
/** State machine
```mermaid
flowchart LR
    Client -->|enqueue| pendingStart
    Client -->|cancel| pendingCancellation
    Recovery-->|recover| pendingCancellation
    Recovery-->|recover| pendingCompletion
    Worker-->|"saveResult"| pendingCompletion
    pendingStart -->|mainLoop| workerRunning["internalState.running"]
    workerRunning-->|"mainLoop(pendingCompletion)"| Retry{"Needs retry?"}
    Retry-->|no| onComplete
    Retry-->|yes| pendingStart
    pendingStart-->|"mainLoop(pendingCancellation)"| onComplete
    workerRunning-->|"mainLoop(pendingCancellation)"| onComplete
```
 *
 * Retention optimization strategy:
 * - Patch singletons to avoid tombstones.
 * - Use segements & cursors to bound reads to latest data.
 *   - Do scans outside of the critical path (during load).
 * - Do point reads otherwise.
 */

export default defineSchema({
  // Written from kickLoop, read everywhere.
  globals: defineTable(config),
  // Singleton, only read & written by `mainLoop`.
  internalState: defineTable({
    // Ensure that only one mainLoop is running at a time.
    generation: v.int64(),
    segmentCursors: v.object({
      incoming: segment,
      completion: segment,
      cancelation: segment,
    }),
    lastRecovery: segment,
    report: v.object({
      completed: v.number(),
      failed: v.number(),
      canceled: v.number(),
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
    segment,
  })
    .index("workId", ["workId"])
    .index("segment", ["segment"]),

  // Written by job, read & deleted by `mainLoop`.
  pendingCompletion: defineTable({
    segment,
    runResult,
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
});
