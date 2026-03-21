import { internalMutation, internalAction } from "../_generated/server";
import { v } from "convex/values";
import { DefaultFunctionArgs } from "convex/server";
import { components, internal } from "../_generated/api";
import {
  vOnCompleteArgs,
  WorkId,
  enqueueBatch,
  enqueue,
} from "@convex-dev/workpool";
import { ActionCtx } from "../_generated/server";

/**
 * Generates a string of the specified length.
 * Shows how big the data is by printing the size progressively.
 * Uses 10-character chunks for efficient string building.
 */
export function generateData(len: number): string {
  const chunk = ".........."; // 10 bytes
  const numChunks = Math.floor(len / 10);
  const chunks = Array(numChunks).fill(chunk);
  if (len % 10) {
    chunks.push(chunk.substring(0, len % 10));
  }
  for (let i = 100; i <= numChunks; i += 100) {
    chunks[i - 1] = (i / 100).toString().padStart(8, ".") + "KB";
  }
  let result = chunks.join("");
  // Add remaining bytes for exact length
  const remaining = len % 10;
  if (remaining > 0) {
    result += chunk.substring(0, remaining);
  }
  return result;
}

// Configurable mutation that simulates database operations
export const configurableMutation = internalMutation({
  args: {
    readWriteData: v.optional(v.number()), // If specified, write then delete this many bytes
    returnBytes: v.number(), // Size of return value
    payload: v.any(), // Separate payload argument
    runId: v.id("runs"), // Run ID for tracking
    conflictDocId: v.optional(v.id("data")), // Shared doc to read+write, causing OCC
    dbOps: v.optional(v.number()), // Number of insert+delete cycles to widen transaction window
  },
  handler: async (ctx, args) => {
    // Widen the transaction window with extra DB operations
    for (let i = 0; i < (args.dbOps ?? 0); i++) {
      const id = await ctx.db.insert("data", { data: i });
      await ctx.db.delete(id);
    }

    // If conflictDocId is specified, read+write shared doc to cause OCC conflicts.
    // Placed AFTER dbOps so the transaction is already wide when we touch the
    // contended document — maximizing the chance of OCC.
    if (args.conflictDocId) {
      const doc = await ctx.db.get(args.conflictDocId);
      if (doc) {
        await ctx.db.patch(args.conflictDocId, { data: (doc.data ?? 0) + 1 });
      }
    }

    // If readWriteData is specified, write then delete
    if (args.readWriteData && args.readWriteData > 0) {
      const dataToWrite = generateData(args.readWriteData);
      const docId = await ctx.db.insert("data", { misc: dataToWrite });
      // Delete immediately - this will read then write
      await ctx.db.delete(docId);
    }

    // Return data of specified size
    return generateData(args.returnBytes);
  },
});

// Configurable action that simulates long-running operations
export const configurableAction = internalAction({
  args: {
    durationMs: v.optional(v.number()), // How long to run
    returnBytes: v.number(), // Size of return value
    payload: v.any(), // Separate payload argument
    runId: v.id("runs"), // Run ID for tracking
  },
  handler: async (ctx, args) => {
    // Simulate work for specified duration
    await new Promise((resolve) =>
      setTimeout(resolve, args.durationMs ?? Math.random() * 1000),
    );

    // Return data of specified size
    return generateData(args.returnBytes);
  },
});

export const createSharedDoc = internalMutation({
  args: {},
  handler: async (ctx) => {
    return await ctx.db.insert("data", { data: 0 });
  },
});

// Mutation that writes to the shared doc, used to generate external OCC contention
export const bumpSharedDoc = internalMutation({
  args: { docId: v.id("data") },
  handler: async (ctx, args) => {
    const doc = await ctx.db.get(args.docId);
    if (doc) {
      await ctx.db.patch(args.docId, { data: (doc.data ?? 0) + 1 });
    }
  },
});

// Action that continuously writes to the shared doc to create OCC contention.
// Fires individual mutations with error handling so failures don't stop the
// contention. Uses a small delay between waves to avoid rate limits.
export const generateContention = internalAction({
  args: {
    docId: v.id("data"),
    durationMs: v.number(),
    concurrency: v.number(),
  },
  handler: async (ctx, { docId, durationMs, concurrency }) => {
    const start = Date.now();
    let totalWrites = 0;
    let errors = 0;
    while (Date.now() - start < durationMs) {
      const batch = Array.from({ length: concurrency }, () =>
        ctx
          .runMutation(internal.test.work.bumpSharedDoc, { docId })
          .then(() => true)
          .catch(() => {
            errors++;
            return false;
          }),
      );
      const results = await Promise.all(batch);
      totalWrites += results.filter(Boolean).length;
      // Small delay to avoid "too many concurrent commits" rate limit
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    console.log(
      `generateContention: ${totalWrites} succeeded, ${errors} errors in ${Date.now() - start}ms`,
    );
  },
});

export const markTaskCompleted = internalMutation({
  args: vOnCompleteArgs(
    v.object({
      runId: v.id("runs"),
      type: v.union(v.literal("mutation"), v.literal("action")),
      enqueuedAt: v.optional(v.number()),
      wave: v.optional(v.number()),
    }),
  ),
  handler: async (ctx, args) => {
    await ctx.db.insert("tasks", {
      runId: args.context.runId,
      workId: args.workId,
      type: args.context.type,
      endTime: Date.now(),
      enqueuedAt: args.context.enqueuedAt,
      wave: args.context.wave,
    });
  },
});

/**
 * Variant of markTaskCompleted that accepts large context data.
 * Used by bigContext scenario to test large context payloads.
 */
export const markTaskCompletedWithContext = internalMutation({
  args: vOnCompleteArgs(
    v.object({
      runId: v.id("runs"),
      type: v.union(v.literal("mutation"), v.literal("action")),
      largeData: v.string(), // Large context data for testing
    }),
  ),
  handler: async (ctx, args) => {
    // Log the size of the context data received
    const contextSize = args.context.largeData?.length ?? 0;
    console.log(`Received context with ${contextSize} bytes of data`);

    await ctx.db.insert("tasks", {
      runId: args.context.runId,
      workId: args.workId,
      type: args.context.type,
      endTime: Date.now(),
    });
  },
});

/**
 * Task type options for enqueueing
 */
export type TaskType = "mutation" | "action";

// Re-export WorkId for convenience
export type { WorkId };

/**
 * Helper function to enqueue tasks with either batch or individual enqueueing.
 * This is shared across all test scenarios for consistent task enqueueing behavior.
 *
 * Note: This function accepts fn and onCompleteOpts as parameters to avoid
 * circular type references when scenarios import from this module.
 *
 * @param options - Configuration options for enqueueing tasks
 * @returns Array of work IDs for the enqueued tasks
 */
export async function enqueueTasks<T extends DefaultFunctionArgs>(options: {
  ctx: ActionCtx;
  taskArgs: T[];
  taskType: TaskType;
  fn: Parameters<typeof enqueue>[3];
  onCompleteOpts: Parameters<typeof enqueue>[5];
  batchEnqueue?: boolean;
}): Promise<WorkId[]> {
  const {
    ctx,
    taskArgs,
    taskType,
    fn,
    onCompleteOpts,
    batchEnqueue = false,
  } = options;

  let workIds: WorkId[];

  if (batchEnqueue) {
    console.log("Using batch enqueue");
    workIds = await enqueueBatch(
      components.testWorkpool,
      ctx,
      taskType,
      fn,
      taskArgs,
      onCompleteOpts,
    );
  } else {
    console.log("Using individual enqueue");
    workIds = await Promise.all(
      taskArgs.map((a) =>
        enqueue(components.testWorkpool, ctx, taskType, fn, a, onCompleteOpts),
      ),
    );
  }

  return workIds;
}
