import { internalMutation, internalAction } from "../_generated/server";
import { v } from "convex/values";
import { internal, components } from "../_generated/api";
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
  let result = "";
  const chunk = "0123456789"; // 10 bytes
  const numChunks = Math.floor(len / 10);
  for (let i = 0; i < numChunks; i++) {
    result += chunk;
  }
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
  },
  handler: async (ctx, args) => {
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

export const markTaskCompleted = internalMutation({
  args: vOnCompleteArgs(
    v.object({
      runId: v.id("runs"),
      type: v.union(v.literal("mutation"), v.literal("action")),
    }),
  ),
  handler: async (ctx, args) => {
    await ctx.db.insert("tasks", {
      runId: args.context.runId,
      workId: args.workId,
      type: args.context.type,
      endTime: Date.now(),
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

/**
 * Options for enqueueing tasks
 */
export interface EnqueueTasksOptions<T> {
  ctx: ActionCtx;
  runId: ReturnType<typeof v.id<"runs">> extends { type: infer U } ? U : never;
  taskArgs: T[];
  taskType: TaskType;
  useBatchEnqueue?: boolean;
}

/**
 * Helper function to enqueue tasks with either batch or individual enqueueing.
 * This is shared across all test scenarios for consistent task enqueueing behavior.
 *
 * @param options - Configuration options for enqueueing tasks
 * @returns Array of work IDs for the enqueued tasks
 */
export async function enqueueTasks<
  T extends { runId: string; returnBytes: number },
>(options: {
  ctx: ActionCtx;
  runId: string;
  taskArgs: T[];
  taskType: TaskType;
  useBatchEnqueue?: boolean;
}): Promise<WorkId[]> {
  const { ctx, runId, taskArgs, taskType, useBatchEnqueue = false } = options;

  const onCompleteOpts = {
    onComplete: internal.test.work.markTaskCompleted,
    context: { runId, type: taskType },
  };

  const fn =
    taskType === "action"
      ? internal.test.work.configurableAction
      : internal.test.work.configurableMutation;

  let workIds: WorkId[];

  if (useBatchEnqueue) {
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
