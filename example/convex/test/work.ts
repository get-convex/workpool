import { internalMutation, internalAction } from "../_generated/server";
import { Infer, v } from "convex/values";
import { internal } from "../_generated/api";
import { vOnCompleteArgs, vWorkId, WorkId } from "@convex-dev/workpool";

// Shows how big the data is by printing the size progressively
// Generates strings in units of 10 characters
function generateData(len: number): string {
  let result = "";
  for (let i = 0; i < len; i += 10) {
    // Add a 10-character number with "." left-padding of the number i
    const number = i.toString().padStart(10, ".");
    result += number;
  }
  return result;
}

// Configurable mutation that simulates database operations
export const configurableMutation = internalMutation({
  args: {
    readWriteData: v.optional(v.number()), // If specified, write then delete this many bytes
    returnBytes: v.number(), // Size of return value
    payload: v.any(), // Separate payload argument
    taskNum: v.number(), // Task number for tracking
    runId: v.id("runs"), // Run ID for tracking
    hasOnComplete: v.boolean(), // Whether task has onComplete handler
  },
  handler: async (ctx, args) => {
    // Mark task as started
    const task = await ctx.db
      .query("tasks")
      .withIndex("runId_status_taskNum", (q) =>
        q
          .eq("runId", args.runId)
          .eq("status", "pending")
          .eq("taskNum", args.taskNum),
      )
      .unique();

    if (task) {
      await ctx.db.patch(task._id, {
        startTime: Date.now(),
        status: "running",
      });
    }

    // If readWriteData is specified, write then delete
    if (args.readWriteData && args.readWriteData > 0) {
      const dataToWrite = generateData(args.readWriteData);
      const docId = await ctx.db.insert("data", { misc: dataToWrite });
      // Delete immediately - this will read then write
      await ctx.db.delete(docId);
    }

    // If no onComplete handler, mark task as done
    if (!args.hasOnComplete && task) {
      await ctx.db.patch(task._id, {
        endTime: Date.now(),
        status: "completed",
      });
    }

    // Return data of specified size
    return generateData(args.returnBytes);
  },
});

// Configurable action that simulates long-running operations
export const configurableAction = internalAction({
  args: {
    durationMs: v.number(), // How long to run
    returnBytes: v.number(), // Size of return value
    payload: v.any(), // Separate payload argument
    taskNum: v.number(), // Task number for tracking
    runId: v.id("runs"), // Run ID for tracking
    hasOnComplete: v.boolean(), // Whether task has onComplete handler
  },
  handler: async (ctx, args) => {
    // Mark task as started
    const workId = await ctx.runMutation(internal.test.work.markTaskStarted, {
      runId: args.runId,
      taskNum: args.taskNum,
    });

    // Simulate work for specified duration
    await new Promise((resolve) => setTimeout(resolve, args.durationMs));

    const ret = generateData(args.returnBytes);
    // If no onComplete handler, mark task as done
    if (!args.hasOnComplete) {
      await ctx.runMutation(internal.test.work.markTaskCompleted, {
        workId,
        context: {
          runId: args.runId,
          taskNum: args.taskNum,
        },
        result: {
          kind: "success",
          returnValue: ret,
        },
      });
    }

    // Return data of specified size
    return ret;
  },
});

// Helper mutations for updating task status from actions
export const markTaskStarted = internalMutation({
  args: {
    runId: v.id("runs"),
    taskNum: v.number(),
  },
  returns: vWorkId,
  handler: async (ctx, args): Promise<WorkId> => {
    const task = await ctx.db
      .query("tasks")
      .withIndex("runId_status_taskNum", (q) =>
        q
          .eq("runId", args.runId)
          .eq("status", "pending")
          .eq("taskNum", args.taskNum),
      )
      .unique();

    if (task) {
      await ctx.db.patch(task._id, {
        startTime: Date.now(),
        status: "running",
      });
    } else {
      throw new Error("Task not found");
    }
    return task.workId;
  },
});

export const vContext = v.object({
  runId: v.id("runs"),
  taskNum: v.number(),
});
export type Context = Infer<typeof vContext>;

export const markTaskCompleted = internalMutation({
  args: vOnCompleteArgs(vContext),
  handler: async (ctx, args) => {
    const task = await ctx.db
      .query("tasks")
      .withIndex("runId_status_taskNum", (q) =>
        q
          .eq("runId", args.context.runId)
          .eq("status", "running")
          .eq("taskNum", args.context.taskNum),
      )
      .unique();

    if (task) {
      await ctx.db.patch(task._id, {
        endTime: Date.now(),
        status: "completed",
      });
    }
  },
});
