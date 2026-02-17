import { internalMutation, internalAction } from "../_generated/server";
import { v } from "convex/values";
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
    durationMs: v.optional(v.number()), // How long to run
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
    await new Promise((resolve) =>
      setTimeout(resolve, args.durationMs ?? Math.random() * 1000),
    );

    const ret = generateData(args.returnBytes);
    // If no onComplete handler, mark task as done
    if (!args.hasOnComplete) {
      await ctx.runMutation(internal.test.work.markTaskCompleted, {
        workId,
        context: {},
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

export const markTaskCompleted = internalMutation({
  args: vOnCompleteArgs(),
  handler: async (ctx, args) => {
    const task = await ctx.db
      .query("tasks")
      .withIndex("workId", (q) => q.eq("workId", args.workId))
      .unique();

    if (task) {
      await ctx.db.patch(task._id, {
        endTime: Date.now(),
        status: "completed",
      });
    }
  },
});

const trackTaskArgs = {
  runId: v.id("runs"),
  taskNum: v.number(),
  workId: vWorkId,
  type: v.union(v.literal("mutation"), v.literal("action")),
  hasOnComplete: v.boolean(),
};

export const trackTask = internalMutation({
  args: trackTaskArgs,
  handler: async (ctx, args) => {
    await ctx.db.insert("tasks", {
      runId: args.runId,
      taskNum: args.taskNum,
      workId: args.workId,
      type: args.type,
      status: "pending",
      hasOnComplete: args.hasOnComplete,
    });
  },
});

export const trackTaskBatch = internalMutation({
  args: {
    tasks: v.array(v.object(trackTaskArgs)),
  },
  handler: async (ctx, args) => {
    for (const task of args.tasks) {
      await ctx.db.insert("tasks", {
        runId: task.runId,
        taskNum: task.taskNum,
        workId: task.workId,
        type: task.type,
        status: "pending",
        hasOnComplete: task.hasOnComplete,
      });
    }
  },
});
