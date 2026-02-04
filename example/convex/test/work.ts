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
    readBytes: v.number(), // How many bytes to simulate reading
    writeBytes: v.number(), // How many bytes to write to DB
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

    // Simulate reading data (could query actual data if needed)
    if (args.readBytes > 0) {
      // In a real scenario, you might query data from the DB
      // For now, we just simulate the operation
      const docs = await ctx.db
        .query("data")
        .take(Math.min(100, args.readBytes / 100));
    }

    // Simulate writing data
    if (args.writeBytes > 0) {
      const dataToWrite = generateData(Math.min(args.writeBytes, 10000)); // Cap individual writes
      await ctx.db.insert("data", { data: dataToWrite.length });
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
