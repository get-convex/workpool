import { internalMutation, internalAction } from "../_generated/server";
import { v } from "convex/values";

import { vOnCompleteArgs } from "@convex-dev/workpool";

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
  args: vOnCompleteArgs(),
  handler: async (ctx, args) => {
    await ctx.db.insert("tasks", {
      runId: args.context.runId,
      workId: args.workId,
      type: args.context.type,
      endTime: Date.now(),
    });
  },
});
