import { internalAction } from "../../_generated/server";
import { v } from "convex/values";
import { internal } from "../../_generated/api";
import { generateData, enqueueTasks } from "../work";

const parameters = {
  taskCount: v.optional(v.number()),
  argSizeBytes: v.optional(v.number()),
  taskType: v.optional(v.union(v.literal("mutation"), v.literal("action"))),
  useBatchEnqueue: v.optional(v.boolean()),
  maxParallelism: v.optional(v.number()),
};

export default internalAction({
  args: parameters,
  handler: async (
    ctx,
    {
      taskCount = 50,
      argSizeBytes = 800_000, // 800KB default
      taskType = "mutation",
      useBatchEnqueue = false,
      maxParallelism = 50,
    },
  ) => {
    const runId = await ctx.runMutation(internal.test.run.start, {
      scenario: "bigArgs",
      parameters: {
        taskCount,
        argSizeBytes,
        taskType,
        useBatchEnqueue,
        maxParallelism,
      },
    });

    console.log(
      `Starting bigArgs test with ${taskCount} tasks of ${argSizeBytes} bytes each`,
    );

    // Generate the large payload once using shared generateData
    const payload = generateData(argSizeBytes);
    const baseArgs = {
      payload,
      returnBytes: 100, // Small return
      runId,
    };

    const taskArgs = Array(taskCount).fill(baseArgs);

    // Use shared enqueueTasks helper
    const workIds = await enqueueTasks({
      ctx,
      runId,
      taskArgs,
      taskType,
      useBatchEnqueue,
    });

    console.log(
      `Enqueued ${workIds.length} tasks with ${argSizeBytes} byte payloads`,
    );
    return { workIds, taskCount, argSizeBytes };
  },
});
