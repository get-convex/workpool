import { internalAction } from "../../_generated/server";
import { v } from "convex/values";
import { internal } from "../../_generated/api";
import { generateData, enqueueTasks, TaskType } from "../work";
import { Id } from "../../_generated/dataModel";
import { WorkId } from "@convex-dev/workpool";

const parameters = {
  taskCount: v.optional(v.number()),
  argSizeBytes: v.optional(v.number()),
  taskType: v.optional(v.union(v.literal("mutation"), v.literal("action"))),
  batchEnqueue: v.optional(v.boolean()),
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
      batchEnqueue = false,
      maxParallelism = 50,
    },
  ): Promise<{
    workIds: WorkId[];
    taskCount: number;
    argSizeBytes: number;
  }> => {
    const runId: Id<"runs"> = await ctx.runMutation(internal.test.run.start, {
      scenario: "bigArgs",
      parameters: {
        taskCount,
        argSizeBytes,
        taskType,
        batchEnqueue,
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

    // Select the appropriate function based on task type
    const fn =
      taskType === "action"
        ? internal.test.work.configurableAction
        : internal.test.work.configurableMutation;

    const onCompleteOpts = {
      onComplete: internal.test.work.markTaskCompleted,
      context: { runId, type: taskType as TaskType },
    };

    // Use shared enqueueTasks helper
    const workIds = await enqueueTasks({
      ctx,
      taskArgs,
      taskType,
      fn,
      onCompleteOpts,
      batchEnqueue,
    });

    console.log(
      `Enqueued ${workIds.length} tasks with ${argSizeBytes} byte payloads`,
    );
    return { workIds, taskCount, argSizeBytes };
  },
});
