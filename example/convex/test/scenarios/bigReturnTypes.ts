import { internalAction } from "../../_generated/server";
import { v } from "convex/values";
import { internal } from "../../_generated/api";
import { enqueueTasks, TaskType } from "../work";
import { Id } from "../../_generated/dataModel";
import { WorkId } from "@convex-dev/workpool";

/**
 * Big Return Types Scenario
 *
 * Tests handling of large return values from functions.
 * Unlike bigArgs which tests large input payloads, this scenario
 * tests how the system handles large data being returned from tasks.
 */

const parameters = {
  taskCount: v.optional(v.number()),
  returnSizeBytes: v.optional(v.number()),
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
      returnSizeBytes = 10_000_000, // 10MB
      taskType = "mutation",
      useBatchEnqueue = false,
      maxParallelism = 50,
    },
  ): Promise<{
    workIds: WorkId[];
    taskCount: number;
    returnSizeBytes: number;
  }> => {
    const runId: Id<"runs"> = await ctx.runMutation(internal.test.run.start, {
      scenario: "bigReturnTypes",
      parameters: {
        taskCount,
        returnSizeBytes,
        taskType,
        useBatchEnqueue,
        maxParallelism,
      },
    });

    console.log(
      `Starting bigReturnTypes test with ${taskCount} tasks returning ${returnSizeBytes} bytes each`,
    );

    // Each task will return large data but have small input
    const baseArgs = {
      payload: "", // Small/empty payload
      returnBytes: returnSizeBytes, // Large return value
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
      useBatchEnqueue,
    });

    console.log(
      `Enqueued ${workIds.length} tasks with ${returnSizeBytes} byte return values`,
    );
    return { workIds, taskCount, returnSizeBytes };
  },
});
