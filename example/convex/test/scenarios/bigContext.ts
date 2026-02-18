import { internalAction } from "../../_generated/server";
import { v } from "convex/values";
import { internal, components } from "../../_generated/api";
import { generateData } from "../work";
import { WorkId, enqueueBatch, enqueue } from "@convex-dev/workpool";

/**
 * Big Context Scenario
 *
 * Tests handling of large data in the onComplete context variable.
 * Unlike bigArgs which tests large input payloads in function arguments,
 * this scenario tests how the system handles large context data passed
 * through the onComplete callback mechanism.
 */

const parameters = {
  taskCount: v.optional(v.number()),
  contextSizeBytes: v.optional(v.number()),
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
      contextSizeBytes = 800_000, // 800KB default context size
      taskType = "mutation",
      useBatchEnqueue = false,
      maxParallelism = 50,
    },
  ) => {
    const runId = await ctx.runMutation(internal.test.run.start, {
      scenario: "bigContext",
      parameters: {
        taskCount,
        contextSizeBytes,
        taskType,
        useBatchEnqueue,
        maxParallelism,
      },
    });

    console.log(
      `Starting bigContext test with ${taskCount} tasks with ${contextSizeBytes} byte context each`,
    );

    // Generate the large context payload once
    const largeContextData = generateData(contextSizeBytes);

    // Task args are small - the large data goes in context
    const baseArgs = {
      payload: "", // Small/empty payload
      returnBytes: 100, // Small return
      runId,
    };

    // Large context data in onComplete options
    const onCompleteOpts = {
      onComplete: internal.test.work.markTaskCompletedWithContext,
      context: {
        runId,
        type: taskType,
        largeData: largeContextData, // Large data in context
      },
    };

    const taskArgs = Array(taskCount).fill(baseArgs);

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

    console.log(
      `Enqueued ${workIds.length} tasks with ${contextSizeBytes} byte context data`,
    );
    return { workIds, taskCount, contextSizeBytes };
  },
});
