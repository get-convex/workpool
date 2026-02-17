import { internalAction } from "../../_generated/server";
import { v } from "convex/values";
import { internal, components } from "../../_generated/api";
import { WorkId, enqueueBatch, enqueue } from "@convex-dev/workpool";

// Helper to generate payload of specified size
function generatePayload(sizeBytes: number): string {
  let result = "";
  const chunk = "0123456789"; // 10 bytes
  const numChunks = Math.floor(sizeBytes / 10);
  for (let i = 0; i < numChunks; i++) {
    result += chunk;
  }
  // Add remaining bytes
  const remaining = sizeBytes % 10;
  if (remaining > 0) {
    result += chunk.substring(0, remaining);
  }
  return result;
}

const parameters = {
  taskCount: v.optional(v.number()),
  argSizeBytes: v.optional(v.number()),
  taskType: v.optional(v.union(v.literal("mutation"), v.literal("action"))),
  useBatchEnqueue: v.optional(v.boolean()),
  maxParallelism: v.optional(v.number()),
};

export default internalAction({
  args: parameters,
  handler: async (ctx, args) => {
    const runId = await ctx.runMutation(internal.test.run.start, {
      scenario: "bigArgs",
      parameters: args,
    });
    const {
      taskCount = 50,
      argSizeBytes = 800_000, // 800KB default
      taskType = "mutation",
      useBatchEnqueue = false,
    } = args;

    console.log(
      `Starting bigArgs test with ${taskCount} tasks of ${argSizeBytes} bytes each`,
    );

    // Generate the large payload once
    const payload = generatePayload(argSizeBytes);
    const baseArgs = {
      payload,
      returnBytes: 100, // Small return
      runId,
    };

    const onCompleteOpts = {
      onComplete: internal.test.work.markTaskCompleted,
      context: { runId, type: taskType },
    };

    let workIds: WorkId[];
    const taskArgs = Array(taskCount).fill(baseArgs);
    const fn =
      taskType === "action"
        ? internal.test.work.configurableAction
        : internal.test.work.configurableMutation;

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
          enqueue(
            components.testWorkpool,
            ctx,
            taskType,
            fn,
            a,
            onCompleteOpts,
          ),
        ),
      );
    }

    console.log(
      `Enqueued ${workIds.length} tasks with ${argSizeBytes} byte payloads`,
    );
    return { workIds, taskCount, argSizeBytes };
  },
});
