import { internalAction } from "../../_generated/server";
import { v } from "convex/values";
import { internal, components } from "../../_generated/api";
import { Workpool, WorkId } from "@convex-dev/workpool";

const testWorkpool = new Workpool(components.testWorkpool, {});

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
  useOnComplete: v.optional(v.boolean()),
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
      maxParallelism,
      taskCount = 50,
      argSizeBytes = 800_000, // 800KB default
      taskType = "mutation",
      useBatchEnqueue = false,
      useOnComplete = false,
    } = args;
    if (maxParallelism !== undefined) {
      await ctx.runMutation(components.testWorkpool.config.update, {
        maxParallelism: maxParallelism,
      });
    }

    console.log(
      `Starting bigArgs test with ${taskCount} tasks of ${argSizeBytes} bytes each`,
    );

    // Generate the large payload once
    const payload = generatePayload(argSizeBytes);
    const baseArgs = {
      payload,
      returnBytes: 100, // Small return
      runId,
      hasOnComplete: useOnComplete,
    };

    const onCompleteOpts = useOnComplete
      ? {
          onComplete: internal.test.work.markTaskCompleted,
          context: {},
        }
      : undefined;

    let workIds: WorkId[];

    if (taskType === "mutation") {
      const taskArgs = Array.from({ length: taskCount }, (_, i) => ({
        ...baseArgs,
        taskNum: i,
        readWriteData: 0,
      }));
      if (useBatchEnqueue) {
        console.log("Using batch enqueue");
        workIds = await testWorkpool.enqueueMutationBatch(
          ctx,
          internal.test.work.configurableMutation,
          taskArgs,
          onCompleteOpts,
        );
      } else {
        console.log("Using individual enqueue");
        workIds = await Promise.all(
          taskArgs.map((a) =>
            testWorkpool.enqueueMutation(
              ctx,
              internal.test.work.configurableMutation,
              a,
              onCompleteOpts,
            ),
          ),
        );
      }
    } else {
      const taskArgs = Array.from({ length: taskCount }, (_, i) => ({
        ...baseArgs,
        taskNum: i,
        durationMs: 100,
      }));
      if (useBatchEnqueue) {
        console.log("Using batch enqueue");
        workIds = await testWorkpool.enqueueActionBatch(
          ctx,
          internal.test.work.configurableAction,
          taskArgs,
          onCompleteOpts,
        );
      } else {
        console.log("Using individual enqueue");
        workIds = await Promise.all(
          taskArgs.map((a) =>
            testWorkpool.enqueueAction(
              ctx,
              internal.test.work.configurableAction,
              a,
              onCompleteOpts,
            ),
          ),
        );
      }
    }

    // Track all tasks
    await ctx.runMutation(internal.test.work.trackTaskBatch, {
      tasks: workIds.map((workId, i) => ({
        runId,
        taskNum: i,
        workId,
        type: taskType,
        hasOnComplete: useOnComplete,
      })),
    });

    console.log(
      `Enqueued ${workIds.length} tasks with ${argSizeBytes} byte payloads`,
    );
    return { workIds, taskCount, argSizeBytes };
  },
});
