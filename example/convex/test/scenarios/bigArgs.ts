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

export const start = internalAction({
  args: {
    runId: v.id("runs"),
    parameters: v.object({
      taskCount: v.optional(v.number()),
      argSizeBytes: v.optional(v.number()),
      taskType: v.optional(v.union(v.literal("mutation"), v.literal("action"))),
      useBatchEnqueue: v.optional(v.boolean()),
      useOnComplete: v.optional(v.boolean()),
      maxParallelism: v.optional(v.number()),
    }),
  },
  handler: async (ctx, args) => {
    const {
      taskCount = 50,
      argSizeBytes = 800_000, // 800KB default
      taskType = "mutation",
      useBatchEnqueue = false,
      useOnComplete = false,
    } = args.parameters;

    console.log(`Starting bigArgs test with ${taskCount} tasks of ${argSizeBytes} bytes each`);

    // Generate the large payload once
    const payload = generatePayload(argSizeBytes);

    const workIds: WorkId[] = [];

    if (useBatchEnqueue) {
      // Batch enqueue all tasks at once
      console.log("Using batch enqueue");

      if (taskType === "mutation") {
        const taskArgs = [];
        for (let i = 0; i < taskCount; i++) {
          taskArgs.push({
            payload,
            returnBytes: 100, // Small return
            taskNum: i,
            runId: args.runId,
            hasOnComplete: useOnComplete,
            readWriteData: 0, // No DB operations for this test
          });
        }
        const ids = await dynamicWorkpool.enqueueMutationBatch(
          ctx,
          internal.test.work.configurableMutation,
          taskArgs,
          useOnComplete ? {
            onComplete: internal.test.work.markTaskCompleted,
            context: { runId: args.runId, taskNum: -1 }, // Will be overridden per task
          } : undefined
        );
        workIds.push(...ids);
      } else {
        const taskArgs = [];
        for (let i = 0; i < taskCount; i++) {
          taskArgs.push({
            payload,
            returnBytes: 100, // Small return
            taskNum: i,
            runId: args.runId,
            hasOnComplete: useOnComplete,
            durationMs: 100, // Short duration
          });
        }
        const ids = await dynamicWorkpool.enqueueActionBatch(
          ctx,
          internal.test.work.configurableAction,
          taskArgs,
          useOnComplete ? {
            onComplete: internal.test.work.markTaskCompleted,
            context: { runId: args.runId, taskNum: -1 }, // Will be overridden per task
          } : undefined
        );
        workIds.push(...ids);
      }
    } else {
      // Enqueue tasks individually
      console.log("Using individual enqueue");

      for (let i = 0; i < taskCount; i++) {
        const baseArgs = {
          payload,
          returnBytes: 100, // Small return
          taskNum: i,
          runId: args.runId,
          hasOnComplete: useOnComplete,
        };

        let workId: WorkId;
        if (taskType === "mutation") {
          workId = await dynamicWorkpool.enqueueMutation(
            ctx,
            internal.test.work.configurableMutation,
            {
              ...baseArgs,
              readWriteData: 0, // No DB operations for this test
            },
            useOnComplete ? {
              onComplete: internal.test.work.markTaskCompleted,
              context: { runId: args.runId, taskNum: i },
            } : undefined
          );
        } else {
          workId = await dynamicWorkpool.enqueueAction(
            ctx,
            internal.test.work.configurableAction,
            {
              ...baseArgs,
              durationMs: 100, // Short duration
            },
            useOnComplete ? {
              onComplete: internal.test.work.markTaskCompleted,
              context: { runId: args.runId, taskNum: i },
            } : undefined
          );
        }

        workIds.push(workId);

        // Track the task
        await ctx.runMutation(internal.test.run.trackTask, {
          runId: args.runId,
          taskNum: i,
          workId,
          type: taskType,
          hasOnComplete: useOnComplete,
        });
      }
    }

    // If using batch enqueue, track all tasks now
    if (useBatchEnqueue) {
      for (let i = 0; i < workIds.length; i++) {
        await ctx.runMutation(internal.test.run.trackTask, {
          runId: args.runId,
          taskNum: i,
          workId: workIds[i],
          type: taskType,
          hasOnComplete: useOnComplete,
        });
      }
    }

    console.log(`Enqueued ${workIds.length} tasks with ${argSizeBytes} byte payloads`);
    return { workIds, taskCount, argSizeBytes };
  },
});