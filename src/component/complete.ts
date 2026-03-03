import type { FunctionHandle } from "convex/server";
import { getConvexSize, type Infer, v } from "convex/values";
import type { Id } from "./_generated/dataModel.js";
import { internal } from "./_generated/api.js";
import { internalMutation, type MutationCtx } from "./_generated/server.js";
import { kickMainLoop } from "./kick.js";
import { createLogger } from "./logging.js";
import { type OnCompleteArgs, type RunResult, vResult } from "./shared.js";
import { recordCompleted } from "./stats.js";
import { assert } from "convex-helpers";

export type CompleteJob = Infer<typeof completeArgs.fields.jobs.element>;

export const completeArgs = v.object({
  jobs: v.array(
    v.object({
      runResult: vResult,
      workId: v.id("work"),
      attempt: v.number(),
    }),
  ),
});
export async function completeHandler(
  ctx: MutationCtx,
  args: Infer<typeof completeArgs>,
) {
  const globals = await ctx.db.query("globals").unique();
  const console = createLogger(globals?.logLevel);
  if (args.jobs.length === 0) {
    console.warn("Trying to complete 0 jobs");
    return;
  }
  const pendingCompletions: {
    runResult: RunResult;
    workId: Id<"work">;
    retry: boolean;
  }[] = [];
  const jobAndWorks = (
    await Promise.all(
      args.jobs.map(async (job) => {
        const work = await ctx.db.get(job.workId);
        if (!work) {
          console.warn(
            `[complete] ${job.workId} is done, but its work is gone`,
          );
          return null;
        }
        if (work.attempts !== job.attempt) {
          console.warn(`[complete] ${job.workId} mismatched attempt number`);
          return null;
        }
        return { job, work };
      }),
    )
  ).filter((a) => a !== null);
  if (jobAndWorks.length === 0) {
    return;
  }
  const MAX_BATCH_SIZE = 2_000_000; // combined job / work / payload size

  // Create batches based on size
  const batches: (typeof jobAndWorks)[] = [];
  let currentBatch: typeof jobAndWorks = [];
  let currentBatchSize = 0;

  for (const item of jobAndWorks) {
    const itemSize =
      getConvexSize(item.job) +
      getConvexSize(item.work) +
      (item.work.payloadSize ?? 0);

    // If adding this item would exceed the limit, start a new batch
    if (
      currentBatch.length > 0 &&
      currentBatchSize + itemSize > MAX_BATCH_SIZE
    ) {
      batches.push(currentBatch);
      currentBatch = [];
      currentBatchSize = 0;
    }

    currentBatch.push(item);
    currentBatchSize += itemSize;
  }

  // Add the last batch if it has items
  if (currentBatch.length > 0) {
    batches.push(currentBatch);
  }

  // Schedule all batches after the first one
  for (let i = 1; i < batches.length; i++) {
    await ctx.scheduler.runAfter(0, internal.complete.complete, {
      jobs: batches[i]!.map(({ job }) => job),
    });
  }

  const ourBatch = batches[0];
  assert(ourBatch);

  await Promise.all(
    ourBatch.map(async ({ work, job }) => {
      work.attempts++;
      await ctx.db.patch(work._id, { attempts: work.attempts });
      const pendingCompletion = await ctx.db
        .query("pendingCompletion")
        .withIndex("workId", (q) => q.eq("workId", job.workId))
        .unique();
      if (pendingCompletion) {
        console.warn(`[complete] ${job.workId} already in pendingCompletion`);
        return;
      }
      const maxAttempts = work.retryBehavior?.maxAttempts;
      const retry =
        job.runResult.kind === "failed" &&
        !!maxAttempts &&
        work.attempts < maxAttempts;
      if (!retry) {
        if (work.onComplete) {
          try {
            // Retrieve large context if stored separately
            let context = work.onComplete.context;
            if (context === undefined && work.payloadId) {
              const payload = await ctx.db.get(work.payloadId);
              if (payload) {
                context = payload.context;
              }
            }

            const handle = work.onComplete.fnHandle as FunctionHandle<
              "mutation",
              OnCompleteArgs,
              void
            >;
            await ctx.runMutation(handle, {
              workId: work._id,
              context,
              result: job.runResult,
            });
            console.debug(`[complete] onComplete for ${job.workId} completed`);
          } catch (e) {
            console.error(
              `[complete] error running onComplete for ${job.workId}`,
              e,
            );
            // TODO: store failures in a table for later debugging
          }
        }
        recordCompleted(console, work, job.runResult.kind);

        // Clean up any large data that was stored separately.
        // TODO: consider async deletion in the future to avoid bandwidth limits.
        if (work.payloadId) {
          await ctx.db.delete(work.payloadId);
        }

        // This is the terminating state for work.
        await ctx.db.delete(job.workId);
      }
      if (job.runResult.kind !== "canceled") {
        pendingCompletions.push({
          runResult: stripResult(job.runResult),
          workId: job.workId,
          retry,
        });
      }
    }),
  );
  if (pendingCompletions.length > 0) {
    const segment = await kickMainLoop(ctx, "complete");
    await Promise.all(
      pendingCompletions.map((completion) =>
        ctx.db.insert("pendingCompletion", {
          ...completion,
          segment,
        }),
      ),
    );
  }
}

function stripResult(result: RunResult): RunResult {
  if (result.kind === "success") {
    return { kind: "success", returnValue: null };
  }
  return result;
}

export const complete = internalMutation({
  args: completeArgs,
  handler: completeHandler,
});
