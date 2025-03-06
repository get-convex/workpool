import { FunctionHandle } from "convex/server";
import { Infer, v } from "convex/values";
import { Doc } from "./_generated/dataModel.js";
import { internalMutation, MutationCtx } from "./_generated/server.js";
import { kickMainLoop } from "./kick.js";
import { createLogger, Logger } from "./logging.js";
import {
  boundScheduledTime,
  nextSegment,
  OnCompleteArgs,
  runResult,
  toSegment,
} from "./shared.js";
import { recordCompleted } from "./stats.js";

export const completeArgs = v.object({
  jobs: v.array(
    v.object({
      runResult: runResult,
      workId: v.id("work"),
    })
  ),
});
export async function completeHandler(
  ctx: MutationCtx,
  args: Infer<typeof completeArgs>
) {
  const globals = await ctx.db.query("globals").unique();
  const console = createLogger(globals?.logLevel);
  await Promise.all(
    args.jobs.map(async (job) => {
      const pendingCancelation = await ctx.db
        .query("pendingCancelation")
        .withIndex("workId", (q) => q.eq("workId", job.workId))
        .unique();
      // Ensure there aren't any pending cancelations for this work.
      if (pendingCancelation) {
        await ctx.db.delete(pendingCancelation._id);
      }
      const work = await ctx.db.get(job.workId);
      if (!work) {
        console.warn(`[complete] ${job.workId} is done, but its work is gone`);
        return;
      }
      const maxAttempts = work.retryBehavior?.maxAttempts;
      const retrying =
        job.runResult.kind === "failed" &&
        !!maxAttempts &&
        !pendingCancelation &&
        work.attempts < maxAttempts;
      if (retrying) {
        const existing = await ctx.db
          .query("pendingStart")
          .withIndex("workId", (q) => q.eq("workId", job.workId))
          .first();
        if (existing) {
          console.warn(
            `[complete] ${job.workId} is trying to retry, but already in pendingStart`
          );
          return;
        }
        await rescheduleJob(ctx, work, console);
      } else {
        if (work.onComplete) {
          try {
            const handle = work.onComplete.fnHandle as FunctionHandle<
              "mutation",
              OnCompleteArgs,
              void
            >;
            await ctx.runMutation(handle, {
              workId: work._id,
              context: work.onComplete.context,
              result: job.runResult,
            });
            console.debug(`[complete] onComplete for ${job.workId} completed`);
          } catch (e) {
            console.error(
              `[complete] error running onComplete for ${job.workId}`,
              e
            );
          }
        }
        // The presence of work is the lock that ensures onComplete only
        // runs once per work item.
        await ctx.db.delete(job.workId);
      }
      console.info(
        recordCompleted(work, retrying ? "retrying" : job.runResult.kind)
      );
      // Canceled jobs are accounted for in the main loop beforehand.
      if (job.runResult.kind !== "canceled") {
        await ctx.db.insert("pendingCompletion", {
          runResult: job.runResult,
          workId: job.workId,
          segment: nextSegment(),
          retrying,
        });
        await kickMainLoop(ctx, "complete");
      }
    })
  );
}

export const complete = internalMutation({
  args: completeArgs,
  handler: completeHandler,
});

export function withJitter(delay: number) {
  return delay * (0.5 + Math.random());
}

async function rescheduleJob(
  ctx: MutationCtx,
  work: Doc<"work">,
  console: Logger
): Promise<number> {
  if (!work.retryBehavior) {
    throw new Error("work has no retryBehavior");
  }
  const backoffMs =
    work.retryBehavior.initialBackoffMs *
    Math.pow(work.retryBehavior.base, work.attempts - 1);
  const nextAttempt = withJitter(backoffMs);
  const startTime = boundScheduledTime(Date.now() + nextAttempt, console);
  const segment = toSegment(startTime);
  await ctx.db.patch(work._id, {
    attempts: work.attempts + 1,
  });
  await ctx.db.insert("pendingStart", {
    workId: work._id,
    segment,
  });
  return nextAttempt;
}
