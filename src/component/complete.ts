import { FunctionHandle } from "convex/server";
import { Infer, v } from "convex/values";
import { internalMutation, MutationCtx } from "./_generated/server.js";
import { kickMainLoop } from "./kick.js";
import { createLogger } from "./logging.js";
import { nextSegment, OnCompleteArgs, runResult } from "./shared.js";
import { recordCompleted } from "./stats.js";

export type CompleteJob = Infer<typeof completeArgs.fields.jobs.element>;

export const completeArgs = v.object({
  jobs: v.array(
    v.object({
      runResult: runResult,
      workId: v.id("work"),
      attempt: v.number(),
    })
  ),
});
export async function completeHandler(
  ctx: MutationCtx,
  args: Infer<typeof completeArgs>
) {
  const globals = await ctx.db.query("globals").unique();
  const console = createLogger(globals?.logLevel);
  let anyPendingCompletions = false;
  await Promise.all(
    args.jobs.map(async (job) => {
      const work = await ctx.db.get(job.workId);
      if (!work) {
        console.warn(`[complete] ${job.workId} is done, but its work is gone`);
        return;
      }
      if (work.attempts !== job.attempt) {
        console.warn(`[complete] ${job.workId} mismatched attempt number`);
        return;
      }
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
            // TODO: store failures in a table for later debugging
          }
        }
        console.info(recordCompleted(work, job.runResult.kind));
        // This is the terminating state for work.
        await ctx.db.delete(job.workId);
      }
      if (job.runResult.kind !== "canceled") {
        await ctx.db.insert("pendingCompletion", {
          runResult: job.runResult,
          workId: job.workId,
          segment: nextSegment(),
          retry,
        });
        anyPendingCompletions = true;
      }
    })
  );
  if (anyPendingCompletions) {
    await kickMainLoop(ctx, "complete");
  }
}

export const complete = internalMutation({
  args: completeArgs,
  handler: completeHandler,
});
