import type { FunctionHandle } from "convex/server";
import { type Infer, v } from "convex/values";
import type { Id } from "./_generated/dataModel.js";
import { internalMutation, type MutationCtx } from "./_generated/server.js";
import { kickMainLoop } from "./kick.js";
import { createLogger } from "./logging.js";
import {
  type OnCompleteArgs,
  type RunResult,
  vResultValidator,
} from "./shared.js";
import { recordCompleted } from "./stats.js";

export type CompleteJob = Infer<typeof completeArgs.fields.jobs.element>;

export const completeArgs = v.object({
  jobs: v.array(
    v.object({
      runResult: vResultValidator,
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
  const pendingCompletions: {
    runResult: RunResult;
    workId: Id<"work">;
    retry: boolean;
  }[] = [];
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
              e,
            );
            // TODO: store failures in a table for later debugging
          }
        }
        recordCompleted(console, work, job.runResult.kind);
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
