import { type Infer, v } from "convex/values";
import { internalMutation, type MutationCtx } from "./_generated/server.js";
import { completeArgs, completeHandler } from "./complete.js";
import { createLogger } from "./logging.js";

const recoveryArgs = v.object({
  jobs: v.array(
    v.object({
      scheduledId: v.id("_scheduled_functions"),
      workId: v.id("work"),
      attempt: v.number(),
      started: v.number(),
    }),
  ),
});

/**
 * This can run when things fail because of server failures / restarts, or when
 * the user cancels scheduled jobs (from the dashboard).
 * Possible states it could be in at the moment this executes:
 * - in internalState.running and complete was never called
 *   -> we should call completeHandler with failure.
 * - complete already called, no action needed (only possible for actions):
 *  - In pendingCompletion still and internalState.running.
 *    -> check for pendingCompletion.
 *  - pendingCompletion already processed.
 *   - No retry: work was deleted, not in internalState.running.
 *     -> check for work.
 *   - Retry: attempts will mismatch
 *     -> check work.attempts
 */
export const recover = internalMutation({
  args: recoveryArgs,
  handler: recoveryHandler,
});

// only exported for testing
export async function recoveryHandler(
  ctx: MutationCtx,
  { jobs }: Infer<typeof recoveryArgs>,
) {
  const globals = await ctx.db.query("globals").unique();
  const console = createLogger(globals?.logLevel);
  const toComplete: Infer<typeof completeArgs.fields.jobs> = [];
  for (let i = 0; i < jobs.length; i++) {
    const job = jobs[i];
    const preamble = `[recovery] Scheduled job ${job.scheduledId} for work ${job.workId}`;
    const pendingCompletion = await ctx.db
      .query("pendingCompletion")
      .withIndex("workId", (q) => q.eq("workId", job.workId))
      .first();
    if (pendingCompletion) {
      // Completion already pending, no need to do anything.
      console.debug(`${preamble} already in pendingCompletion, skipping`);
      continue;
    }
    const work = await ctx.db.get(job.workId);
    if (work === null) {
      // Completion already executed w/o retries, no need to do anything.
      console.warn(`${preamble} work not found, skipping`);
      continue;
    }
    if (work.attempts !== job.attempt) {
      // Retry already started, no need to do anything.
      console.warn(`${preamble} attempts mismatch, skipping`);
      continue;
    }
    const scheduled = await ctx.db.system.get(job.scheduledId);
    if (scheduled === null) {
      console.warn(`${preamble} not found in _scheduled_functions`);
      toComplete.push({
        workId: job.workId,
        runResult: { kind: "failed", error: `Scheduled job not found` },
        attempt: job.attempt,
      });
      continue;
    }
    // This will find everything that timed out, failed ungracefully, was
    // canceled, or succeeded without a return value.
    switch (scheduled.state.kind) {
      case "failed": {
        console.debug(`${preamble} failed and detected in recovery`);
        toComplete.push({
          workId: job.workId,
          runResult: scheduled.state,
          attempt: job.attempt,
        });
        break;
      }
      case "canceled": {
        console.debug(`${preamble} was canceled and detected in recovery`);
        toComplete.push({
          workId: job.workId,
          runResult: { kind: "failed", error: "Canceled via scheduler" },
          attempt: job.attempt,
        });
        break;
      }
    }
  }
  if (toComplete.length > 0) {
    await completeHandler(ctx, { jobs: toComplete });
  }
}
