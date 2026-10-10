import { type Infer, v } from "convex/values";
import { internalMutation, type MutationCtx } from "./_generated/server.js";
import { createLogger } from "./logging.js";
import { type CompleteJob, completeHandler } from "./complete.js";
import { MINUTE } from "./shared.js";

const recoveryArgs = v.object({
  jobs: v.array(
    v.object({
      scheduledId: v.id("_scheduled_functions"),
      workId: v.id("work"),
      attempt: v.number(),
      started: v.number(),
    }),
  ),
  // Time at which the main loop scheduled this recovery to run (≈ Date.now()
  // at scheduling time, since we use runAfter(0, ...)). We use the delta vs.
  // now to estimate how backlogged the scheduler is, then judge "stuck"
  // relative to that lag — see MUTATION_STUCK_THRESHOLD_MS below.
  scheduledAt: v.optional(v.number()),
});

// A mutation that's been "pending" in the scheduler for this long beyond the
// current scheduler lag is treated as stuck. We cancel and re-enqueue it so
// it can free up a workpool slot — staying pending past the scheduler's own
// backlog usually means a doc-write conflict the scheduler keeps losing.
const MUTATION_STUCK_THRESHOLD_MS = 10 * MINUTE;

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
  { jobs, scheduledAt }: Infer<typeof recoveryArgs>,
) {
  const globals = await ctx.db.query("globals").unique();
  const console = createLogger(globals?.logLevel);
  // If scheduledAt is omitted (older callers, or tests), treat lag as 0.
  const schedulerLagMs = scheduledAt ? Math.max(0, Date.now() - scheduledAt) : 0;
  const completionJobs: CompleteJob[] = [];
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
    const work = await ctx.db.get("work", job.workId);
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
    const scheduled = await ctx.db.system.get(
      "_scheduled_functions",
      job.scheduledId,
    );
    if (scheduled === null) {
      console.warn(`${preamble} not found in _scheduled_functions`);
      completionJobs.push({
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
        completionJobs.push({
          workId: job.workId,
          runResult: scheduled.state,
          attempt: job.attempt,
        });
        break;
      }
      case "canceled": {
        console.debug(`${preamble} was canceled and detected in recovery`);
        completionJobs.push({
          workId: job.workId,
          runResult: { kind: "failed", error: "Canceled via scheduler" },
          attempt: job.attempt,
        });
        break;
      }
      case "pending": {
        // We only re-enqueue mutations. Actions sitting in pending usually
        // mean an overloaded scheduler — cancelling them doesn't help.
        if (work.fnType !== "mutation") {
          break;
        }
        const pendingMs = Date.now() - scheduled.scheduledTime;
        if (pendingMs < MUTATION_STUCK_THRESHOLD_MS + schedulerLagMs) {
          break;
        }
        console.warn(
          `${preamble} pending in scheduler for ${pendingMs}ms ` +
            `(lag ${schedulerLagMs}ms, threshold ${MUTATION_STUCK_THRESHOLD_MS}ms) — re-enqueueing`,
        );
        await ctx.scheduler.cancel(scheduled._id);
        completionJobs.push({
          workId: job.workId,
          runResult: { kind: "stuckInScheduler" },
          attempt: job.attempt,
        });
        break;
      }
    }
  }
  if (completionJobs.length > 0) {
    await completeHandler(ctx, { jobs: completionJobs });
  }
}
