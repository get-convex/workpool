import { Infer } from "convex/values";
import { internalMutation } from "./_generated/server.js";
import { completeArgs, completeHandler } from "./complete.js";
import { createLogger } from "./logging.js";
import schema from "./schema.js";

export const recover = internalMutation({
  args: {
    jobs: schema.tables.internalState.validator.fields.running,
  },
  handler: async (ctx, { jobs }) => {
    const globals = await ctx.db.query("globals").unique();
    const console = createLogger(globals?.logLevel);
    const toComplete: Infer<typeof completeArgs.fields.jobs.element>[] = [];
    for (let i = 0; i < jobs.length; i++) {
      const job = jobs[i];
      const scheduled = await ctx.db.system.get(job.scheduledId);
      const preamble = `[recovery] Scheduled job ${job.scheduledId} for work ${job.workId}`;
      if (scheduled === null) {
        console.warn(`${preamble} not found`);
        toComplete.push({
          workId: job.workId,
          runResult: { kind: "failed", error: `Scheduled job not found` },
        });
        continue;
      }
      // This will find everything that timed out, failed ungracefully, was
      // canceled, or succeeded without a return value.
      switch (scheduled.state.kind) {
        case "failed": {
          console.debug(`${preamble} failed and detected in recovery`);
          const pendingCompletion = await ctx.db
            .query("pendingCompletion")
            .withIndex("workId", (q) => q.eq("workId", job.workId))
            .first();
          if (pendingCompletion) {
            console.debug(
              `${preamble} already in pendingCompletion, not reporting`
            );
          } else {
            toComplete.push({
              workId: job.workId,
              runResult: scheduled.state,
            });
          }
          break;
        }
        case "canceled": {
          console.debug(`${preamble} was canceled and detected in recovery`);
          const pendingCancelation = await ctx.db
            .query("pendingCancelation")
            .withIndex("workId", (q) => q.eq("workId", job.workId))
            .first();
          if (pendingCancelation) {
            console.debug(
              `${preamble} already in pendingCancelation, not reporting`
            );
          } else {
            toComplete.push({
              workId: job.workId,
              runResult: { kind: "canceled" },
            });
          }
          break;
        }
      }
    }
    if (toComplete.length > 0) {
      await completeHandler(ctx, { jobs: toComplete });
    }
  },
});
