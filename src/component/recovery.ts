import { Id } from "./_generated/dataModel";
import { internalMutation } from "./_generated/server";
import { createLogger } from "./logging";
import { kickMainLoop } from "./kick";
import schema from "./schema";
import { RunResult, nextSegment } from "./shared";

export const recover = internalMutation({
  args: {
    jobs: schema.tables.internalState.validator.fields.running,
  },
  handler: async (ctx, { jobs }) => {
    const globals = await ctx.db.query("globals").unique();
    const console = createLogger(globals?.logLevel);
    const completed: { workId: Id<"work">; runResult: RunResult }[] = [];
    let didAnything = false;
    const segment = nextSegment();
    await Promise.all(
      jobs.map(async (job) => {
        const scheduled = await ctx.db.system.get(job.scheduledId);
        const preamble = `[recovery] Scheduled job ${job.scheduledId} for work ${job.workId}`;
        if (scheduled === null) {
          console.warn(`${preamble} not found`);
          completed.push({
            workId: job.workId,
            runResult: { kind: "failed", error: `Scheduled job not found` },
          });
          return;
        }
        // This will find everything that timed out, failed ungracefully, was
        // cancelled, or succeeded without a return value.
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
              await ctx.db.insert("pendingCompletion", {
                runResult: scheduled.state,
                workId: job.workId,
                segment,
              });
              didAnything = true;
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
              await ctx.db.insert("pendingCancelation", {
                workId: job.workId,
                segment,
              });
              didAnything = true;
            }
            break;
          }
        }
      })
    );
    if (didAnything) {
      await kickMainLoop(ctx, "recovery");
    }
  },
});
