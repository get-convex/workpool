import {
  internalMutation,
  QueryCtx,
  internalQuery,
} from "../_generated/server";
import { v } from "convex/values";
import { Id } from "../_generated/dataModel";

export async function runStatus(ctx: QueryCtx, runId: Id<"runs">) {
  const last = await ctx.db
    .query("tasks")
    .withIndex("runId_status_taskNum", (q) => q.eq("runId", runId))
    .order("desc")
    .first();
  return last?.status;
}

// Start a new test run
export const start = internalMutation({
  args: {
    scenario: v.string(),
    parameters: v.any(),
  },
  handler: async (ctx, args) => {
    // Check for in-flight tasks from the latest run
    const latestRun = await ctx.db.query("runs").order("desc").first();

    if (latestRun) {
      // Check if there are any in-flight tasks
      const status = await runStatus(ctx, latestRun._id);

      if (status && ["running", "pending"].includes(status)) {
        throw new Error(
          `Cannot start new run: previous run ${latestRun.scenario} is ${status} (started at ${new Date(latestRun.startTime).toISOString()})`,
        );
      }
      if (latestRun.startTime + 5_000 > Date.now()) {
        throw new Error(
          `Cannot start new run: previous run ${latestRun.scenario} was started less than 5 seconds ago (started at ${new Date(latestRun.startTime).toISOString()})`,
        );
      }
    }

    // Create new run
    const runId = await ctx.db.insert("runs", {
      startTime: Date.now(),
      scenario: args.scenario,
      parameters: args.parameters,
    });

    return runId;
  },
});

// Get the status of the latest run
export const status = internalQuery({
  handler: async (ctx) => {
    const latestRun = await ctx.db.query("runs").order("desc").first();

    if (!latestRun) {
      return null;
    }
    const status = await runStatus(ctx, latestRun._id);

    return { run: latestRun, status };
  },
});
