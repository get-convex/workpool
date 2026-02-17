import {
  internalMutation,
  QueryCtx,
  internalQuery,
} from "../_generated/server";
import { v } from "convex/values";
import { Id } from "../_generated/dataModel";
import { assert } from "convex-helpers";
import { components } from "../_generated/api";

export async function runStatus(
  ctx: QueryCtx,
  run: { _id: Id<"runs">; taskCount?: number },
) {
  const completedTasks = await ctx.db
    .query("tasks")
    .withIndex("runId", (q) => q.eq("runId", run._id))
    .collect();
  const completedCount = completedTasks.length;
  if (run.taskCount === undefined) {
    return "pending";
  }
  if (completedCount < run.taskCount) {
    return "running";
  }
  return "completed";
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
      const status = await runStatus(ctx, latestRun);

      if (["running", "pending"].includes(status)) {
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
    if (args.parameters.maxParallelism !== undefined) {
      await ctx.runMutation(components.testWorkpool.config.update, {
        maxParallelism: args.parameters.maxParallelism,
      });
    }

    // Create new run
    const runId = await ctx.db.insert("runs", {
      startTime: Date.now(),
      scenario: args.scenario,
      parameters: args.parameters,
      taskCount: args.parameters.taskCount,
    });

    return runId;
  },
});

export const cancel = internalMutation({
  args: {
    runId: v.optional(v.id("runs")),
  },
  handler: async (ctx, args) => {
    const runId =
      args.runId ?? (await ctx.db.query("runs").order("desc").first())?._id;
    if (!runId) throw new Error("No run found");
    const run = await ctx.db.get("runs", runId);
    assert(run);
    console.log(`Deleting run ${runId}: ${JSON.stringify(run)}`);
    await ctx.db.delete("runs", runId);
  },
});

// Get the status of the latest run
export const status = internalQuery({
  handler: async (ctx) => {
    const latestRun = await ctx.db.query("runs").order("desc").first();

    if (!latestRun) {
      return null;
    }
    const status = await runStatus(ctx, latestRun);

    return { run: latestRun, status };
  },
});
