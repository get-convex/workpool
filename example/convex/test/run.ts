import {
  internalMutation,
  QueryCtx,
  internalQuery,
} from "../_generated/server";
import { v } from "convex/values";
import { components, internal } from "../_generated/api";
import { vWorkId } from "@convex-dev/workpool";
import { Id } from "../_generated/dataModel";
import { parse } from "convex-helpers/validators";

export async function runStatus(ctx: QueryCtx, runId: Id<"runs">) {
  const last = await ctx.db
    .query("tasks")
    .withIndex("runId_status_taskNum", (q) => q.eq("runId", runId))
    .order("desc")
    .first();
  return last?.status;
}

// Start a new test run
export default internalMutation({
  args: {
    scenario: v.union(v.literal("bigArgs")),
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

    const { maxParallelism } = parse(
      v.object({
        maxParallelism: v.optional(v.number()),
      }),
      args.parameters,
    );
    if (maxParallelism !== undefined) {
      await ctx.runMutation(components.dynamicWorkpool.config.update, {
        maxParallelism,
      });
    }

    // Create new run
    const runId = await ctx.db.insert("runs", {
      startTime: Date.now(),
      scenario: args.scenario,
      parameters: args.parameters,
    });
    await ctx.scheduler.runAfter(
      0,
      internal.test.scenarios[args.scenario].start,
      {
        runId,
        parameters: args.parameters,
      },
    );

    return runId;
  },
});

// Track a new task
export const trackTask = internalMutation({
  args: {
    runId: v.id("runs"),
    taskNum: v.number(),
    workId: vWorkId,
    type: v.union(v.literal("mutation"), v.literal("action")),
    hasOnComplete: v.boolean(),
  },
  handler: async (ctx, args) => {
    await ctx.db.insert("tasks", {
      runId: args.runId,
      taskNum: args.taskNum,
      workId: args.workId,
      type: args.type,
      status: "pending",
      hasOnComplete: args.hasOnComplete,
    });
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
