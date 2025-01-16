import { v } from "convex/values";
import {
  internalAction,
  internalMutation,
  mutation,
  MutationCtx,
  query,
} from "./_generated/server";
import { FunctionHandle } from "convex/server";
import { Doc, Id } from "./_generated/dataModel";
import { internal } from "./_generated/api";
import { components } from "./_generated/api";
import { Crons } from "@convex-dev/crons";

// const crons = new Crons(components.crons);

// XXX think through all the potential contention that can happen, e.g., with cancel getting called

// XXX make this have a symmetric API with action retrier
export const enqueue = mutation({
  args: {
    fnHandle: v.string(),
    fnName: v.string(),
    fnArgs: v.any(),
    fnType: v.union(v.literal("action"), v.literal("mutation")),
    // XXX shouldn't have to pass this in each time
    workers: v.number(),
  },
  returns: v.id("jobs"),
  handler: async (ctx, { fnHandle, fnName, fnArgs, fnType, workers }) => {
    await ensurePoolExists(ctx, workers);
    console.log("adding job to pool", fnHandle, fnName, fnType, fnArgs);
    const workId = await ctx.db.insert("jobs", {
      fnHandle,
      fnName,
      fnType,
      fnArgs,
      state: "pending",
    });
    await ctx.scheduler.runAfter(0, internal.lib.mainLoop, {});
    return workId;
  },
});

export const cancel = mutation({
  args: {
    id: v.id("jobs"),
  },
  handler: async (ctx, { id }) => {
    const work = await ctx.db.get(id);
    if (!work) {
      throw new Error("work not found");
    }
    if (work.state === "pending") {
      console.log("canceling pending work", work._id);
      await ctx.db.patch(work._id, { state: "canceled" });
    } else if (work.state === "inProgress") {
      // XXX ensure it ends up as canceled
      if (!work.scheduledJob) {
        throw new Error("inProgress work has no scheduledJob");
      }
      console.log(
        "canceling inProgress work which will end up in state canceled after main wakes up",
        work._id
      );
      ctx.scheduler.cancel(work.scheduledJob);
    }
    await ctx.scheduler.runAfter(0, internal.lib.mainLoop, {});
  },
});

// There should only ever be at most one of these scheduled or running.
// The scheduled one is in the "mainLoop" table.
export const mainLoop = internalMutation({
  args: {},
  handler: async (ctx) => {
    // XXX ensure there is only one main loop
    const poolInfo = await ctx.db.query("pools").unique();
    if (!poolInfo) {
      throw new Error("no pool");
    }
    const numWorkers = poolInfo.workers;

    // Check if any inProgress jobs are done.
    const inProgress = await ctx.db
      .query("jobs")
      .withIndex("state", (q) => {
        return q.eq("state", "inProgress");
      })
      .collect();
    var numRunning = inProgress.length;
    await Promise.all(
      inProgress.map(async (work) => {
        if (!work.scheduledJob) {
          throw new Error("inProgress work has no scheduledJob");
        }
        const doc = await ctx.db.system.get(work.scheduledJob);
        if (!doc) {
          throw new Error("scheduled job not found");
        }
        if (doc.state.kind === "success") {
          numRunning--;
          console.log("work done", work._id);
          await ctx.db.patch(work._id, { state: "success" });
        } else if (doc.state.kind === "failed") {
          numRunning--;
          console.log("work failed", work._id);
          await ctx.db.patch(work._id, {
            state: "failed",
            error: doc.state.error,
          });
        } else if (doc.state.kind === "canceled") {
          numRunning--;
          console.log("work canceled", work._id);
          await ctx.db.patch(work._id, { state: "canceled" });
        }
      })
    );

    // Schedule more if we have the capacity.
    const toSchedule = Math.max(0, numWorkers - numRunning);
    const pending = await ctx.db
      .query("jobs")
      .withIndex("state", (q) => q.eq("state", "pending"))
      .take(toSchedule);
    await Promise.all(
      pending.map(async (work) => {
        await beginWork(ctx, work);
      })
    );
  },
});

async function beginWork(ctx: MutationCtx, work: Doc<"jobs">) {
  if (work.state !== "pending") {
    throw new Error(`Unexpected state ${work.state}`);
  }

  let scheduledJob: Id<"_scheduled_functions">;
  if (work.fnType === "action") {
    scheduledJob = await ctx.scheduler.runAfter(
      0,
      internal.lib.runActionWrapper,
      {
        fnHandle: work.fnHandle,
        fnArgs: work.fnArgs,
      }
    );
  } else {
    scheduledJob = await ctx.scheduler.runAfter(
      0,
      internal.lib.runMutationWrapper,
      {
        fnHandle: work.fnHandle,
        fnArgs: work.fnArgs,
      }
    );
  }
  await ctx.db.patch(work._id, {
    state: "inProgress",
    scheduledJob,
  });
}

// XXX
// async function checkInProgressWork(
//   ctx: MutationCtx,
//   doc: Doc<"inProgressWork">
//   // XXX change to a boolean or something
// ): Promise<{ finished: boolean; error?: string }> {
//   const workStatus = await ctx.db.system.get(doc.running);
//   if (workStatus === null) {
//     return { finished: true, error: "Timeout" };
//   } else if (
//     workStatus.state.kind === "pending" ||
//     workStatus.state.kind === "inProgress"
//   ) {
//     // XXX custom timeout per action/mutation
//     if (Date.now() - workStatus._creationTime > 15 * 60 * 1000) {
//       await ctx.scheduler.cancel(doc.running);
//       return { finished: true, error: "Timeout" };
//     }
//   } else if (workStatus.state.kind === "success") {
//     // Usually this would be handled by pendingCompletion, but for "unknown"
//     // functions, this is how we know that they're done, and we can't get their
//     // return values.
//     return { finished: true };
//   } else if (workStatus.state.kind === "canceled") {
//     return { finished: true, error: "Canceled" };
//   } else if (workStatus.state.kind === "failed") {
//     return { finished: true, error: workStatus.state.error };
//   }
//   return { finished: false };
// }

// XXX try to remove
export const runActionWrapper = internalAction({
  args: {
    fnHandle: v.string(),
    fnArgs: v.any(),
  },
  handler: async (ctx, { fnHandle: handleStr, fnArgs }) => {
    try {
      const fnHandle = handleStr as FunctionHandle<"action">;
      await ctx.runAction(fnHandle, fnArgs);
    } finally {
      // XXX this might not run so we need exponential backoff to check
      await ctx.scheduler.runAfter(0, internal.lib.mainLoop, {});
    }
  },
});

// Run a mutation synchronously and then trigger the state machine to wake up.
// Will propagate any errors raised by the mutation so that the status in the
// scheduler will be the same as the status of the job.
export const runMutationWrapper = internalMutation({
  args: {
    fnHandle: v.string(),
    fnArgs: v.any(),
  },
  handler: async (ctx, { fnHandle: handleStr, fnArgs }) => {
    try {
      const fnHandle = handleStr as FunctionHandle<"mutation">;
      await ctx.runMutation(fnHandle, fnArgs);
    } finally {
      await ctx.scheduler.runAfter(0, internal.lib.mainLoop, {});
    }
  },
});

// export const stopCleanup = mutation({
//   args: {},
//   handler: async (ctx) => {
//     const cron = await crons.get(ctx, { name: CLEANUP_CRON_NAME });
//     if (cron) {
//       await crons.delete(ctx, { id: cron.id });
//     }
//   },
// });

export const state = query({
  args: {
    id: v.id("jobs"),
  },
  returns: v.union(
    v.object({
      kind: v.literal("pending"),
    }),
    v.object({
      kind: v.literal("inProgress"),
    }),
    v.object({
      kind: v.literal("success"),
    }),
    v.object({
      kind: v.literal("failed"),
      error: v.string(),
    }),
    v.object({
      kind: v.literal("canceled"),
    })
  ),
  handler: async (
    ctx,
    { id }
  ): Promise<
    | { kind: "pending" }
    | { kind: "inProgress" }
    | { kind: "success" }
    | { kind: "failed"; error: string }
    | { kind: "canceled" }
  > => {
    const job = await ctx.db.get(id);
    if (!job) {
      throw new Error("Job not found");
    }
    switch (job.state) {
      case "pending":
        return { kind: "pending" };
      case "inProgress":
        return { kind: "inProgress" };
      case "success":
        return { kind: "success" };
      case "failed":
        if (!job.error) {
          throw new Error("Failed job missing error message");
        }
        // XXX replace with actual error
        // return { kind: "failed", error: job.error };
        return { kind: "failed", error: "testing" };
      case "canceled":
        return { kind: "canceled" };
      default:
        throw new Error(`Invalid job state: ${job.state}`);
    }
  },
});

// export const cleanup = mutation({
//   args: {},
//   handler: async (ctx) => {
//     const old = Date.now() - 24 * 60 * 60 * 1000;
//     const docs = await ctx.db
//       .query("completedWork")
//       .withIndex("by_creation_time", (q) => q.lt("_creationTime", old))
//       .collect();
//     await Promise.all(docs.map((doc) => ctx.db.delete(doc._id)));
//   },
// });

const MAX_POSSIBLE_PARALLELISM = 300;
// const CLEANUP_CRON_NAME = "cleanup";

async function ensurePoolExists(ctx: MutationCtx, workers: number) {
  if (workers > MAX_POSSIBLE_PARALLELISM) {
    throw new Error(`workers must be <= ${MAX_POSSIBLE_PARALLELISM}`);
  }
  if (workers < 1) {
    throw new Error("workers must be >= 1");
  }
  const pool = await ctx.db.query("pools").unique();
  if (pool) {
    if (pool.workers != workers) {
      await ctx.db.patch(pool._id, { workers });
    }
  }
  if (!pool) {
    await ctx.db.insert("pools", { workers });
  }
  // await ensureCleanupCron(ctx);
}

// async function ensureCleanupCron(ctx: MutationCtx) {
//   const cronFrequencyMs = 24 * 60 * 60 * 1000;
//   let cleanupCron = await crons.get(ctx, { name: CLEANUP_CRON_NAME });
//   if (
//     cleanupCron !== null &&
//     !(
//       cleanupCron.schedule.kind === "interval" &&
//       cleanupCron.schedule.ms === cronFrequencyMs
//     )
//   ) {
//     await crons.delete(ctx, { id: cleanupCron.id });
//     cleanupCron = null;
//   }
//   if (cleanupCron === null) {
//     await crons.register(
//       ctx,
//       { kind: "interval", ms: cronFrequencyMs },
//       api.lib.cleanup,
//       {},
//       CLEANUP_CRON_NAME
//     );
//   }
// }
