import { v } from "convex/values";
import {
  ActionCtx,
  internalAction,
  internalMutation,
  mutation,
  MutationCtx,
  query,
  QueryCtx,
} from "./_generated/server";
import { FunctionHandle, WithoutSystemFields } from "convex/server";
import { Doc, Id } from "./_generated/dataModel";
import { api, internal } from "./_generated/api";
import { createLogger, logLevel } from "./logging";
import { components } from "./_generated/api";
import { Crons } from "@convex-dev/crons";
import { recordCompleted, recordStarted } from "./stats";
import { completionStatus } from "./schema";

const crons = new Crons(components.crons);

const ACTION_TIMEOUT_MS = 15 * 60 * 1000;

export const enqueue = mutation({
  args: {
    fnHandle: v.string(),
    fnName: v.string(),
    fnArgs: v.any(),
    fnType: v.union(v.literal("action"), v.literal("mutation")),
    options: v.object({
      maxParallelism: v.number(),
      logLevel: v.optional(logLevel),
      statusTtl: v.optional(v.number()),
    }),
  },
  returns: v.id("work"),
  handler: async (ctx, { fnHandle, fnName, options, fnArgs, fnType }) => {
    await ensurePoolAndLoopExist(
      ctx,
      {
        maxParallelism: options.maxParallelism,
        statusTtl: options.statusTtl ?? 24 * 60 * 60 * 1000,
        logLevel: options.logLevel ?? "WARN",
      },
      "enqueue"
    );
    const workId = await ctx.db.insert("work", {
      fnHandle,
      fnName,
      fnArgs,
      fnType,
    });
    await ctx.db.insert("pendingStart", { workId });
    await kickMainLoop(ctx, "enqueue");
    return workId;
  },
});

export const cancel = mutation({
  args: {
    id: v.id("work"),
  },
  handler: async (ctx, { id }) => {
    await ctx.db.insert("pendingCancelation", { workId: id });
  },
});

async function console(ctx: QueryCtx | ActionCtx) {
  if ("runAction" in ctx) {
    return globalThis.console;
  }
  const pool = await ctx.db.query("pool").unique();
  if (!pool) {
    return globalThis.console;
  }
  return createLogger(pool.logLevel);
}

const BATCH_SIZE = 10;

// There should only ever be at most one of these scheduled or running.
// The scheduled one is in the "mainLoop" table.
export const mainLoop = internalMutation({
  args: {
    generation: v.number(),
  },
  handler: async (ctx, args) => {
    const console_ = await console(ctx);

    const options = (await ctx.db.query("pool").unique())!;
    if (!options) {
      throw new Error("no pool in mainLoop");
    }
    const { maxParallelism } = options;
    let didSomething = false;

    let inProgressCountChange = 0;

    // Move from pendingCompletion to completedWork, deleting from inProgressWork.
    // Generation is used to avoid OCCs with work completing.
    console_.time("[mainLoop] pendingCompletion");
    const generation = await ctx.db.query("completionGeneration").unique();
    const generationNumber = generation?.generation ?? 0;
    if (generationNumber !== args.generation) {
      throw new Error(
        `generation mismatch: ${generationNumber} !== ${args.generation}`
      );
    }
    // Collect all pending completions for the previous generation.
    // This won't be too many because the jobs all correspond to being scheduled
    // by a single mainLoop (the previous one), so they're limited by MAX_PARALLELISM.
    const completed = await ctx.db
      .query("pendingCompletion")
      .withIndex("generation", (q) => q.eq("generation", generationNumber - 1))
      .collect();
    console_.debug(`[mainLoop] completing ${completed.length}`);
    await Promise.all(
      completed.map(async (pendingCompletion) => {
        const inProgressWork = await ctx.db
          .query("inProgressWork")
          .withIndex("workId", (q) => q.eq("workId", pendingCompletion.workId))
          .unique();
        if (inProgressWork) {
          await ctx.db.delete(inProgressWork._id);
          inProgressCountChange--;
          await ctx.db.insert("completedWork", {
            completionStatus: pendingCompletion.completionStatus,
            workId: pendingCompletion.workId,
          });
          const work = (await ctx.db.get(pendingCompletion.workId))!;
          console_.info(
            recordCompleted(work, pendingCompletion.completionStatus)
          );
          await ctx.db.delete(work._id);
        }
        await ctx.db.delete(pendingCompletion._id);
        didSomething = true;
      })
    );
    console_.timeEnd("[mainLoop] pendingCompletion");

    console_.time("[mainLoop] inProgress count");
    // This is the only function reading and writing inProgressWork,
    // and it's bounded by MAX_POSSIBLE_PARALLELISM, so we can
    // read it all into memory. BUT we don't have to -- we can just read
    // the count from the inProgressCount table.
    const inProgressCount = await ctx.db.query("inProgressCount").unique();
    const inProgressBefore =
      (inProgressCount?.count ?? 0) + inProgressCountChange;
    console_.debug(`[mainLoop] ${inProgressBefore} in progress`);
    console_.timeEnd("[mainLoop] inProgress count");

    // Move from pendingStart to inProgressWork.
    console_.time("[mainLoop] pendingStart");

    // Start reading from the latest cursor _creationTime, which allows us to
    // skip over tombstones of pendingStart documents which haven't been cleaned up yet.
    // WARNING: this might skip over pendingStart documents if their _creationTime
    // was assigned out of order. We handle that below.
    const pendingStartCursorDoc = await ctx.db
      .query("pendingStartCursor")
      .unique();
    const pendingStartCursor = pendingStartCursorDoc?.cursor ?? 0;

    // Schedule as many as needed to reach maxParallelism.
    const toSchedule = maxParallelism - inProgressBefore;

    const pending = await ctx.db
      .query("pendingStart")
      .withIndex("by_creation_time", (q) =>
        q.gt("_creationTime", pendingStartCursor)
      )
      .take(toSchedule);
    console_.debug(`[mainLoop] scheduling ${pending.length} pending work`);
    await Promise.all(
      pending.map(async (pendingWork) => {
        const { scheduledId, timeoutMs } = await beginWork(ctx, pendingWork);
        await ctx.db.insert("inProgressWork", {
          running: scheduledId,
          timeoutMs,
          workId: pendingWork.workId,
        });
        inProgressCountChange++;
        await ctx.db.delete(pendingWork._id);
        didSomething = true;
      })
    );
    const newPendingStartCursor =
      pending.length > 0
        ? pending[pending.length - 1]._creationTime
        : pendingStartCursor;
    if (!pendingStartCursorDoc) {
      await ctx.db.insert("pendingStartCursor", {
        cursor: newPendingStartCursor,
      });
    } else {
      await ctx.db.patch(pendingStartCursorDoc._id, {
        cursor: newPendingStartCursor,
      });
    }
    console_.timeEnd("[mainLoop] pendingStart");

    console_.time("[mainLoop] pendingCancelation");
    const canceled = await ctx.db.query("pendingCancelation").take(BATCH_SIZE);
    console_.debug(`[mainLoop] canceling ${canceled.length}`);
    await Promise.all(
      canceled.map(async (pendingCancelation) => {
        const inProgressWork = await ctx.db
          .query("inProgressWork")
          .withIndex("workId", (q) => q.eq("workId", pendingCancelation.workId))
          .unique();
        if (inProgressWork) {
          await ctx.scheduler.cancel(inProgressWork.running);
          await ctx.db.delete(inProgressWork._id);
          inProgressCountChange--;
          await ctx.db.insert("completedWork", {
            workId: pendingCancelation.workId,
            completionStatus: "canceled",
          });
          const work = (await ctx.db.get(pendingCancelation.workId))!;
          console_.info(recordCompleted(work, "canceled"));
          await ctx.db.delete(work._id);
        }
        await ctx.db.delete(pendingCancelation._id);
        didSomething = true;
      })
    );
    console_.timeEnd("[mainLoop] pendingCancelation");

    // In case there are more pending completions at higher generation numbers,
    // there's more to do.
    if (!didSomething) {
      const nextPendingCompletion = await ctx.db
        .query("pendingCompletion")
        .withIndex("generation", (q) => q.eq("generation", generationNumber))
        .first();
      didSomething = nextPendingCompletion !== null;
    }

    // In case there are more "pendingStart" items we missed due to out-of-order
    // _creationTime, we need to check for them. Do that here, when we're
    // otherwise idle so it doesn't matter if this function takes a while walking
    // tombstones.
    if (!didSomething && pendingStartCursorDoc) {
      const pendingStartDoc = await ctx.db
        .query("pendingStart")
        .order("desc")
        .first();
      if (pendingStartDoc) {
        console_.warn(`[mainLoop] missed pendingStart docs; discarding cursor`);
        await ctx.db.delete(pendingStartCursorDoc._id);
        didSomething = true;
      }
    }

    if (!didSomething) {
      console_.time("[mainLoop] inProgressWork check for unclean exits");
      // If all completions are handled, check everything in inProgressWork.
      // This will find everything that timed out, failed ungracefully, was
      // cancelled, or succeeded without a return value.
      const inProgress = await ctx.db.query("inProgressWork").collect();
      await Promise.all(
        inProgress.map(async (inProgressWork) => {
          const result = await checkInProgressWork(ctx, inProgressWork);
          if (result !== null) {
            console_.warn(
              "[mainLoop] inProgressWork finished uncleanly",
              inProgressWork.workId,
              result
            );
            inProgressCountChange--;
            await ctx.db.delete(inProgressWork._id);
            await ctx.db.insert("completedWork", {
              workId: inProgressWork.workId,
              completionStatus: result.completionStatus,
            });
            const work = (await ctx.db.get(inProgressWork.workId))!;
            console_.info(recordCompleted(work, result.completionStatus));
            didSomething = true;
          }
        })
      );
      console_.timeEnd("[mainLoop] inProgressWork check for unclean exits");
    }

    if (inProgressCountChange !== 0) {
      if (inProgressCount) {
        await ctx.db.patch(inProgressCount._id, {
          count: inProgressCount.count + inProgressCountChange,
        });
      } else {
        await ctx.db.insert("inProgressCount", {
          count: inProgressCountChange,
        });
      }
    }

    console_.time("[mainLoop] kickMainLoop");
    if (didSomething) {
      // There might be more to do.
      await loopFromMainLoop(ctx, 0);
    } else {
      // Decide when to wake up.
      const allInProgressWork = await ctx.db.query("inProgressWork").collect();
      const nextPending = await ctx.db.query("pendingStart").first();
      const nextPendingTime = nextPending
        ? nextPending._creationTime
        : Number.POSITIVE_INFINITY;
      const nextInProgress = allInProgressWork.length
        ? Math.min(
            ...allInProgressWork
              .filter((w) => w.timeoutMs !== null)
              .map((w) => w._creationTime + w.timeoutMs!)
          )
        : Number.POSITIVE_INFINITY;
      const nextTime = Math.min(nextPendingTime, nextInProgress);
      await loopFromMainLoop(ctx, nextTime - Date.now());
    }
    console_.timeEnd("[mainLoop] kickMainLoop");
  },
});

async function beginWork(
  ctx: MutationCtx,
  pendingStart: Doc<"pendingStart">
): Promise<{
  scheduledId: Id<"_scheduled_functions">;
  timeoutMs: number | null;
}> {
  const console_ = await console(ctx);
  const work = await ctx.db.get(pendingStart.workId);
  if (!work) {
    throw new Error("work not found");
  }
  console_.info(recordStarted(work));
  if (work.fnType === "action") {
    return {
      scheduledId: await ctx.scheduler.runAfter(
        0,
        internal.lib.runActionWrapper,
        {
          workId: work._id,
          fnHandle: work.fnHandle,
          fnArgs: work.fnArgs,
        }
      ),
      timeoutMs: ACTION_TIMEOUT_MS,
    };
  } else if (work.fnType === "mutation") {
    return {
      scheduledId: await ctx.scheduler.runAfter(
        0,
        internal.lib.runMutationWrapper,
        {
          workId: work._id,
          fnHandle: work.fnHandle,
          fnArgs: work.fnArgs,
        }
      ),
      timeoutMs: null, // Mutations cannot timeout
    };
  } else {
    throw new Error(`Unexpected fnType ${work.fnType}`);
  }
}

async function checkInProgressWork(
  ctx: MutationCtx,
  doc: Doc<"inProgressWork">
): Promise<{
  completionStatus: "success" | "error" | "canceled" | "timeout";
} | null> {
  const workStatus = await ctx.db.system.get(doc.running);
  if (workStatus === null) {
    return { completionStatus: "timeout" };
  } else if (
    workStatus.state.kind === "pending" ||
    workStatus.state.kind === "inProgress"
  ) {
    if (
      doc.timeoutMs !== null &&
      Date.now() - workStatus._creationTime > doc.timeoutMs
    ) {
      await ctx.scheduler.cancel(doc.running);
      return { completionStatus: "timeout" };
    }
  } else if (workStatus.state.kind === "success") {
    // Usually this would be handled by pendingCompletion, but for "unknown"
    // functions, this is how we know that they're done, and we can't get their
    // return values.
    return { completionStatus: "success" };
  } else if (workStatus.state.kind === "canceled") {
    return { completionStatus: "canceled" };
  } else if (workStatus.state.kind === "failed") {
    return { completionStatus: "error" };
  }
  return null;
}

export const runActionWrapper = internalAction({
  args: {
    workId: v.id("work"),
    fnHandle: v.string(),
    fnArgs: v.any(),
  },
  handler: async (ctx, { workId, fnHandle: handleStr, fnArgs }) => {
    const console_ = await console(ctx);
    const fnHandle = handleStr as FunctionHandle<"action">;
    try {
      await ctx.runAction(fnHandle, fnArgs);
      // NOTE: we could run `ctx.runMutation`, but we want to guarantee execution,
      // and `ctx.scheduler.runAfter` won't OCC.
      await ctx.scheduler.runAfter(0, internal.lib.saveResult, {
        workId,
        completionStatus: "success",
      });
    } catch (e: unknown) {
      console_.error(e);
      await ctx.scheduler.runAfter(0, internal.lib.saveResult, {
        workId,
        completionStatus: "error",
      });
    }
  },
});

export const saveResult = internalMutation({
  args: {
    workId: v.id("work"),
    completionStatus,
  },
  handler: async (ctx, args) => {
    const currentGeneration = await ctx.db
      .query("completionGeneration")
      .unique();
    const generation = currentGeneration?.generation ?? 0;
    await ctx.db.insert("pendingCompletion", {
      completionStatus: args.completionStatus,
      workId: args.workId,
      generation,
    });
    await kickMainLoop(ctx, "saveResult");
  },
});

export const bumpGeneration = internalMutation({
  args: {},
  handler: async (ctx) => {
    const currentGeneration = await ctx.db
      .query("completionGeneration")
      .unique();
    const generation = (currentGeneration?.generation ?? 0) + 1;
    if (!currentGeneration) {
      await ctx.db.insert("completionGeneration", { generation });
    } else {
      await ctx.db.patch(currentGeneration._id, { generation });
    }
    await ctx.scheduler.runAfter(0, internal.lib.mainLoop, { generation });
  },
});

export const runMutationWrapper = internalMutation({
  args: {
    workId: v.id("work"),
    fnHandle: v.string(),
    fnArgs: v.any(),
  },
  handler: async (ctx, { workId, fnHandle: handleStr, fnArgs }) => {
    const console_ = await console(ctx);
    const fnHandle = handleStr as FunctionHandle<"mutation">;
    try {
      await ctx.runMutation(fnHandle, fnArgs);
      // NOTE: we could run the `saveResult` handler here, or call `ctx.runMutation`,
      // but we want the mutation to be a separate transaction to reduce the window for OCCs.
      await ctx.scheduler.runAfter(0, internal.lib.saveResult, {
        workId,
        completionStatus: "success",
      });
    } catch (e: unknown) {
      console_.error(e);
      await ctx.scheduler.runAfter(0, internal.lib.saveResult, {
        workId,
        completionStatus: "error",
      });
    }
  },
});

async function getMainLoop(ctx: QueryCtx) {
  const mainLoop = await ctx.db.query("mainLoop").unique();
  if (!mainLoop) {
    throw new Error("mainLoop doesn't exist");
  }
  return mainLoop;
}

export const stopCleanup = mutation({
  args: {},
  handler: async (ctx) => {
    const cron = await crons.get(ctx, { name: CLEANUP_CRON_NAME });
    if (cron) {
      await crons.delete(ctx, { id: cron.id });
    }
  },
});

async function loopFromMainLoop(ctx: MutationCtx, delayMs: number) {
  const console_ = await console(ctx);
  const mainLoop = await getMainLoop(ctx);
  if (mainLoop.state.kind === "idle") {
    throw new Error("mainLoop is idle but `loopFromMainLoop` was called");
  }
  if (delayMs <= 0) {
    console_.debug(
      "[mainLoop] mainLoop is actively running and wants to keep running"
    );
    await ctx.scheduler.runAfter(0, internal.lib.bumpGeneration, {});
    if (mainLoop.state.kind !== "running") {
      await ctx.db.patch(mainLoop._id, { state: { kind: "running" } });
    }
  } else if (delayMs < Number.POSITIVE_INFINITY) {
    console_.debug(`[mainLoop] mainLoop wants to run after ${delayMs}ms`);
    const runAtTime = Date.now() + delayMs;
    const fn = await ctx.scheduler.runAt(
      runAtTime,
      internal.lib.bumpGeneration,
      {}
    );
    await ctx.db.patch(mainLoop._id, {
      state: {
        kind: "scheduled",
        fn,
        runAtTime,
      },
    });
  } else {
    console_.debug("[mainLoop] mainLoop wants to become idle");
    await ctx.db.patch(mainLoop._id, { state: { kind: "idle" } });
  }
}

async function kickMainLoop(
  ctx: MutationCtx,
  source: "saveResult" | "enqueue"
): Promise<void> {
  const console_ = await console(ctx);

  // Look for mainLoop documents that we want to reschedule.
  // Only kick to run now if we're scheduled or idle.
  const mainLoop = await getMainLoop(ctx);
  if (mainLoop.state.kind === "running") {
    console_.debug(
      `[${source}] mainLoop is actively running, so we don't need to do anything`
    );
    return;
  }
  // mainLoop is scheduled to run later, so we should cancel it and reschedule.
  if (mainLoop.state.kind === "scheduled") {
    await ctx.scheduler.cancel(mainLoop.state.fn);
  }
  const currentGeneration = await ctx.db.query("completionGeneration").unique();
  const generation = currentGeneration?.generation ?? 0;
  await ctx.scheduler.runAfter(0, internal.lib.mainLoop, { generation });
  console_.debug(
    `[${source}] mainLoop was scheduled later, so reschedule it to run now`
  );
  await ctx.db.patch(mainLoop._id, { state: { kind: "running" } });
}

export const status = query({
  args: {
    id: v.id("work"),
  },
  returns: v.union(
    v.object({
      kind: v.literal("pending"),
    }),
    v.object({
      kind: v.literal("inProgress"),
    }),
    v.object({
      kind: v.literal("completed"),
      completionStatus,
    })
  ),
  handler: async (ctx, { id }) => {
    const completedWork = await ctx.db
      .query("completedWork")
      .withIndex("workId", (q) => q.eq("workId", id))
      .unique();
    if (completedWork) {
      return {
        kind: "completed",
        completionStatus: completedWork.completionStatus,
      } as const;
    }
    const pendingStart = await ctx.db
      .query("pendingStart")
      .withIndex("workId", (q) => q.eq("workId", id))
      .unique();
    if (pendingStart) {
      return { kind: "pending" } as const;
    }
    // If it's not pending or completed, it must be in progress.
    // Note we do not check inProgressWork, because we don't want to intersect
    // mainLoop.
    return { kind: "inProgress" } as const;
  },
});

export const MAX_CLEANUP_DOCS = 1000;

export const cleanup = mutation({
  args: {
    maxAgeMs: v.number(),
  },
  handler: async (ctx, { maxAgeMs }) => {
    const old = Date.now() - maxAgeMs;
    const docs = await ctx.db
      .query("completedWork")
      .withIndex("by_creation_time", (q) => q.lte("_creationTime", old))
      .order("desc")
      .take(MAX_CLEANUP_DOCS);
    await Promise.all(
      docs.map(async (doc) => {
        await ctx.db.delete(doc._id);
        const work = await ctx.db.get(doc.workId);
        if (work) {
          await ctx.db.delete(work._id);
        }
      })
    );
    if (docs.length === MAX_CLEANUP_DOCS) {
      // Schedule the next cleanup to run starting from the oldest document.
      await ctx.scheduler.runAfter(0, api.lib.cleanup, {
        maxAgeMs: docs[docs.length - 1]._creationTime,
      });
    }
  },
});

const MAX_POSSIBLE_PARALLELISM = 300;
const CLEANUP_CRON_NAME = "cleanup";

async function ensurePoolAndLoopExist(
  ctx: MutationCtx,
  opts: WithoutSystemFields<Doc<"pool">>,
  source: "enqueue" | "saveResult" | "mainLoop"
) {
  if (opts.maxParallelism > MAX_POSSIBLE_PARALLELISM) {
    throw new Error(`maxParallelism must be <= ${MAX_POSSIBLE_PARALLELISM}`);
  }
  if (opts.maxParallelism < 1) {
    throw new Error("maxParallelism must be >= 1");
  }
  const pool = await ctx.db.query("pool").unique();
  if (pool) {
    let update = false;
    for (const key in opts) {
      if (pool[key as keyof typeof opts] !== opts[key as keyof typeof opts]) {
        update = true;
      }
    }
    if (update) {
      await ctx.db.patch(pool._id, opts);
    }
  } else {
    const console_ = await console(ctx);
    await ctx.db.insert("pool", opts);
    console_.debug(`[${source}] starting mainLoop`);
    const exists = await ctx.db.query("mainLoop").unique();
    if (exists) {
      throw new Error("mainLoop already exists");
    }
    await ctx.db.insert("mainLoop", { state: { kind: "running" } });
    const currentGeneration = await ctx.db
      .query("completionGeneration")
      .unique();
    if (currentGeneration) {
      throw new Error("completionGeneration already exists");
    }
    await ctx.db.insert("completionGeneration", { generation: 0 });
    await ctx.scheduler.runAfter(0, internal.lib.mainLoop, { generation: 0 });
  }
  await ensureCleanupCron(ctx, opts.statusTtl);
}

async function ensureCleanupCron(ctx: MutationCtx, ttl: number) {
  let cleanupCron = await crons.get(ctx, { name: CLEANUP_CRON_NAME });
  if (ttl === Number.POSITIVE_INFINITY) {
    if (cleanupCron) {
      await crons.delete(ctx, { id: cleanupCron.id });
    }
    return;
  }
  const cronFrequencyMs = Math.min(ttl, 24 * 60 * 60 * 1000);
  if (
    cleanupCron !== null &&
    !(
      cleanupCron.schedule.kind === "interval" &&
      cleanupCron.schedule.ms === cronFrequencyMs
    )
  ) {
    await crons.delete(ctx, { id: cleanupCron.id });
    cleanupCron = null;
  }
  if (cleanupCron === null) {
    await crons.register(
      ctx,
      { kind: "interval", ms: ttl },
      api.lib.cleanup,
      { maxAgeMs: ttl },
      CLEANUP_CRON_NAME
    );
  }
}
