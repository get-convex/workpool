import { v } from "convex/values";
import {
  ActionCtx,
  DatabaseReader,
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

export const ACTION_TIMEOUT_MS = 15 * 60 * 1000; // 15 minutes

export const enqueue = mutation({
  args: {
    fnHandle: v.string(),
    fnName: v.string(),
    fnArgs: v.any(),
    fnType: v.union(v.literal("action"), v.literal("mutation")),
    options: v.object({
      maxParallelism: v.number(),
      logLevel: v.optional(logLevel),
      ttl: v.optional(v.number()),
    }),
  },
  returns: v.id("work"),
  handler: async (ctx, { fnHandle, fnName, options, fnArgs, fnType }) => {
    await ensurePoolExists(ctx, {
      maxParallelism: options.maxParallelism,
      ttl: options.ttl ?? 24 * 60 * 60 * 1000,
      logLevel: options.logLevel ?? "WARN",
    });
    const workId = await ctx.db.insert("work", {
      fnHandle,
      fnName,
      fnArgs,
      fnType,
    });
    await ctx.db.insert("pendingStart", { workId });
    await kickMainLoop(ctx);
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

async function getOptions(db: DatabaseReader) {
  return db.query("pools").unique();
}

async function console(ctx: QueryCtx | ActionCtx) {
  if ("runAction" in ctx) {
    return globalThis.console;
  }
  const pool = await ctx.db.query("pools").unique();
  if (!pool) {
    return globalThis.console;
  }
  return createLogger(pool.logLevel);
}

const BATCH_SIZE = 10;

// There should only ever be at most one of these scheduled or running.
// The scheduled one is in the "mainLoop" table.
export const mainLoop = internalMutation({
  args: {},
  handler: async (ctx, _args) => {
    const console_ = await console(ctx);

    const options = await getOptions(ctx.db);
    if (!options) {
      console_.info("no pool, skipping mainLoop");
      return;
    }
    const { maxParallelism } = options;

    console_.time("inProgress count");
    // This is the only function reading and writing inProgressWork,
    // and it's bounded by MAX_POSSIBLE_PARALLELISM, so we can
    // read it all into memory.
    const inProgressBefore = await ctx.db.query("inProgressWork").collect();
    console_.debug(`${inProgressBefore.length} in progress`);
    console_.timeEnd("inProgress count");

    // Move from pendingWork to inProgressWork.
    console_.time("pendingWork");
    const toSchedule = Math.min(
      maxParallelism - inProgressBefore.length,
      BATCH_SIZE
    );
    let didSomething = false;
    const pending = await ctx.db.query("pendingStart").take(toSchedule);
    console_.debug(`scheduling ${pending.length} pending work`);
    await Promise.all(
      pending.map(async (work) => {
        const { scheduledId, timeoutMs } = await beginWork(ctx, work);
        await ctx.db.insert("inProgressWork", {
          running: scheduledId,
          timeoutMs,
          workId: work.workId,
        });
        await ctx.db.delete(work._id);
        didSomething = true;
      })
    );
    console_.timeEnd("pendingWork");

    // Move from pendingCompletion to completedWork, deleting from inProgressWork.
    // We could do all of these, but we don't want to OCC with work completing,
    // so we only do a few at a time.
    console_.time("pendingCompletion");
    const completed = await ctx.db.query("pendingCompletion").take(BATCH_SIZE);
    console_.debug(`completing ${completed.length}`);
    await Promise.all(
      completed.map(async (work) => {
        const inProgressWork = await ctx.db
          .query("inProgressWork")
          .withIndex("workId", (q) => q.eq("workId", work.workId))
          .unique();
        if (inProgressWork) {
          await ctx.db.delete(inProgressWork._id);
        }
        await ctx.db.delete(work._id);
        await ctx.db.insert("completedWork", {
          completionStatus: work.completionStatus,
          workId: work.workId,
        });
        recordCompleted(work.workId, work.completionStatus);
        didSomething = true;
      })
    );
    console_.timeEnd("pendingCompletion");

    console_.time("pendingCancelation");
    const canceled = await ctx.db.query("pendingCancelation").take(BATCH_SIZE);
    console_.debug(`canceling ${canceled.length}`);
    await Promise.all(
      canceled.map(async (work) => {
        const inProgressWork = await ctx.db
          .query("inProgressWork")
          .withIndex("workId", (q) => q.eq("workId", work.workId))
          .unique();
        if (inProgressWork) {
          await ctx.scheduler.cancel(inProgressWork.running);
          await ctx.db.delete(inProgressWork._id);
          await ctx.db.insert("completedWork", {
            workId: work.workId,
            completionStatus: "canceled",
          });
          recordCompleted(work.workId, "canceled");
        }
        await ctx.db.delete(work._id);
        didSomething = true;
      })
    );
    console_.timeEnd("pendingCancelation");

    if (completed.length === 0) {
      console_.time("inProgressWork check for unclean exits");
      // If all completions are handled, check everything in inProgressWork.
      // This will find everything that timed out, failed ungracefully, was
      // cancelled, or succeeded without a return value.
      const inProgress = await ctx.db.query("inProgressWork").collect();
      await Promise.all(
        inProgress.map(async (work) => {
          const result = await checkInProgressWork(ctx, work);
          if (result !== null) {
            console_.debug(
              "inProgressWork finished uncleanly",
              work.workId,
              result
            );
            await ctx.db.delete(work._id);
            await ctx.db.insert("completedWork", {
              workId: work.workId,
              completionStatus: result.completionStatus,
            });
            recordCompleted(work.workId, result.completionStatus);
            didSomething = true;
          }
        })
      );
      console_.timeEnd("inProgressWork check for unclean exits");
    }

    console_.time("kickMainLoop");
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
    console_.timeEnd("kickMainLoop");
  },
});

async function beginWork(
  ctx: MutationCtx,
  pendingStart: Doc<"pendingStart">
): Promise<{
  scheduledId: Id<"_scheduled_functions">;
  timeoutMs: number | null;
}> {
  const options = await getOptions(ctx.db);
  if (!options) {
    throw new Error("cannot begin work with no pool");
  }
  const work = await ctx.db.get(pendingStart.workId);
  if (!work) {
    throw new Error("work not found");
  }
  recordStarted(work._id, work.fnName, work._creationTime);
  // Use ACTION_TIMEOUT_MS constant for timeout
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
      await ctx.runMutation(internal.lib.saveResult, {
        workId,
        completionStatus: "success",
      });
    } catch (e: unknown) {
      console_.error(e);
      await ctx.runMutation(internal.lib.saveResult, {
        workId,
        completionStatus: "error",
      });
    }
  },
});

export const saveResult = internalMutation({
  args: {
    workId: v.id("work"),
    result: v.optional(v.any()),
    error: v.optional(v.string()),
  },
  handler: saveResultHandler,
});

async function saveResultHandler(
  ctx: MutationCtx,
  {
    workId,
    completionStatus,
  }: {
    workId: Id<"work">;
    completionStatus: "success" | "error" | "canceled" | "timeout";
  }
): Promise<void> {
  const options = await getOptions(ctx.db);
  if (!options) {
    throw new Error("cannot save result with no pool");
  }
  await ctx.db.insert("pendingCompletion", {
    completionStatus,
    workId,
  });
  await kickMainLoop(ctx);
}

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
      await saveResultHandler(ctx, { workId, completionStatus: "success" });
    } catch (e: unknown) {
      console_.error(e);
      await saveResultHandler(ctx, { workId, completionStatus: "error" });
    }
  },
});

async function startMainLoopHandler(ctx: MutationCtx) {
  const mainLoop = await ctx.db.query("mainLoop").unique();
  const console_ = await console(ctx);
  if (!mainLoop) {
    console_.debug("starting mainLoop");
    await ctx.scheduler.runAfter(0, internal.lib.mainLoop, {});
    await ctx.db.insert("mainLoop", {
      state: { kind: "running" },
    });
    return;
  }
  if (mainLoop.state.kind === "running") {
    console_.info(
      "mainLoop should be actively running; if it's not, run `mainLoop` directly"
    );
    return;
  }
  console_.debug("mainLoop is scheduled to run later, so run it now");
  if (mainLoop.state.kind === "scheduled") {
    await ctx.scheduler.cancel(mainLoop.state.fn);
  }
  await ctx.db.patch(mainLoop._id, { state: { kind: "running" } });
  await ctx.scheduler.runAfter(0, internal.lib.mainLoop, {});
}

export const startMainLoop = mutation({
  args: {},
  handler: startMainLoopHandler,
});

export const stopCleanup = mutation({
  args: {},
  handler: async (ctx) => {
    const cron = await crons.get(ctx, { name: CLEANUP_CRON_NAME });
    if (cron) {
      await crons.delete(ctx, { id: cron.id });
    }
  },
});

export const stopMainLoop = mutation({
  args: {},
  handler: async (ctx) => {
    const mainLoop = await ctx.db.query("mainLoop").unique();
    if (!mainLoop) {
      return;
    }
    if (mainLoop.state.kind === "scheduled") {
      await ctx.scheduler.cancel(mainLoop.state.fn);
    }
    await ctx.db.patch(mainLoop._id, { state: { kind: "idle" } });
  },
});

async function loopFromMainLoop(ctx: MutationCtx, delayMs: number) {
  const console_ = await console(ctx);
  const mainLoop = await ctx.db.query("mainLoop").unique();
  if (mainLoop === null) {
    console_.debug("mainLoop not found, so we need to start it");
    await startMainLoopHandler(ctx);
    return;
  }
  if (mainLoop.state.kind === "idle") {
    throw new Error("mainLoop is idle but `loopFromMainLoop` was called");
  }
  if (delayMs <= 0) {
    console_.debug("mainLoop is actively running and wants to keep running");
    await ctx.scheduler.runAfter(0, internal.lib.mainLoop, {});
    if (mainLoop.state.kind !== "running") {
      await ctx.db.patch(mainLoop._id, { state: { kind: "running" } });
    }
  } else if (delayMs < Number.POSITIVE_INFINITY) {
    console_.debug(`mainLoop wants to run after ${delayMs}ms`);
    const runAtTime = Date.now() + delayMs;
    const fn = await ctx.scheduler.runAt(runAtTime, internal.lib.mainLoop, {});
    await ctx.db.patch(mainLoop._id, {
      state: { kind: "scheduled", fn, runAtTime },
    });
  } else {
    console_.debug("mainLoop wants to become idle");
    await ctx.db.patch(mainLoop._id, { state: { kind: "idle" } });
  }
}

async function kickMainLoop(ctx: MutationCtx): Promise<void> {
  const console_ = await console(ctx);

  // Look for mainLoop documents that we want to reschedule.
  // Only kick to run now if we're scheduled or idle.
  const mainLoop = await ctx.db.query("mainLoop").unique();
  if (!mainLoop) {
    console_.debug("mainLoop doesn't exist, so we need to start it");
    await startMainLoopHandler(ctx);
    return;
  }
  if (mainLoop.state.kind === "running") {
    console_.debug(
      "mainLoop is actively running, so we don't need to do anything"
    );
    return;
  }
  // mainLoop is scheduled to run later, so we should cancel it and reschedule.
  if (mainLoop.state.kind === "scheduled") {
    await ctx.scheduler.cancel(mainLoop.state.fn);
  }
  await ctx.scheduler.runAfter(0, internal.lib.mainLoop, {});
  console_.debug("mainLoop was scheduled later, so reschedule it to run now");
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

export const cleanup = mutation({
  args: {
    maxAgeMs: v.number(),
  },
  handler: async (ctx, { maxAgeMs }) => {
    const old = Date.now() - maxAgeMs;
    const docs = await ctx.db
      .query("completedWork")
      .withIndex("by_creation_time", (q) => q.lt("_creationTime", old))
      .collect();
    await Promise.all(docs.map((doc) => ctx.db.delete(doc._id)));
  },
});

const MAX_POSSIBLE_PARALLELISM = 300;
const CLEANUP_CRON_NAME = "cleanup";

async function ensurePoolExists(
  ctx: MutationCtx,
  opts: WithoutSystemFields<Doc<"pools">>
) {
  if (opts.maxParallelism > MAX_POSSIBLE_PARALLELISM) {
    throw new Error(`maxParallelism must be <= ${MAX_POSSIBLE_PARALLELISM}`);
  }
  if (opts.maxParallelism < 1) {
    throw new Error("maxParallelism must be >= 1");
  }
  const pool = await ctx.db.query("pools").unique();
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
  }
  if (!pool) {
    await ctx.db.insert("pools", opts);
    await startMainLoopHandler(ctx);
  }
  await ensureCleanupCron(ctx, opts.ttl);
}

async function ensureCleanupCron(ctx: MutationCtx, ttl: number) {
  if (ttl === Number.POSITIVE_INFINITY) {
    (await console(ctx)).info(
      "completedWorkMaxAgeMs is Infinity, so we won't schedule cleanup"
    );
    return;
  }
  const cronFrequencyMs = Math.min(ttl, 24 * 60 * 60 * 1000);
  let cleanupCron = await crons.get(ctx, { name: CLEANUP_CRON_NAME });
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
