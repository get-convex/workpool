import { v } from "convex/values";
import { DatabaseReader, internalAction, internalMutation, mutation, MutationCtx, query, QueryCtx } from "./_generated/server";
import { FunctionHandle } from "convex/server";
import { Doc, Id } from "./_generated/dataModel";
import { api, internal } from "./_generated/api";
import { createLogger, logLevel, LogLevel } from "./logging";
import { components } from "./_generated/api";
import { Crons } from "@convex-dev/crons";

const crons = new Crons(components.crons);

export const enqueue = mutation({
  args: {
    handle: v.string(),
    options: v.object({
      maxParallelism: v.number(),
      actionTimeoutMs: v.optional(v.number()),
      mutationTimeoutMs: v.optional(v.number()),
      unknownTimeoutMs: v.optional(v.number()),
      debounceMs: v.optional(v.number()),
      fastHeartbeatMs: v.optional(v.number()),
      slowHeartbeatMs: v.optional(v.number()),
      logLevel: v.optional(logLevel),
      completedWorkMaxAgeMs: v.optional(v.number()),
    }),
    fnArgs: v.any(),
    fnType: v.union(v.literal("action"), v.literal("mutation"), v.literal("unknown")),
    runAtTime: v.number(),
  },
  returns: v.id("pendingWork"),
  handler: async (ctx, { handle, options, fnArgs, fnType, runAtTime }) => {
    const debounceMs = options.debounceMs ?? 50;
    await ensurePoolExists(
      ctx,
      {
        maxParallelism: options.maxParallelism,
        actionTimeoutMs: options.actionTimeoutMs ?? 15 * 60 * 1000,
        mutationTimeoutMs: options.mutationTimeoutMs ?? 30 * 1000,
        unknownTimeoutMs: options.unknownTimeoutMs ?? 15 * 60 * 1000,
        debounceMs,
        fastHeartbeatMs: options.fastHeartbeatMs ?? 10 * 1000,
        slowHeartbeatMs: options.slowHeartbeatMs ?? 2 * 60 * 60 * 1000,
        completedWorkMaxAgeMs: options.completedWorkMaxAgeMs ?? 24 * 60 * 60 * 1000,
        logLevel: options.logLevel ?? "WARN",
      },
    );
    const workId = await ctx.db.insert("pendingWork", {
      handle,
      fnArgs,
      fnType,
      runAtTime,
    });
    const delay = Math.max(runAtTime - Date.now(), debounceMs);
    await kickMainLoop(ctx, delay, false);
    return workId;
  },
});

export const cancel = mutation({
  args: {
    id: v.id("pendingWork"),
  },
  handler: async (ctx, { id }) => {
    await ctx.db.insert("pendingCancelation", { workId: id });
  },
});

async function getOptions(db: DatabaseReader) {
  const pool = (await db.query("pools").unique())!;
  return {
    maxParallelism: pool.maxParallelism,
    actionTimeoutMs: pool.actionTimeoutMs,
    mutationTimeoutMs: pool.mutationTimeoutMs,
    unknownTimeoutMs: pool.unknownTimeoutMs,
    debounceMs: pool.debounceMs,
    fastHeartbeatMs: pool.fastHeartbeatMs,
    slowHeartbeatMs: pool.slowHeartbeatMs,
  };
}

async function console(ctx: QueryCtx) {
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
  args: {
    generation: v.number(),
  },
  handler: async (ctx, args) => {

    // Make sure mainLoop is serialized.
    const loopDoc = await ctx.db.query("mainLoop").unique();
    const expectedGeneration = loopDoc?.generation ?? 0;
    if (expectedGeneration !== args.generation) {
      throw new Error(`mainLoop generation mismatch ${expectedGeneration} !== ${args.generation}`);
    }
    if (loopDoc) {
      await ctx.db.patch(loopDoc._id, { generation: args.generation + 1 });
    } else {
      await ctx.db.insert("mainLoop", { generation: args.generation + 1 });
    }

    const { maxParallelism, debounceMs } = await getOptions(ctx.db);

    // This is the only function reading and writing inProgressWork,
    // and it's bounded by MAX_POSSIBLE_PARALLELISM, so we can
    // read it all into memory.
    const inProgressBefore = await ctx.db.query("inProgressWork").collect();

    // Move from pendingWork to inProgressWork.
    const toSchedule = Math.min(maxParallelism - inProgressBefore.length, BATCH_SIZE);
    let didSomething = false;
    const pending = await ctx.db.query("pendingWork")
      .withIndex("runAtTime", q=>q.lte("runAtTime", Date.now()))
      .take(toSchedule);
    await Promise.all(pending.map(async (work) => {
      const { scheduledId, timeoutMs } = await beginWork(ctx, work);
      await ctx.db.insert("inProgressWork", {
        running: scheduledId,
        timeoutMs,
        workId: work._id,
      });
      await ctx.db.delete(work._id);
      didSomething = true;
    }));

    // Move from pendingCompletion to completedWork, deleting from inProgressWork.
    // We could do all of these, but we don't want to OCC with work completing,
    // so we only do a few at a time.
    const completed = await ctx.db.query("pendingCompletion").take(BATCH_SIZE);
    await Promise.all(completed.map(async (work) => {
      const inProgressWork = await ctx.db.query("inProgressWork").withIndex("workId", (q) => q.eq("workId", work.workId)).unique();
      if (inProgressWork) {
        await ctx.db.delete(inProgressWork._id);
      }
      await ctx.db.delete(work._id);
      await ctx.db.insert("completedWork", {
        result: work.result,
        error: work.error,
        workId: work.workId,
      });
      didSomething = true;
    }));

    const canceled = await ctx.db.query("pendingCancelation").take(BATCH_SIZE);
    await Promise.all(canceled.map(async (work) => {
      const inProgressWork = await ctx.db.query("inProgressWork").withIndex("workId", (q) => q.eq("workId", work.workId)).unique();
      if (inProgressWork) {
        await ctx.scheduler.cancel(inProgressWork.running);
        await ctx.db.delete(inProgressWork._id);
        await ctx.db.insert("completedWork", {
          workId: work.workId,
          error: "Canceled",
        });
      }
      await ctx.db.delete(work._id);
      didSomething = true;
    }));

    if (completed.length === 0) {
      // If all completions are handled, check everything in inProgressWork.
      // This will find everything that timed out, failed ungracefully, was
      // cancelled, or succeeded without a return value.
      const inProgress = await ctx.db.query("inProgressWork").collect();
      await Promise.all(inProgress.map(async (work) => {
        const result = await checkInProgressWork(ctx, work);
        if (result !== null) {
          (await console(ctx)).debug("inProgressWork finished uncleanly", work.workId, result);
          await ctx.db.delete(work._id);
          await ctx.db.insert("completedWork", {
            workId: work.workId,
            ...result,
          });
          didSomething = true;
        }
      }));
    }

    if (didSomething) {
      // There might be more to do.
      await kickMainLoop(ctx, debounceMs, true);
    } else {
      // Decide when to wake up.
      const { fastHeartbeatMs, slowHeartbeatMs } = await getOptions(ctx.db);
      const allInProgressWork = await ctx.db.query("inProgressWork").collect();
      const nextPending = await ctx.db.query("pendingWork").withIndex("runAtTime").first();
      const nextPendingTime = nextPending ? nextPending.runAtTime : slowHeartbeatMs + Date.now();
      const nextInProgress = allInProgressWork.length ? Math.min(
        fastHeartbeatMs + Date.now(),
        ...allInProgressWork.map((w) => w._creationTime + w.timeoutMs),
      ) : Number.POSITIVE_INFINITY;
      const nextTime = Math.min(nextPendingTime, nextInProgress);
      await kickMainLoop(ctx, nextTime - Date.now(), true);
    }
  }
});

async function beginWork(
  ctx: MutationCtx,
  work: Doc<"pendingWork">,
): Promise<{
  scheduledId: Id<"_scheduled_functions">,
  timeoutMs: number,
}> {
  const { mutationTimeoutMs, actionTimeoutMs, unknownTimeoutMs } = await getOptions(ctx.db);
  if (work.fnType === "action") {
    return {
      scheduledId: await ctx.scheduler.runAfter(0, internal.public.runActionWrapper, {
        workId: work._id,
        handle: work.handle,
        fnArgs: work.fnArgs,
      }),
      timeoutMs: actionTimeoutMs,
    };
  } else if (work.fnType === "mutation") {
    return {
      scheduledId: await ctx.scheduler.runAfter(0, internal.public.runMutationWrapper, {
        workId: work._id,
        handle: work.handle,
        fnArgs: work.fnArgs,
      }),
      timeoutMs: mutationTimeoutMs,
    };
  } else if (work.fnType === "unknown") {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const handle = work.handle as FunctionHandle<'action' | 'mutation', any, any>;
    return {
      scheduledId: await ctx.scheduler.runAfter(0, handle, work.fnArgs),
      timeoutMs: unknownTimeoutMs,
    };
  } else {
    throw new Error(`Unexpected fnType ${work.fnType}`);
  }
}

async function checkInProgressWork(
  ctx: MutationCtx,
  doc: Doc<"inProgressWork">,
): Promise<{ result?: unknown, error?: string } | null> {
  const workStatus = await ctx.db.system.get(doc.running);
  if (workStatus === null) {
    return { error: "Timeout" };
  } else if (workStatus.state.kind === "pending" || workStatus.state.kind === "inProgress") {
    if (Date.now() - workStatus._creationTime > doc.timeoutMs) {
      await ctx.scheduler.cancel(doc.running);
      return { error: "Timeout" };
    }
  } else if (workStatus.state.kind === "success") {
    // Usually this would be handled by pendingCompletion, but for "unknown"
    // functions, this is how we know that they're done, and we can't get their
    // return values.
    return { result: null };
  } else if (workStatus.state.kind === "canceled") {
    return { error: "Canceled" };
  } else if (workStatus.state.kind === "failed") {
    return { error: workStatus.state.error };
  }
  return null;
}

export const runActionWrapper = internalAction({
  args: {
    workId: v.id("pendingWork"),
    handle: v.string(),
    fnArgs: v.any(),
  },
  handler: async (ctx, { workId, handle: handleStr, fnArgs }) => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const handle = handleStr as FunctionHandle<'action', any, any>;
    try {
      const retval = await ctx.runAction(handle, fnArgs);
      await ctx.runMutation(internal.public.saveResult, { workId, result: retval });
    } catch (e: unknown) {
      await ctx.runMutation(internal.public.saveResult, { workId, error: (e as Error).message });
    }
  },
});

export const saveResult = internalMutation({
  args: {
    workId: v.id("pendingWork"),
    result: v.optional(v.any()),
    error: v.optional(v.string()),
  },
  handler: saveResultHandler,
});

async function saveResultHandler(ctx: MutationCtx, { workId, result, error }: {
  workId: Id<"pendingWork">,
  result?: unknown,
  error?: string,
}): Promise<void> {
  const { debounceMs } = await getOptions(ctx.db);
  await ctx.db.insert("pendingCompletion", {
    result,
    error,
    workId,
  });
  await kickMainLoop(ctx, debounceMs, false);
}

export const runMutationWrapper = internalMutation({
  args: {
    workId: v.id("pendingWork"),
    handle: v.string(),
    fnArgs: v.any(),
  },
  handler: async (ctx, { workId, handle: handleStr, fnArgs }) => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const handle = handleStr as FunctionHandle<'mutation', any, any>;
    try {
      const retval = await ctx.runMutation(handle, fnArgs);
      await saveResultHandler(ctx, { workId, result: retval });
    } catch (e: unknown) {
      await saveResultHandler(ctx, { workId, error: (e as Error).message });
    }
  }
});

async function kickMainLoop(ctx: MutationCtx, delayMs: number, isCurrentlyExecuting: boolean): Promise<void> {
  const { debounceMs } = await getOptions(ctx.db);
  const delay = Math.max(delayMs, debounceMs);
  const mainLoop = await ctx.db.query("mainLoop").unique();
  if (!mainLoop) {
    (await console(ctx)).debug("starting mainLoop");
    const fn = await ctx.scheduler.runAfter(delay, internal.public.mainLoop, { generation: 0 });
    await ctx.db.insert("mainLoop", { fn, generation: 0 });
    return;
  }
  const existingFn = mainLoop.fn ? await ctx.db.system.get(mainLoop.fn) : null;
  if (existingFn === null || existingFn.completedTime) {
    // mainLoop stopped, so we restart it.
    const fn = await ctx.scheduler.runAfter(delay, internal.public.mainLoop, { generation: mainLoop.generation });
    await ctx.db.patch(mainLoop._id, { fn });
    (await console(ctx)).debug("mainLoop stopped, so we restarted it");
  } else if (!isCurrentlyExecuting && existingFn.scheduledTime < Date.now() + delay) {
    // mainLoop will run soon anyway, so we don't need to kick it.
    // console.trace("mainLoop already scheduled to run soon");
    return;
  } else {
    // mainLoop is scheduled to run later, so we should cancel it and reschedule.
    if (!isCurrentlyExecuting && mainLoop.fn) {
      await ctx.scheduler.cancel(mainLoop.fn);
    }
    const fn = await ctx.scheduler.runAfter(delay, internal.public.mainLoop, { generation: mainLoop.generation });
    await ctx.db.patch(mainLoop._id, { fn });
    (await console(ctx)).debug("mainLoop was scheduled later, so reschedule it to run sooner");
  }
}

export const status = query({
  args: {
    id: v.id("pendingWork"),
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
      result: v.any(),
    }),
    v.object({
      kind: v.literal("error"),
      error: v.string(),
    }),
  ),
  handler: async (ctx, { id }) => {
    const completedWork = await ctx.db.query("completedWork")
      .withIndex("workId", (q) => q.eq("workId", id))
      .unique();
    if (completedWork) {
      if (completedWork.error) {
        return { kind: "error", error: completedWork.error! } as const;
      }
      return { kind: "success", result: completedWork.result! } as const;
    }
    const pendingWork = await ctx.db.get(id);
    if (pendingWork) {
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
    const docs = await ctx.db.query("completedWork")
      .withIndex("by_creation_time", (q) => q.lt("_creationTime", old))
      .collect();
    await Promise.all(docs.map((doc) => ctx.db.delete(doc._id)));
  },
});

const MAX_POSSIBLE_PARALLELISM = 300;
const CLEANUP_CRON_NAME = "cleanup";

async function ensurePoolExists(
  ctx: MutationCtx,
  {
    maxParallelism,
    actionTimeoutMs,
    mutationTimeoutMs,
    unknownTimeoutMs,
    debounceMs,
    fastHeartbeatMs,
    slowHeartbeatMs,
    completedWorkMaxAgeMs,
    logLevel,
  }: {
    maxParallelism: number,
    actionTimeoutMs: number,
    mutationTimeoutMs: number,
    unknownTimeoutMs: number,
    debounceMs: number,
    fastHeartbeatMs: number,
    slowHeartbeatMs: number,
    completedWorkMaxAgeMs: number,
    logLevel: LogLevel,
  },
) {
  if (maxParallelism > MAX_POSSIBLE_PARALLELISM) {
    throw new Error(`maxParallelism must be <= ${MAX_POSSIBLE_PARALLELISM}`);
  }
  if (maxParallelism < 1) {
    throw new Error("maxParallelism must be >= 1");
  }
  if (debounceMs < 10) {
    throw new Error("debounceMs must be >= 10 to prevent OCCs");
  }
  const pool = await ctx.db.query("pools").unique();
  if (pool) {
    if (pool.maxParallelism !== maxParallelism) {
      await ctx.db.patch(pool._id, { maxParallelism });
    }
    if (pool.actionTimeoutMs !== actionTimeoutMs) {
      await ctx.db.patch(pool._id, { actionTimeoutMs });
    }
    if (pool.mutationTimeoutMs !== mutationTimeoutMs) {
      await ctx.db.patch(pool._id, { mutationTimeoutMs });
    }
    if (pool.unknownTimeoutMs !== unknownTimeoutMs) {
      await ctx.db.patch(pool._id, { unknownTimeoutMs });
    }
    if (pool.debounceMs !== debounceMs) {
      await ctx.db.patch(pool._id, { debounceMs });
    }
    if (pool.fastHeartbeatMs !== fastHeartbeatMs) {
      await ctx.db.patch(pool._id, { fastHeartbeatMs });
    }
    if (pool.slowHeartbeatMs !== slowHeartbeatMs) {
      await ctx.db.patch(pool._id, { slowHeartbeatMs });
    }
    if (pool.completedWorkMaxAgeMs !== completedWorkMaxAgeMs) {
      await ctx.db.patch(pool._id, { completedWorkMaxAgeMs });
    }
    if (pool.logLevel !== logLevel) {
      await ctx.db.patch(pool._id, { logLevel });
    }
  }
  if (!pool) {
    await ctx.db.insert("pools", {
      maxParallelism,
      actionTimeoutMs,
      mutationTimeoutMs,
      unknownTimeoutMs,
      debounceMs,
      fastHeartbeatMs,
      slowHeartbeatMs,
      completedWorkMaxAgeMs,
      logLevel,
    });
  }
  let cleanupCron = await crons.get(ctx, { name: CLEANUP_CRON_NAME });
  const cronFrequencyMs = Math.min(completedWorkMaxAgeMs, 24 * 60 * 60 * 1000);
  if (cleanupCron !== null && !(cleanupCron.schedule.kind === "interval" && cleanupCron.schedule.ms === cronFrequencyMs)) {
    await crons.delete(ctx, { id: cleanupCron.id });
    cleanupCron = null;
  }
  if (cleanupCron === null) {
    await crons.register(
      ctx,
      { kind: "interval", ms: cronFrequencyMs },
      api.public.cleanup,
      { maxAgeMs: completedWorkMaxAgeMs },
      CLEANUP_CRON_NAME,
    );
  }
}
