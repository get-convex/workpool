import { v } from "convex/values";
import { DatabaseReader, DatabaseWriter, internalAction, internalMutation, mutation, MutationCtx, query } from "./_generated/server";
import { FunctionHandle } from "convex/server";
import { Id } from "./_generated/dataModel";
import { internal } from "./_generated/api";

// Intentionally written so it never contends with any other mutations.
export const enqueue = mutation({
  args: {
    handle: v.string(),
    maxParallelism: v.number(),
    fnArgs: v.any(),
    fnType: v.union(v.literal("action"), v.literal("mutation")),
  },
  returns: v.id("pendingWork"),
  handler: async (ctx, { handle, maxParallelism, fnArgs, fnType }) => {
    await ensurePoolExists(ctx.db, maxParallelism);
    const workId = await ctx.db.insert("pendingWork", {
      handle,
      fnArgs,
      fnType,
    });
    await kickMainLoop(ctx, MAIN_LOOP_DEBOUNCE_MS, true);
    return workId;
  },
});

async function getMaxParallelism(db: DatabaseReader): Promise<number> {
  const pool = (await db.query("pools").unique())!;
  return pool.maxParallelism;
}

async function getCountInProgress(db: DatabaseReader): Promise<number> {
  const poolState = (await db.query("poolState").unique())!;
  return poolState.countInProgress;
}

async function getPoolState(db: DatabaseReader): Promise<{
  countInProgress: number;
  maxParallelism: number;
}> {
  return {
    countInProgress: await getCountInProgress(db),
    maxParallelism: await getMaxParallelism(db),
  };
}

const WORK_TIMEOUT = 10 * 60 * 1000;

// There should only ever be at most one of these scheduled or running.
// The scheduled one is in the "mainLoop" table.
export const mainLoop = internalMutation({
  args: {},
  handler: async (ctx) => {
    const { countInProgress, maxParallelism } = await getPoolState(ctx.db);

    // Move from pendingWork to inProgressWork.
    const toSchedule = Math.min(maxParallelism - countInProgress, 100);
    let count = countInProgress;
    let didSomething = false;
    const pending = await ctx.db.query("pendingWork").take(toSchedule);
    for (const work of pending) {
      let scheduledId: Id<"_scheduled_functions">;
      if (work.fnType === "action") {
        await kickMainLoop(ctx, WORK_TIMEOUT, false);
        scheduledId = await ctx.scheduler.runAfter(0, internal.public.runActionWrapper, {
          workId: work._id,
          handle: work.handle,
          fnArgs: work.fnArgs,
        });
      } else {
        scheduledId = await ctx.scheduler.runAfter(0, internal.public.runMutationWrapper, {
          workId: work._id,
          handle: work.handle,
          fnArgs: work.fnArgs,
        });
      }
      await ctx.db.insert("inProgressWork", {
        running: scheduledId,
        workId: work._id,
      });
      await ctx.db.delete(work._id);
      didSomething = true;
      count++;
    }

    // Move from pendingCompletion to completedWork, deleting from inProgressWork.
    const completed = await ctx.db.query("pendingCompletion").take(100);
    for (const work of completed) {
      const inProgressWork = await ctx.db.query("inProgressWork").withIndex("workId", (q) => q.eq("workId", work.workId)).unique();
      if (!inProgressWork) {
        continue;
      }
      await ctx.db.delete(inProgressWork._id);
      await ctx.db.insert("completedWork", {
        result: work.result,
        error: work.error,
        workId: work.workId,
      });
      didSomething = true;
      count--;
    }

    // Check for timeouts in inProgressWork.
    const oldInProgress = await ctx.db.query("inProgressWork").withIndex("by_creation_time", q=>q.lt("_creationTime", Date.now() - WORK_TIMEOUT)).collect();
    for (const work of oldInProgress) {
      await ctx.scheduler.cancel(work.running);
      await ctx.db.delete(work._id);
      await ctx.db.insert("completedWork", {
        error: "Timeout",
        workId: work.workId,
      });
      didSomething = true;
      count--;
    }

    if (didSomething) {
      const poolState = (await ctx.db.query("poolState").unique())!;
      await ctx.db.patch(poolState._id, { countInProgress: count });

      // There might be more to do.
      await kickMainLoop(ctx, MAIN_LOOP_DEBOUNCE_MS, false);
    }
  }
});

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
  await ctx.db.insert("pendingCompletion", {
    result,
    error,
    workId,
  });
  await kickMainLoop(ctx, MAIN_LOOP_DEBOUNCE_MS, false);
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

const MAIN_LOOP_DEBOUNCE_MS = 50;

async function kickMainLoop(ctx: MutationCtx, delay: number, cancelExisting: boolean): Promise<void> {
  const mainLoop = await ctx.db.query("mainLoop").unique();
  if (mainLoop) {
    const existingFn = await ctx.db.system.get(mainLoop.fn);
    if (existingFn === null || existingFn.completedTime) {
      // mainLoop stopped, so we restart it.
      const fn = await ctx.scheduler.runAfter(delay, internal.public.mainLoop, {});
      await ctx.db.patch(mainLoop._id, { fn });
    } else if (existingFn.scheduledTime < Date.now() + delay) {
      // mainLoop will run soon anyway, so we don't need to kick it.
      return;
    } else {
      // mainLoop is scheduled to run later, so we should cancel it and reschedule.
      if (cancelExisting) {
        await ctx.scheduler.cancel(mainLoop.fn);
      }
      const fn = await ctx.scheduler.runAfter(delay, internal.public.mainLoop, {});
      await ctx.db.patch(mainLoop._id, { fn });
    }
  } else {
    const fn = await ctx.scheduler.runAfter(delay, internal.public.mainLoop, {});
    await ctx.db.insert("mainLoop", { fn });
  }
}

export const result = query({
  args: {
    id: v.id("pendingWork"),
  },
  returns: v.object({
    result: v.optional(v.any()),
  }),
  handler: async (ctx, { id }) => {
    const completedWork = await ctx.db.query("completedWork").withIndex("workId", (q) => q.eq("workId", id)).unique();
    if (completedWork) {
      if (completedWork.error) {
        throw new Error(completedWork.error);
      }
      return { result: completedWork.result };
    } else {
      return {};
    }
  },
});

async function ensurePoolExists(
  db: DatabaseWriter,
  maxParallelism: number,
) {
  const pool = await db.query("pools").unique();
  if (!pool) {
    await db.insert("pools", {
      maxParallelism,
    });
    await db.insert("poolState", {
      countInProgress: 0,
    });
  }
}
