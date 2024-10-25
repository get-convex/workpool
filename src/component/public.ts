import { Infer, v } from "convex/values";
import { DatabaseReader, DatabaseWriter, internalAction, internalMutation, mutation, MutationCtx, query } from "./_generated/server";
import { FunctionArgs, FunctionHandle, FunctionType } from "convex/server";
import { Doc, Id } from "./_generated/dataModel";
import { internal } from "./_generated/api";

// In order from child to parent.
const poolOptionsValidator = v.array(v.object({
  name: v.string(),
  maxParallelism: v.optional(v.number()),
  priority: v.optional(v.union(v.literal("low"), v.literal("normal"), v.literal("high"))),
}));

// Intentionally written so it never contends with any other mutations.
export const enqueue = mutation({
  args: {
    handle: v.string(),
    options: poolOptionsValidator,
    fnArgs: v.any(),
    fnType: v.union(v.literal("action"), v.literal("mutation")),
  },
  returns: v.string(),
  handler: async (ctx, { handle, options, fnArgs, fnType }) => {
    const { pool } = await ensurePoolsExist(ctx.db, options);
    const workId = await ctx.db.insert("pendingWork", {
      pool,
      handle,
      fnArgs,
      fnType,
    });
    await ctx.scheduler.runAfter(0, internal.public.runNew);
    return workId;
  },
});

export const runNew = internalMutation({
  args: {},
  handler: async (ctx) => {
    await kickMainLoop(ctx, true);
  },
});

async function getEffectiveMaxParallelism(db: DatabaseReader, poolId: Id<"pools">): Promise<number> {
  const pool = (await db.get(poolId))!;
  const individualMaxParallelism = pool.maxParallelism ?? Number.POSITIVE_INFINITY;
  const parentMaxParallelism = pool.parent ? await getEffectiveMaxParallelism(db, pool.parent) : Number.POSITIVE_INFINITY;
  return Math.min(individualMaxParallelism, parentMaxParallelism);
}

async function getEffectiveCountInProgress(db: DatabaseReader, poolId: Id<"pools">): Promise<number> {
  const poolState = (await db.query("poolState").withIndex("pool", (q) => q.eq("pool", poolId)).unique())!;
  const children = await db.query("pools").withIndex("name", (q) => q.eq("parent", poolId)).collect();
  const childrenCount = await Promise.all(children.map((child) => getEffectiveCountInProgress(db, child._id)));
  return poolState.countInProgress + childrenCount.reduce((a, b) => a + b, 0);
}

async function getPoolState(db: DatabaseReader, poolId: Id<"pools">): Promise<{
  countInProgress: number;
  maxParallelism: number;
}> {
  return {
    countInProgress: await getEffectiveCountInProgress(db, poolId),
    maxParallelism: await getEffectiveMaxParallelism(db, poolId),
  };
}

// There should only ever be at most one of these scheduled or running.
// The scheduled one is in the "mainLoop" table.
export const mainLoop = internalMutation({
  args: {},
  handler: async (ctx) => {
    for await (const pool of ctx.db.query("pools").withIndex("overallPriority").order("desc")) {
      const { countInProgress, maxParallelism } = await getPoolState(ctx.db, pool._id);
      if (countInProgress >= maxParallelism) {
        continue;
      }
      const toSchedule = Math.min(maxParallelism - countInProgress, 100);
      const pending = await ctx.db.query("pendingWork").withIndex("pool", (q) => q.eq("pool", pool._id)).take(toSchedule);
      for await (const work of pending) {
        let scheduledId: Id<"_scheduled_functions">;
        if (work.fnType === "action") {
          await ctx.scheduler.runAfter(10 * 60 * 1000, internal.public.actionTimeout, {
            pool: work.pool,
            workId: work._id,
          });
          scheduledId = await ctx.scheduler.runAfter(0, internal.public.runActionWrapper, {
            workId: work._id,
            pool: work.pool,
            handle: work.handle,
            fnArgs: work.fnArgs,
          });
        } else {
          scheduledId = await ctx.scheduler.runAfter(0, internal.public.runMutationWrapper, {
            workId: work._id,
            pool: work.pool,
            handle: work.handle,
            fnArgs: work.fnArgs,
          });
        }
        await ctx.db.insert("inProgressWork", {
          pool: work.pool,
          running: scheduledId,
          handle: work.handle,
          workId: work._id,
        });
        await ctx.db.delete(work._id);
        const poolState = (await ctx.db.query("poolState").withIndex("pool", (q) => q.eq("pool", work.pool)).unique())!;
        await ctx.db.patch(poolState._id, { countInProgress: poolState.countInProgress + 1 });
      }
      if (toSchedule < maxParallelism - countInProgress) {
        await kickMainLoop(ctx, false);
        break;
      }
    }
  }
});

export const runActionWrapper = internalAction({
  args: {
    workId: v.id("pendingWork"),
    pool: v.id("pools"),
    handle: v.string(),
    fnArgs: v.any(),
  },
  handler: async (ctx, { pool, workId, handle: handleStr, fnArgs }) => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const handle = handleStr as FunctionHandle<'action', any, any>;
    try {
      const retval = await ctx.runAction(handle, fnArgs);
      await ctx.runMutation(internal.public.saveResult, { pool, workId, result: retval });
    } catch (e: unknown) {
      await ctx.runMutation(internal.public.saveResult, { pool, workId, error: (e as Error).message });
    }
  },
});

export const actionTimeout = internalMutation({
  args: {
    workId: v.id("pendingWork"),
    pool: v.id("pools"),
  },
  handler: async (ctx, { pool, workId }) => {
    const work = await ctx.db.query("inProgressWork").withIndex("workId", (q) => q.eq("workId", workId)).unique();
    if (work) {
      await ctx.scheduler.cancel(work.running);
      await ctx.db.delete(work._id);
      await ctx.db.insert("completedWork", {
        pool,
        error: "Timeout",
        workId,
      });
      const poolState = (await ctx.db.query("poolState").withIndex("pool", (q) => q.eq("pool", pool)).unique())!;
      await ctx.db.patch(poolState._id, { countInProgress: poolState.countInProgress - 1 });
      await kickMainLoop(ctx, true);
    }
  }
});

export const saveResult = internalMutation({
  args: {
    pool: v.id("pools"),
    workId: v.id("pendingWork"),
    result: v.optional(v.any()),
    error: v.optional(v.string()),
  },
  handler: saveResultHandler,
});

async function saveResultHandler(ctx: MutationCtx, { pool, workId, result, error }: {
  pool: Id<"pools">,
  workId: Id<"pendingWork">,
  result?: unknown,
  error?: string,
}): Promise<void> {
  const inProgressWork = await ctx.db.query("inProgressWork").withIndex("workId", (q) => q.eq("workId", workId)).unique();
  if (!inProgressWork) {
    return;
  }
  await ctx.db.delete(inProgressWork._id);
  await ctx.db.insert("completedWork", {
    pool,
    result,
    error,
    workId,
  });
  await ctx.scheduler.runAfter(0, internal.public.decrementInProgress, { pool });
}

export const decrementInProgress = internalMutation({
  args: {
    pool: v.id("pools"),
  },
  handler: async (ctx, { pool }) => {
    const poolState = (await ctx.db.query("poolState").withIndex("pool", (q) => q.eq("pool", pool)).unique())!;
    await ctx.db.patch(poolState._id, { countInProgress: poolState.countInProgress - 1 });
    await kickMainLoop(ctx, true);
  },
});

export const runMutationWrapper = internalMutation({
  args: {
    workId: v.id("pendingWork"),
    pool: v.id("pools"),
    handle: v.string(),
    fnArgs: v.any(),
  },
  handler: async (ctx, { pool, workId, handle: handleStr, fnArgs }) => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const handle = handleStr as FunctionHandle<'mutation', any, any>;
    try {
      const retval = await ctx.runMutation(handle, fnArgs);
      await saveResultHandler(ctx, { pool, workId, result: retval });
    } catch (e: unknown) {
      await saveResultHandler(ctx, { pool, workId, error: (e as Error).message });
    }
  }
});

async function kickMainLoop(ctx: MutationCtx, cancelExisting: boolean): Promise<void> {
  const fn = await ctx.scheduler.runAfter(0, internal.public.mainLoop, {});
  const mainLoop = await ctx.db.query("mainLoop").unique();
  if (mainLoop) {
    if (cancelExisting) {
      await ctx.scheduler.cancel(mainLoop.fn);
    }
    await ctx.db.patch(mainLoop._id, { fn });
  } else {
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

async function ensurePoolsExist(
  db: DatabaseWriter,
  options: Infer<typeof poolOptionsValidator>,
): Promise<{
  pool: Id<"pools">,
}> {
  if (options.length === 0) {
    throw new Error("Must have at least one pool");
  }
  options.reverse();
  let parent: Id<"pools"> | undefined = undefined;
  let minPriority = 0;
  let maxPriority = 100;
  for (const poolOptions of options) {
    const priorityOption = poolOptions.priority ?? "normal";
    const firstThird = Math.floor((2 * minPriority + maxPriority) / 3);
    const secondThird = Math.ceil((minPriority + 2 * maxPriority) / 3);
    switch (priorityOption) {
      case "low":
        maxPriority = firstThird;
        break;
      case "normal":
        minPriority = firstThird;
        maxPriority = secondThird;
        break;
      case "high":
        minPriority = secondThird;
        break;
    }
    const overallPriority = Math.round((minPriority + maxPriority) / 2);

    const pool = await db
      .query("pools")
      .withIndex("name", (q) => q.eq("parent", parent).eq("name", poolOptions.name))
      .unique();
    if (pool) {
      if (poolOptions.maxParallelism !== pool?.maxParallelism) {
        await db.patch(pool._id, { maxParallelism: poolOptions.maxParallelism });
      }
      if (priorityOption !== pool?.priority) {
        await db.patch(pool._id, { priority: priorityOption });
      }
      if (pool.parent !== parent) {
        await db.patch(pool._id, { parent });
      }
      if (pool.overallPriority !== overallPriority) {
        await db.patch(pool._id, { overallPriority });
      }
      parent = pool._id;
    } else {
      const id: Id<"pools"> = await db.insert("pools", {
        name: poolOptions.name,
        maxParallelism: poolOptions.maxParallelism,
        priority: priorityOption,
        parent,
        overallPriority,
      });
      await db.insert("poolState", { pool: id, countInProgress: 0 });
      parent = id;
    }
  }
  return {
    pool: parent!,
  };
}
