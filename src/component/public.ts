import { Infer, v } from "convex/values";
import { DatabaseWriter, internalMutation, mutation, query } from "./_generated/server";
import { FunctionArgs, FunctionHandle, FunctionType } from "convex/server";
import { Id } from "./_generated/dataModel";
import { internal } from "./_generated/api";

// In order from child to parent.
const poolOptionsValidator = v.array(v.object({
  name: v.string(),
  maxParallelism: v.optional(v.number()),
  priority: v.optional(v.union(v.literal("low"), v.literal("normal"), v.literal("high"))),
}));

// Intentionally written so it never contends with any other mutations.
export const enqueueAction = mutation({
  args: {
    handle: v.string(),
    delayMs: v.number(),
    options: poolOptionsValidator,
    fnArgs: v.any(),
  },
  returns: v.string(),
  handler: async (ctx, { handle: handleStr, options, fnArgs, delayMs }) => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const handle = handleStr as FunctionHandle<FunctionType, any, any>;
    const { pool, priority } = await ensurePools(ctx.db, options);
    const workId = await ctx.db.insert("pendingWork", {
      pool,
      handle,
      priority,
      fnArgs,
    });
    await ctx.scheduler.runAfter(delayMs, internal.public.runNew);
    return workId;
  },
});

export const runNew = internalMutation({
  args: {},
  handler: async (ctx) => {
    const mainLoop = await ctx.db.query("mainLoop").unique();
    if (mainLoop) {
      await ctx.scheduler.cancel(mainLoop.fn);
      await ctx.db.delete(mainLoop._id);
    }
    const scheduledId = await ctx.scheduler.runAfter(0, internal.public.mainLoop, {});
    await ctx.db.insert("mainLoop", { fn: internal.public.mainLoop });
  },
});

// There should only ever be one of these scheduled or running.
// The scheduled one is in the "mainLoop" table.
export const mainLoop = internalMutation({
  args: {},
  handler: async (ctx) => {
    if (await ctx.db.query("mainLoop").count() > 0) {
    }
    const pending = await ctx.db.query("pendingWork").collect();
    for (const work of pending) {
      await ctx.db.insert("inProgressWork", {
        pool: work.pool,
        running: work._id,
        handle: work.handle,
      });
      await ctx.db.patch(work.pool, { countInProgress: (work.countInProgress ?? 0) + 1 });
      await ctx.scheduler.runNow(work.handle, work.fnArgs);
    }
  }
});

async function ensurePools(
  db: DatabaseWriter,
  options: Infer<typeof poolOptionsValidator>,
): Promise<{
  pool: Id<"pools">,
  priority: number,
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
    const pool = await db
      .query("pools")
      .withIndex("name", (q) => q.eq("name", poolOptions.name))
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
      parent = pool._id;
    } else {
      const id: Id<"pools"> = await db.insert("pools", {
        name: poolOptions.name,
        maxParallelism: poolOptions.maxParallelism,
        priority: priorityOption,
        parent,
      });
      parent = id;
    }
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
  }
  return {
    pool: parent!,
    priority: Math.round((minPriority + maxPriority) / 2),
  };
}

export const add = mutation({
  args: {
    name: v.string(),
    count: v.number(),
    shards: v.optional(v.number()),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const shard = Math.floor(Math.random() * (args.shards ?? 1));
    const counter = await ctx.db
      .query("counters")
      .withIndex("name", (q) => q.eq("name", args.name).eq("shard", shard))
      .unique();
    if (counter) {
      await ctx.db.patch(counter._id, {
        value: counter.value + args.count,
      });
    } else {
      await ctx.db.insert("counters", {
        name: args.name,
        value: args.count,
        shard,
      });
    }
  },
});

export const count = query({
  args: { name: v.string() },
  returns: v.number(),
  handler: async (ctx, args) => {
    const counters = await ctx.db
      .query("counters")
      .withIndex("name", (q) => q.eq("name", args.name))
      .collect();
    return counters.reduce((sum, counter) => sum + counter.value, 0);
  },
});
