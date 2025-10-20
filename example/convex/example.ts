import {
  mutation,
  action,
  query,
  internalAction,
  internalMutation,
} from "./_generated/server";
import { api, components, internal } from "./_generated/api";
import {
  WorkId,
  vWorkId,
  Workpool,
  vOnCompleteArgs,
} from "@convex-dev/workpool";
import { v } from "convex/values";
import { createLogger } from "../../src/component/logging";
import { FunctionArgs } from "convex/server";

const bigPool = new Workpool(components.bigPool, {
  maxParallelism: 20,
  defaultRetryBehavior: {
    maxAttempts: 3,
    initialBackoffMs: 100,
    base: 2,
  },
  retryActionsByDefault: true,
  logLevel: "INFO",
});
const smallPool = new Workpool(components.smallPool, {
  maxParallelism: 3,
  retryActionsByDefault: true,
  logLevel: "INFO",
});
const serializedPool = new Workpool(components.serializedPool, {
  maxParallelism: 1,
  retryActionsByDefault: true,
  logLevel: "INFO",
});
const console = createLogger("DEBUG");

export const addMutation = mutation({
  args: { data: v.optional(v.number()) },
  handler: async (ctx, { data }) => {
    const d = data ?? Math.random();
    await ctx.db.insert("data", { data: d });
    return d;
  },
});

export const addAction = action({
  args: { data: v.optional(v.number()) },
  handler: async (ctx, { data }): Promise<number> => {
    return await ctx.runMutation(api.example.addMutation, { data });
  },
});

export const queryData = query({
  args: { add: v.optional(v.number()) },
  handler: async (ctx, { add }) => {
    const dataDocs = await ctx.db.query("data").collect();
    return dataDocs.map((doc) => doc.data + (add ?? 0));
  },
});

export const enqueueOneMutation = mutation({
  args: { data: v.number() },
  handler: async (ctx, { data }): Promise<WorkId> => {
    return await smallPool.enqueueMutation(ctx, api.example.addMutation, {
      data,
    });
  },
});

export const enqueueOneQuery = mutation({
  args: { add: v.optional(v.number()) },
  handler: async (ctx, { add }): Promise<WorkId> => {
    return await smallPool.enqueueQuery(
      ctx,
      api.example.queryData,
      { add },
      {
        onComplete: internal.example.complete,
        context: Date.now(),
      },
    );
  },
});

export const cancelMutation = mutation({
  args: { id: vWorkId },
  handler: async (ctx, { id }) => {
    await smallPool.cancel(ctx, id);
  },
});

export const status = query({
  args: { id: vWorkId },
  handler: async (ctx, { id }) => {
    return await smallPool.status(ctx, id);
  },
});

export const enqueueABunchOfMutations = action({
  args: {},
  handler: async (ctx, _args) => {
    await Promise.all(
      Array.from({ length: 30 }, () =>
        smallPool.enqueueMutation(ctx, api.example.addMutation, {}),
      ),
    );
  },
});

export const addLowPri = mutation({
  args: { data: v.optional(v.number()) },
  handler: async (ctx, { data }) => {
    const d = -(data ?? Math.random());
    await ctx.db.insert("data", { data: d });
    return d;
  },
});

export const enqueueLowPriMutations = action({
  args: {},
  handler: async (ctx, _args) => {
    await Promise.all(
      Array.from({ length: 30 }, () =>
        serializedPool.enqueueMutation(ctx, api.example.addLowPri, {}),
      ),
    );
  },
});

export const highPriMutation = mutation({
  args: { data: v.number() },
  handler: async (ctx, { data }) => {
    await bigPool.enqueueMutation(ctx, api.example.addMutation, { data });
  },
});

export const enqueueABunchOfActions = action({
  args: {},
  handler: async (ctx, _args) => {
    await Promise.all(
      Array.from({ length: 30 }, () =>
        bigPool.enqueueAction(ctx, api.example.addAction, {}),
      ),
    );
  },
});

export const enqueueAnAction = mutation({
  args: {},
  handler: async (ctx, _args): Promise<void> => {
    await bigPool.enqueueAction(ctx, api.example.addAction, {});
  },
});

export const echo = query({
  args: { num: v.number() },
  handler: async (ctx, { num }) => {
    return num;
  },
});

async function sampleWork() {
  const index = Math.floor(Math.random() * 1000) + 1;
  await new Promise((resolve) => setTimeout(resolve, Math.random() * index));
}

// Example background work: scraping from a website.
export const backgroundWork = internalAction({
  args: {},
  handler: async () => {
    await sampleWork();
  },
});

export const startBackgroundWork = internalAction({
  args: {},
  handler: async (ctx, _args) => {
    await Promise.all(
      Array.from({ length: 20 }, () =>
        serializedPool.enqueueAction(ctx, internal.example.backgroundWork, {}),
      ),
    );
  },
});

// Example foreground work: calling an API on behalf of a user.
export const foregroundWork = internalAction({
  args: { ms: v.optional(v.number()) },
  handler: async (ctx, args) => {
    if (args.ms) {
      await new Promise((resolve) => setTimeout(resolve, args.ms));
    }
    await sampleWork();
  },
});

export const startForegroundWork = internalAction({
  args: {},
  handler: async (ctx, _args) => {
    await Promise.all(
      Array.from(
        { length: 100 },
        async () =>
          await bigPool.enqueueAction(ctx, internal.example.foregroundWork, {}),
      ),
    );
  },
});

export const enqueueABatchOfActions = internalAction({
  args: {},
  handler: async (ctx, _args) => {
    await smallPool.enqueueActionBatch(
      ctx,
      internal.example.myAction,
      Array.from({ length: 10 }, () => ({
        fate: "succeed",
        ms: 1000,
      })),
    );
  },
});

const fate = v.union(
  v.literal("succeed"),
  v.literal("fail randomly"),
  v.literal("fail always"),
);

export const myAction = internalAction({
  args: { fate, ms: v.optional(v.number()) },
  handler: async (_ctx, { fate, ms }) => {
    if (ms) {
      await new Promise((resolve) => setTimeout(resolve, ms));
    }
    switch (fate) {
      case "succeed":
        console.debug("success");
        break;
      case "fail randomly":
        if (Math.random() < 0.8) {
          throw new Error("action failed.");
        }
        if (Math.random() < 0.01) {
          // Incur a timeout.
          console.debug("I'm a baaaad timeout job.");
          await new Promise((resolve) => setTimeout(resolve, 15 * 60 * 1000));
        }
        console.debug("action succeded.");
        break;
      case "fail always":
        throw new Error("action failed.");
      default:
        throw new Error("invalid action");
    }
  },
});

// A convenient way to define the onComplete mutation.
export const onComplete = bigPool.defineOnComplete({
  context: v.number(),
  handler: async (ctx, args) => {
    console.info("total", (Date.now() - args.context) / 1000);
  },
});

export const noop = internalMutation({
  args: { started: v.number() },
  handler: async (ctx, args) => {
    console.warn(`lag: ${Date.now() - args.started}`);
    return Date.now();
  },
});

// Another way to define the onComplete mutation.
export const complete = internalMutation({
  args: vOnCompleteArgs(v.number()),
  handler: async (ctx, args) => {
    if (args.result.kind === "success") {
      console.warn("onComplete delay", Date.now() - args.result.returnValue);
    }
    console.warn("total", (Date.now() - args.context) / 1000);
  },
});

export const singleLatency = internalMutation({
  args: {},
  handler: async (ctx, _args) => {
    const started = Date.now();
    await bigPool.enqueueMutation(
      ctx,
      internal.example.noop,
      { started },
      {
        context: started,
        onComplete: internal.example.complete,
      },
    );
  },
});

const N = 100;
const BASE_MS = 100;
const MAX_MS = 1000;
const CONCURRENCY = 15;
export const runPaced = internalAction({
  args: { n: v.optional(v.number()) },
  handler: async (ctx, args) => {
    const ids: WorkId[] = [];
    const start = Date.now();
    for (let i = 0; i < (args.n ?? N); i++) {
      const args = {
        fate: "fail randomly",
        ms: BASE_MS + (MAX_MS - BASE_MS) * Math.random(),
      } as FunctionArgs<typeof internal.example.myAction>;
      const id: WorkId = await bigPool.enqueueAction(
        ctx,
        internal.example.myAction,
        args,
        { onComplete: internal.example.onComplete, context: start },
      );
      console.debug("enqueued", Date.now());
      ids.push(id);
      // exponential distribution of time to wait.
      const avgRate = CONCURRENCY / ((BASE_MS + MAX_MS) / 2);
      const t = -Math.log(Math.random()) / avgRate;
      await new Promise((resolve) => setTimeout(resolve, t));
    }
  },
});

export const cancel = internalAction({
  args: {
    id: v.optional(vWorkId),
  },
  handler: async (ctx, args) => {
    console.debug("Canceling", args.id);
    if (args.id) {
      console.debug("Canceling", args.id);
      await bigPool.cancel(ctx, args.id as WorkId);
    } else {
      await bigPool.cancelAll(ctx);
    }
  },
});
