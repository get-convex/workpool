# Convex Component: Workpool

[![npm version](https://badge.fury.io/js/@convex-dev%2Fworkpool.svg)](https://badge.fury.io/js/@convex-dev%2Fworkpool)

**Note: Convex Components are currently in beta**

<!-- START: Include on https://convex.dev/components -->

This Convex component pools actions and mutations to restrict parallel requests.

Suppose you have some important async work, like sending verification emails,
and some less important async work, like scraping data from an API. If all of
these are scheduled with `ctx.scheduler.runAfter`, they'll compete with each
other for resources. The emails might be delayed if there are too many scraping
requests queued ahead of them.

To resolve this problem, you can separate work into different pools.

```ts
const emailPool = new Workpool(components.emailWorkpool, {
  maxParallelism: 5,
});
const scrapePool = new Workpool(components.scrapeWorkpool, {
  maxParallelism: 1,
});

export const signUp = mutation({
  handler: async (ctx, args) => {
    const userId = await ctx.db.insert("users", args);
    await emailPool.enqueueAction(internal.auth.sendEmailVerification, {
      userId,
    });
  },
});

export const downloadLatestWeather = mutation({
  handler: async (ctx, args) => {
    for (const city of allCities) {
      await scrapePool.enqueueAction(internal.weather.scrape, { city });
    }
  },
});
```

With limited parallelism, you can reduce
[OCC errors](https://docs.convex.dev/error#1)
from mutations that read and write the same data.

Additionally, a Workpool stores return values when async work completes.
And you can use a Workpool's `ctx` method to make sure `runMutation`,
`runAction`, and `scheduler` all use the pool.

Consider this action that calls a mutation to increment a singleton counter.
By calling the mutation on a workpool with `maxParallelism: 1`, it will never
throw an error due to conflicts with parallel mutations.

```ts
const counterPool = new Workpool(components.counterWorkpool, {
  maxParallelism: 1,
});
export const doSomethingAndCount = action({
  handler: async (ctx) => {
    // poolCtx has the same interface as ctx, but it runs everything in the pool.
    const poolCtx = counterPool.ctx(ctx);
    const newValue = await poolCtx.runMutation(api.counter.increment);
    // You can schedule things and they will run in the pool.
    await poolCtx.scheduler.runAfter(100, api.counter.increment);
  },
});
```

Effectively, Workpool runs async functions similar to `ctx.scheduler`, but with
limited parallelism. And it provides additional features like return values and
configurable timeouts.

## Pre-requisite: Convex

You'll need an existing Convex project to use the component.
Convex is a hosted backend platform, including a database, serverless functions,
and a ton more you can learn about [here](https://docs.convex.dev/get-started).

Run `npm create convex` or follow any of the [quickstarts](https://docs.convex.dev/home) to set one up.

## Installation

See [`example/`](./example/convex/) for a working demo.

1. Install the Workpool component:

```bash
npm install @convex-dev/workpool
```

2. Create a [`convex.config.ts`](./example/convex/convex.config.ts) file in your
   app's `convex/` folder and install the component by calling `use`:

```ts
// convex/convex.config.ts
import { defineApp } from "convex/server";
import workpool from "@convex-dev/workpool/convex.config";

const app = defineApp();
app.use(workpool, { name: "emailWorkpool" });
app.use(workpool, { name: "scrapeWorkpool" });
export default app;
```

## Usage

```ts
import { components } from "./_generated/api";
import { Workpool } from "@convex-dev/counter";

const pool = new Workpool(components.emailWorkpool, {
  maxParallelism: 10,
  // More options available, such as:
  actionTimeoutMs: 10 * 60 * 1000,
  ttl: 7 * 24 * 60 * 60 * 1000,
});
```

Then you have the following interface on `pool`:

```ts
// Schedule functions to run in the background.
const id = await pool.enqueueMutation(api.foo.bar, args);
const id = await pool.enqueueAction(api.foo.bar, args);

// Is it done yet?
const status = await pool.status(id);
// Wait for it to be done and get the return value.
const result = await pool.pollResult(id);

// ActionCtx that uses the pool to run and schedule actions and mutations.
const poolCtx = pool.ctx(ctx);
const result = await poolCtx.runMutation(api.foo.bar, args);
const result = await poolCtx.runAction(api.foo.bar, args);
const id = await poolCtx.scheduler.runAfter(100, api.foo.bar, args);
const id = await poolCtx.scheduler.runAt(timestamp, api.foo.bar, args);
// Note `poolCtx.scheduler.cancel` will only work on IDs returned by
// `poolCtx.scheduler.runAt` or `poolCtx.scheduler.runAfter`
await poolCtx.scheduler.cancel(id);
```

See more example usage in [example.ts](./example/convex/example.ts).

<!-- END: Include on https://convex.dev/components -->
