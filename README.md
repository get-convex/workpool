# Convex Component: Workpool

[![npm version](https://badge.fury.io/js/@convex-dev%2Fworkpool.svg)](https://badge.fury.io/js/@convex-dev%2Fworkpool)

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

Consider this action that calls a mutation to increment a singleton counter.
By calling the mutation on a workpool with `maxParallelism: 1`, it will never
throw an error due to conflicts with parallel mutations.

```ts
const counterPool = new Workpool(components.counterWorkpool, {
  maxParallelism: 1,
});

export const doSomethingAndCount = action({
  handler: async (ctx) => {
    const doSomething = await fetch("https://example.com");
    await counterPool.enqueueMutation(internal.counter.increment, {});
  },
});

// This mutation is prone to conflicting with itself, because it always reads
// and writes the same data. By running it in a workpool with low parallelism,
// it will run serially.
export const increment = internalMutation({
  handler: async (ctx) => {
    const countDoc = await ctx.db.query("counter").unique();
    await ctx.db.patch(countDoc!._id, { count: countDoc!.count + 1 });
  },
});
```

Effectively, Workpool runs async functions similar to
`ctx.scheduler.runAfter(0, ...)`, but it limits the number of functions that
can run in parallel.

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
import { Workpool } from "@convex-dev/workpool";

const pool = new Workpool(components.emailWorkpool, { maxParallelism: 10 });
```

Then you have the following interface on `pool`:

```ts
// Schedule functions to run in the background.
const id = await pool.enqueueMutation(internal.foo.bar, args);
// Or for an action:
const id = await pool.enqueueAction(internal.foo.baz, args);

// Is it done yet? Did it succeed or fail?
const status = await pool.status(id);

// You can cancel the work, if it hasn't finished yet.
await pool.cancel(id);
```

See more example usage in [example.ts](./example/convex/example.ts).

## Optimizations with and without Workpool

The benefit of Workpool is that it won't fall over if there are many jobs
scheduled at once, and it allows you to throttle low-priority jobs.

However, Workpool has some overhead and can slow down your workload compared
to using `ctx.scheduler` directly.

Since each Workpool has some overhead -- each runs several functions to
coordinate its work -- don't create too many of them.

If you're running into issues with too many concurrent functions, there are
alternatives to Workpool:

- Try combining multiple mutations into a single mutation, with batching or
  debouncing.
- Call plain TypeScript functions if possible.
  - In particular, an action calling `ctx.runAction` has more overhead than just
    calling the action's handler directly.

See [best practices](https://docs.convex.dev/production/best-practices) for more.

## Reading function status

The workpool stores the status of each function in the database, so you can
read it even after the function has finished.
By default, it will keep the status for 1 day but you can change this with
the `statusTtl` option to `Workpool`.

To keep the status forever, set `statusTtl: Number.POSITIVE_INFINITY`.

You can read the status of a function by calling `pool.status(id)`.

The status will be one of:

- `{ kind: "pending" }`: The function has not started yet.
- `{ kind: "inProgress" }`: The function is currently running.
- `{ kind: "completed"; completionStatus: CompletionStatus }`: The function has
  finished.

The `CompletionStatus` type is one of:

- `"success"`: The function completed successfully.
- `"error"`: The function threw an error.
- `"canceled"`: The function was canceled.
- `"timeout"`: The function timed out.

<!-- END: Include on https://convex.dev/components -->
