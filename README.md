# Convex Component: Workpool

[![npm version](https://badge.fury.io/js/@convex-dev%2Fworkpool.svg)](https://badge.fury.io/js/@convex-dev%2Fworkpool)

<!-- START: Include on https://convex.dev/components -->

This Convex component pools actions and mutations to restrict parallel requests.

- Configure multiple pools with different parallelism.
- Retry failed actions (with backoff and jitter) for
  [idempotent actions](#idempotency), fully configurable (respecting parallelism).
- An `onComplete` callback so you can build durable, reliable workflows. Called
  when the work is finished, whether it succeeded, failed, or was canceled.

## Use cases

### Separating and throttling async workloads

Suppose you have some important async work, like sending verification emails,
and some less important async work, like scraping data from an API. If all of
these are scheduled with `ctx.scheduler.runAfter`, they'll compete with each
other for resources. The emails might be delayed if there are too many scraping
requests queued ahead of them.

To resolve this problem, you can separate work into different pools.

```ts
const emailPool = new Workpool(components.emailWorkpool, {
  maxParallelism: 10,
});
const scrapePool = new Workpool(components.scrapeWorkpool, {
  maxParallelism: 5,
});

export const userSignUp = mutation({
  args: {...},
  handler: async (ctx, args) => {
    const userId = await ctx.db.insert("users", args);
    await emailPool.enqueueAction(ctx, internal.auth.sendEmailVerification, {
      userId,
    });
  },
});

export const downloadLatestWeather = mutation({
  handler: async (ctx, args) => {
    for (const city of allCities) {
      await scrapePool.enqueueAction(ctx, internal.weather.scrape, { city });
    }
  },
});
```

### Durable, reliable workflows meet workpool

#### Retry management

Imagine that the payment processor is a 3rd party API, and they temporarily have an
outage. Now imagine you implement your own action retrying logic for your busy app.
You'll find very quickly that your entire backend is overwhelmed with retrying actions.
This could bog down live traffic with background work, and/or cause you to exceed
rate limits with the payment provider.

Creating an upper bound on how much work will be done in parallel is a good way to
mitigate this risk. Actions that are currently backing off awaiting retry will not tie
up a thread in the workpool.

#### Completion handling

By handing off asynchronous work, it will be guaranteed to run, and with retries
you can account for temporary failures, while avoiding a "stampeding herd"
during third party outages.

With the `onComplete` callback, you can define how to proceed after each step,
whether that enqueues another job to the workpool, updates the database, etc.
It will always be called, whether the work was successful, failed, or was
canceled. See [below](#options-for-enqueueing-work) for more info.

Example:

```ts
const pool = new Workpool(components.emailWorkpool, {
  retryActionsByDefault: true,
  defaultRetryBehavior: { maxAttempts: 3, initialBackoffMs: 1000, base: 2 },
});

const sendEmailReliablyWithRetries = mutation({
  args: {
    emailType: v.string(),
    userId: v.id("users"),
    title: v.string(),
    body: v.string(),
  },
  handler: async (ctx, args) => {
    // ... do other things in the transaction
    await pool.enqueueAction(ctx, internal.email.send, args, {
      onComplete: internal.email.emailSent,
      context: { emailType: args.emailType, userId: args.userId },
      retry: false, // don't retry this action, as we can't guarantee idempotency.
    });
  },
});

export const emailSent = internalMutation({
  args: vOnCompleteValidator(
    v.object({ emailType: v.string(), userId: v.id("users") })
  ),
  handler: async (ctx, { workId, context, result }) => {
    if (result.kind === "canceled") return;
    const emailLogId = await ctx.db.insert("userEmailLog", {
      userId: context.userId,
      emailType: context.emailType,
      result: result.kind === "success" ? result.returnValue : null,
      error: result.kind === "failed" ? result.error : null,
    });
    if (result.kind === "failed") {
      await pool.enqueueAction(ctx, internal.email.checkResendStatus, context, {
        retry: { maxAttempts: 10, initialBackoffMs: 250, base: 2 }, // custom
        onComplete: internal.email.handleEmailStatus,
        context: { emailLogId },
      });
    }
  },
});
```

Note: the `onComplete` handler runs in a different transaction than the job
enqueued. If you want to run it in the same transaction, you can do that work
at the end of the enqueued function, before returning. This is generally faster
and more typesafe when handling the "success" case.

You can also use this equivalent helper to define an `onComplete` mutation.
Note the `DataModel` type parameter, if you want ctx.db to be type safe.

```ts
export const emailSent = pool.defineOnComplete<DataModel>({
  context: v.object({ emailType: v.string(), userId: v.id("users") }),
  handler: async (ctx, { workId, context, result }) => {
    // ...
  },
});
```

#### Idempotency?

Idempotent actions are actions that can be run multiple times safely. This typically
means they don't cause any side effects that would be a problem if executed twice or more.

As an example of an unsafe, non-idempotent action, consider an action that charges
a user's credit card without providing a unique transaction id to the payment
processor. The first time the action is run, imagine that the API call succeeds to the
payment provider, but then the action throws an exception before the transaction is marked
finished in our Convex database. If the action is run twice, the user may be
double charged for the transaction!

If we alter this action to provide a consistent transaction id to the payment provider, they
can simply NOOP the second payment attempt. The this makes the action idempotent, and
it can safely be retried.

If you're creating complex workflows with many steps involving 3rd party APIs:

1.  You should ensure that each step is an idempotent Convex action.
2.  You should use this component to manage these actions so it all just works!

### Optimize OCC errors

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
    await counterPool.enqueueMutation(ctx, internal.counter.increment, {});
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
import { vWorkIdValidator } from "@convex-dev/workpool";

export const myMutation = mutation({
  args: {},
  handler: async (ctx, args) => {
    // Schedule functions to run in the background.
    const id = await pool.enqueueMutation(ctx, internal.foo.bar, args);
    // Or for an action:
    const id = await pool.enqueueAction(ctx, internal.foo.baz, args);
  },
});

export const getStatus = query({
  args: { id: vWorkIdValidator },
  handler: async (ctx, args) => {
    // Is it done yet? Did it succeed or fail?
    const status = await pool.status(args.id);
    return status;
  },
});

export const cancelWork = mutation({
  args: { id: vWorkIdValidator },
  handler: async (ctx, args) => {
    // You can cancel the work, if it hasn't finished yet.
    await pool.cancel(ctx, args.id);
  },
});
```

See more example usage in [example.ts](./example/convex/example.ts).

### Configuring the Workpool

Check out the [docstrings](./src/client/index.ts), but notable options include:

- `maxParallelism`: How many actions/mutations can run at once within this pool.
  Avoid exceeding 100 on Pro, 20 on the free plan, across all workpools and workflows.
- `retryActionsByDefault`: Whether to retry actions that fail by default.
- `defaultRetryBehavior`: The default retry behavior for enqueued actions.

You can override the retry behavior per-call with the `retry` option.

### Options for enqueueing work

See the [docstrings](./src/client/index.ts) for more details, but notable options include:

- `retry`: Whether to retry the action if it fails. Overrides defaults.
  If it's set to `true`, it will use the `defaultRetryBehavior`.
  If it's set to a custom config, it will use that (and do retries).
- `onComplete`: A mutation to run after the function finishes.
- `context`: Any data you want to pass to the `onComplete` mutation.
- `runAt` and `runAfter`: Similar to `ctx.scheduler.run*`, allows you to
  schedule the work to run later. By default it's immediate.

### Retry behavior

The retry options work like this:

- The first request runs as it's scheduled.
- If it fails, it will wait _around_ `initialBackoffMs` and then try again.
- Each subsequent retry waits `initialBackoffMs * base^<retryNumber - 1>`.
- The standard base is 2.
- The actual wait time uses "jitter" to avoid all retries happening at once
  if they all fail at the same time.

You can override the retry behavior per-call with the `retry` option.

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

- `{ kind: "pending"; previousAttempts: number }`: The function has not started yet.
- `{ kind: "running"; previousAttempts: number }`: The function is currently running.
- `{ kind: "finished" }`: The function has succeeded, failed, or been canceled.

To get the result of your function, you can either write to the database from
within your function, call or schedule another function from there, or use the
`onComplete` handler to respond to the job result.

## Canceling work

You can cancel work by calling `pool.cancel(id)` or all of them with
`pool.cancelAll()`.

This will avoid starting or retrying, but will not stop in-progress work.

## Monitoring the workpool

If you want to know the status of your workpool, here are some queries to use
for [Axiom](https://axiom.co/docs/send-data/convex).
Just replace `your-dataset` with your dataset's name (which is also
what you enter in the log streaming configuration in the Convex dashboard).

Note: these are optimized for monitors. For dashboards, you might want to change
`bin(_time, X)` to `bin_auto(_time)`.

### Is it backlogged

Reports the current backlog length, where "backlog" is tasks that are past due,
not including tasks that have been scheduled for the future. This reports the
max for 1 minute intervals (which is roughly how often the report is generated).

```txt
['your-dataset']
| extend parsed_message = iff(isnotnull(parse_json(trim("'", tostring(["data.message"])))),
  parse_json(trim("'", tostring(["data.message"]))),
  parse_json('{}') )
| where parsed_message["component"] == "workpool" and parsed_message["event"] == "report"
| summarize max_backlog = max(toint(parsed_message["backlog"]))
  by bin(_time, 1m), workpool = tostring(["data.function.component_path"])
```

### Are functions failing (after retries)

Reports the overall average failure rate per registered workpool in 5 minute intervals.

```txt
['your-dataset']
| extend parsed_message = iff(isnotnull(parse_json(trim("'", tostring(["data.message"])))),
  parse_json(trim("'", tostring(["data.message"]))),
  parse_json('{}') )
| where parsed_message["component"] == "workpool" and parsed_message["event"] == "report"
| extend permanentFailureRate = parsed_message["permanentFailureRate"]
| summarize avg(todouble(permanentFailureRate))
  by bin(\_time, 5m), workpool = tostring(["data.function.component_path"])
```

### Are functions retrying a lot

Reports the ratio (0 to 1) of failures per function, in 5 minute intervals.
Note: to get this data, set the workpool `logLevel` to `"INFO"` (or `"DEBUG"`).

```txt
['your-dataset']
| extend parsed_message = iff( 	isnotnull(parse_json(trim("'", tostring(["data.message"])))),
  parse_json(trim("'", tostring(["data.message"]))),
  parse_json('{}') )
| where parsed_message["component"] == "workpool" and (parsed_message["event"] == "completed") and parsed_message["status"] != "canceled"
| summarize failure_ratio = avg(iff(parsed_message["status"] != "success", 1, 0))
  by bin(_time, 5m), function = tostring(parsed_message["fnName"])
```

### Is there a big delay between being enqueued and starting

Reports the average time between enqueueing work and it actually starting.
Note: to get this data, set the workpool `logLevel` to `"INFO"` (or `"DEBUG"`).

```txt
['your-dataset']
| extend parsed_message = iff(isnotnull(parse_json(trim("'", tostring(["data.message"])))),
  parse_json(trim("'", tostring(["data.message"]))),
  parse_json('{}') )
| where parsed_message["component"] == "workpool" and parsed_message["event"] == "started"
| summarize start_lag_seconds = avg(todouble(parsed_message["startLag"])/1000)
  by bin(_time, 1m), function = tostring(parsed_message["fnName"])
```

While similar to the backlog size, this is a more concrete value, since the
events in the backlog may take variable amounts of time. This is a more user-
visible metric, though it is a "lagging" indicator - this will be high when the
backlog was large enough to delay the processing of an entry. So alerting on
the backlog size will give you a faster indicator, while this is a metric of the
severity of the incident.

<!-- END: Include on https://convex.dev/components -->
