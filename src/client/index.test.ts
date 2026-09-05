import {
  anyApi,
  componentsGeneric,
  defineSchema,
  defineTable,
  internalActionGeneric,
  internalMutationGeneric,
  internalQueryGeneric,
  makeFunctionReference,
  type ApiFromModules,
  type DataModelFromSchemaDefinition,
  type FunctionArgs,
  type GenericDataModel,
  type GenericMutationCtx,
} from "convex/server";
import { type Infer, type ObjectType, v } from "convex/values";
import { convexTest } from "convex-test";
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  expectTypeOf,
  test,
  vi,
} from "vitest";
import workpool from "../test.js";
import type { api } from "../component/_generated/api.js";
import {
  type EnqueueOptions,
  NonRetryableError,
  type OnCompleteArgs,
  type RetryOption,
  Workpool,
  type WorkId,
  type WorkpoolComponent,
  vOnCompleteArgs,
  vResult,
  vWorkId,
} from "./index.js";

const schema = defineSchema({
  events: defineTable({
    key: v.string(),
    kind: v.union(
      v.literal("attempt"),
      v.literal("work"),
      v.literal("callback"),
    ),
    workId: v.optional(vWorkId),
    result: v.optional(vResult),
    context: v.optional(v.any()),
  }).index("key", ["key"]),
});
const runArgs = {
  key: v.string(),
  fail: v.optional(v.boolean()),
  failures: v.optional(v.number()),
  nonRetryable: v.optional(v.boolean()),
  payload: v.optional(v.string()),
};
type RunArgs = ObjectType<typeof runArgs>;
type Context = { key: string; padding?: string; throw?: boolean };

// Observe real callback execution while retaining Convex validation and rollback.
const callback = vi.fn(
  async (
    ctx: GenericMutationCtx<DataModelFromSchemaDefinition<typeof schema>>,
    args: OnCompleteArgs<Context | undefined> & { workId: WorkId },
  ) => {
    await ctx.db.insert("events", {
      key: args.context?.key ?? "default",
      kind: "callback",
      workId: args.workId,
      context: args.context,
      result: args.result,
    });
    if (args.context?.throw) {
      throw new Error("callback failed");
    }
    return null;
  },
);
const fixtures = {
  attempt: internalMutationGeneric({
    args: { key: v.string() },
    returns: v.number(),
    handler: async (ctx, { key }) => {
      await ctx.db.insert("events", { key, kind: "attempt" });
      const events = await ctx.db
        .query("events")
        .withIndex("key", (q) => q.eq("key", key))
        .collect();
      return events.filter((e) => e.kind === "attempt").length;
    },
  }),
  action: internalActionGeneric({
    args: runArgs,
    returns: v.string(),
    handler: async (ctx, args): Promise<string> => {
      const attempt = await ctx.runMutation(refs.attempt, { key: args.key });
      if (args.fail || attempt <= (args.failures ?? 0)) {
        if (args.nonRetryable) throw new NonRetryableError("work failed");
        throw new Error("work failed");
      }
      return "action result";
    },
  }),
  mutation: internalMutationGeneric({
    args: runArgs,
    returns: v.string(),
    handler: async (ctx, args) => {
      await ctx.db.insert("events", { key: args.key, kind: "work" });
      if (args.fail) throw new Error("work failed");
      return "mutation result";
    },
  }),
  query: internalQueryGeneric({
    args: runArgs,
    returns: v.string(),
    handler: async (_ctx, args) => {
      if (args.fail) throw new Error("work failed");
      return "query result";
    },
  }),
  complete: internalMutationGeneric({
    args: vOnCompleteArgs(
      v.object({
        key: v.string(),
        padding: v.optional(v.string()),
        throw: v.optional(v.boolean()),
      }),
    ),
    returns: v.null(),
    handler: callback,
  }),
  optionalComplete: internalMutationGeneric({
    args: vOnCompleteArgs(),
    returns: v.null(),
    handler: callback,
  }),
};
const refs = (
  anyApi as unknown as ApiFromModules<{ callbacks: typeof fixtures }>
).callbacks;
// An application with real callback functions and a nested workpool component.
// The generated path provides the module root expected by convex-test.
const modules = {
  "./_generated/server.ts": async () => ({}),
  "./callbacks.ts": async () => fixtures,
};
const component = componentsGeneric().workpool as unknown as WorkpoolComponent;
const pool = new Workpool(component, { maxParallelism: 3, logLevel: "ERROR" });
const retries = { maxAttempts: 3, initialBackoffMs: 1, base: 2 };
type Kind = "action" | "mutation" | "query";
type Mode = "onComplete" | "onFailure";

describe("completion callbacks through the client and scheduler", () => {
  let t: ReturnType<typeof setup>;
  function setup() {
    const t = convexTest(schema, modules);
    workpool.register(t);
    return t;
  }
  beforeEach(() => {
    vi.useFakeTimers();
    callback.mockClear();
    t = setup();
  });
  afterEach(async () => {
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    vi.useRealTimers();
  });
  const drain = () => t.finishAllScheduledFunctions(vi.runAllTimers);
  const events = (key: string) =>
    t.query((ctx) =>
      ctx.db
        .query("events")
        .withIndex("key", (q) => q.eq("key", key))
        .collect(),
    );
  function enqueue(
    kind: Kind,
    args: RunArgs,
    options: {
      mode?: Mode;
      context?: Context;
      retry?: RetryOption["retry"];
      runAt?: number;
    } = {},
  ) {
    const {
      mode = "onFailure",
      context = { key: args.key },
      ...rest
    } = options;
    const opts =
      mode === "onFailure"
        ? { onFailure: refs.complete, context, ...rest }
        : { onComplete: refs.complete, context, ...rest };
    return t.mutation((ctx) => {
      switch (kind) {
        case "action":
          return pool.enqueueAction(ctx, refs.action, args, opts);
        case "mutation":
          return pool.enqueueMutation(ctx, refs.mutation, args, opts);
        case "query":
          return pool.enqueueQuery(ctx, refs.query, args, opts);
      }
    });
  }

  test.each(["action", "mutation", "query"] as const)(
    "onFailure skips successful %s work",
    async (kind) => {
      const id = await enqueue(kind, { key: kind });
      await drain();
      expect(callback).not.toHaveBeenCalled();
      expect(await t.query((ctx) => pool.status(ctx, id))).toEqual({
        state: "finished",
      });
    },
  );

  test.each(["action", "mutation", "query"] as const)(
    "onFailure reuses an existing completion handler for failed %s work",
    async (kind) => {
      const id = await enqueue(kind, { key: kind, fail: true });
      await drain();
      expect(callback).toHaveBeenCalledTimes(1);
      expect(
        (await events(kind)).filter((e) => e.kind === "callback"),
      ).toMatchObject([
        {
          workId: id,
          context: { key: kind },
          result: { kind: "failed", error: "work failed" },
        },
      ]);
      expect(await t.query((ctx) => pool.status(ctx, id))).toEqual({
        state: "finished",
      });
      if (kind === "mutation") {
        expect((await events(kind)).some((e) => e.kind === "work")).toBe(false);
      }
    },
  );

  test("onFailure runs once after retry exhaustion", async () => {
    await enqueue("action", { key: "retry", fail: true }, { retry: retries });
    await drain();
    expect(callback).toHaveBeenCalledTimes(1);
    expect((await events("retry")).map((e) => e.kind)).toEqual([
      "attempt",
      "attempt",
      "attempt",
      "callback",
    ]);
  });

  test("onFailure is skipped if a retry succeeds", async () => {
    await enqueue("action", { key: "retry", failures: 2 }, { retry: retries });
    await drain();
    expect(callback).not.toHaveBeenCalled();
    expect((await events("retry")).map((e) => e.kind)).toEqual([
      "attempt",
      "attempt",
      "attempt",
    ]);
  });

  test("NonRetryableError skips remaining attempts and invokes onFailure", async () => {
    await enqueue(
      "action",
      { key: "terminal", fail: true, nonRetryable: true },
      { retry: retries },
    );
    await drain();
    expect(callback).toHaveBeenCalledTimes(1);
    expect((await events("terminal")).map((e) => e.kind)).toEqual([
      "attempt",
      "callback",
    ]);
    expect((await events("terminal"))[1].result).toEqual({
      kind: "failed",
      error: "work failed",
    });
  });

  test.each(["onComplete", "onFailure"] as const)(
    "%s preserves cancellation semantics",
    async (mode) => {
      const id = await enqueue(
        "action",
        { key: "cancel", fail: true },
        { mode, runAt: Date.now() + 60_000 },
      );
      await t.mutation((ctx) => pool.cancel(ctx, id));
      await drain();
      const recorded = await events("cancel");
      if (mode === "onFailure") {
        expect(callback).not.toHaveBeenCalled();
        expect(recorded).toEqual([]);
      } else {
        expect(callback).toHaveBeenCalledTimes(1);
        expect(recorded).toMatchObject([
          { workId: id, kind: "callback", result: { kind: "canceled" } },
        ]);
      }
      expect(await t.query((ctx) => pool.status(ctx, id))).toEqual({
        state: "finished",
      });
    },
  );

  test.each(["action", "mutation", "query"] as const)(
    "onComplete still receives successful %s results",
    async (kind) => {
      const id = await enqueue(kind, { key: kind }, { mode: "onComplete" });
      await drain();
      expect(callback).toHaveBeenCalledTimes(1);
      expect(
        (await events(kind)).filter((e) => e.kind === "callback"),
      ).toMatchObject([
        {
          workId: id,
          result: { kind: "success", returnValue: `${kind} result` },
        },
      ]);
    },
  );

  test.each([false, true])(
    "onFailure restores large context (large arguments: %s)",
    async (largeArgs) => {
      const context = { key: "large", padding: "x".repeat(12_000) };
      const id = await enqueue(
        "action",
        {
          key: "large",
          fail: true,
          ...(largeArgs ? { payload: "x".repeat(12_000) } : {}),
        },
        { context },
      );
      await drain();
      expect(callback).toHaveBeenCalledTimes(1);
      const recorded = await events("large");
      expect(recorded.map((e) => e.kind)).toEqual(["attempt", "callback"]);
      expect(recorded[1]).toMatchObject({
        context,
        result: { kind: "failed", error: "work failed" },
      });
      expect(await t.query((ctx) => pool.status(ctx, id))).toEqual({
        state: "finished",
      });
    },
  );

  test("onFailure supports omitted context", async () => {
    const id = await t.mutation((ctx) =>
      pool.enqueueMutation(
        ctx,
        refs.mutation,
        { key: "default", fail: true },
        { onFailure: refs.optionalComplete },
      ),
    );
    await drain();
    expect(callback).toHaveBeenCalledTimes(1);
    expect((await events("default"))[0]).toMatchObject({
      workId: id,
      result: { kind: "failed" },
    });
    expect((await events("default"))[0].context).toBeUndefined();
  });

  test.each(["action", "mutation", "query"] as const)(
    "batch %s enqueue only calls back for failed items",
    async (kind) => {
      const args = [{ key: "success" }, { key: "failure", fail: true }];
      const opts = { onFailure: refs.complete, context: { key: "batch" } };
      const ids = await t.mutation((ctx) => {
        switch (kind) {
          case "action":
            return pool.enqueueActionBatch(ctx, refs.action, args, opts);
          case "mutation":
            return pool.enqueueMutationBatch(ctx, refs.mutation, args, opts);
          case "query":
            return pool.enqueueQueryBatch(ctx, refs.query, args, opts);
        }
      });
      await drain();
      expect(callback).toHaveBeenCalledTimes(1);
      expect(await events("batch")).toMatchObject([
        { workId: ids[1], result: { kind: "failed" } },
      ]);
      expect(await t.query((ctx) => pool.statusBatch(ctx, ids))).toEqual([
        { state: "finished" },
        { state: "finished" },
      ]);
    },
  );

  test("a failing onFailure callback rolls back without retrying the original work", async () => {
    const id = await enqueue(
      "action",
      { key: "broken", fail: true },
      { retry: retries, context: { key: "broken", throw: true } },
    );
    await drain();
    expect(callback).toHaveBeenCalledTimes(1);
    expect((await events("broken")).map((e) => e.kind)).toEqual([
      "attempt",
      "attempt",
      "attempt",
    ]);
    expect(await t.query((ctx) => pool.status(ctx, id))).toEqual({
      state: "finished",
    });
  });

  test.each([false, true])(
    "rejects mixed completion options at runtime (batch: %s)",
    async (batch) => {
      // JavaScript callers can bypass TypeScript's mutually exclusive options.
      const opts = {
        onComplete: refs.complete,
        onFailure: refs.complete,
        context: { key: "invalid" },
      } as unknown as EnqueueOptions<Context>;
      await expect(
        t.mutation((ctx) =>
          batch
            ? pool.enqueueMutationBatch(
                ctx,
                refs.mutation,
                [{ key: "invalid" }],
                opts,
              )
            : pool.enqueueMutation(
                ctx,
                refs.mutation,
                { key: "invalid" },
                opts,
              ),
        ),
      ).rejects.toThrow("Cannot define both onComplete and onFailure");
      await drain();
      expect(await events("invalid")).toEqual([]);
      expect(callback).not.toHaveBeenCalled();
    },
  );
});

type MutationCtx = GenericMutationCtx<GenericDataModel>;
const action = makeFunctionReference<"action", { value: number }, number>(
  "work:action",
);
const onComplete = makeFunctionReference<
  "mutation",
  OnCompleteArgs<{ label: string }, number>
>("work:onComplete");

test("onComplete retains context and return value types", () => {
  // tsc checks these call sites; the function is never invoked at runtime.
  expectTypeOf((pool: Workpool, ctx: MutationCtx) => {
    void pool.enqueueAction(
      ctx,
      action,
      { value: 1 },
      // @ts-expect-error The callback requires a string label.
      { onComplete, context: { label: 1 } },
    );
    const stringAction = makeFunctionReference<
      "action",
      { value: number },
      string
    >("work:stringAction");
    void pool.enqueueAction(
      ctx,
      // @ts-expect-error The callback requires a number result.
      stringAction,
      { value: 1 },
      { onComplete, context: { label: "job" } },
    );
    return pool.enqueueAction(
      ctx,
      action,
      { value: 1 },
      { onComplete, context: { label: "job" } },
    );
  }).returns.toEqualTypeOf<Promise<WorkId>>();
});

test("completion validators preserve the declared context and result", () => {
  const _args = vOnCompleteArgs(v.object({ label: v.string() }), v.number());
  type Args = Infer<typeof _args>;
  expectTypeOf<Args["context"]>().toEqualTypeOf<{ label: string }>();
  expectTypeOf<
    Extract<Args["result"], { kind: "success" }>["returnValue"]
  >().toEqualTypeOf<number>();
});

test("runAt and runAfter are mutually exclusive", () => {
  // @ts-expect-error Only one scheduling option can be specified.
  const options: EnqueueOptions = { runAt: 1, runAfter: 2 };
  expectTypeOf(options).toMatchTypeOf<EnqueueOptions>();
});

test("onFailure accepts existing onComplete handlers on all enqueue methods", () => {
  const options = { onFailure: onComplete, context: { label: "job" } };
  const mutation = makeFunctionReference<"mutation", { value: number }, number>(
    "work:mutation",
  );
  const query = makeFunctionReference<"query", { value: number }, number>(
    "work:query",
  );
  expectTypeOf((pool: Workpool, ctx: MutationCtx) =>
    pool.enqueueAction(ctx, action, { value: 1 }, options),
  ).returns.toEqualTypeOf<Promise<WorkId>>();
  expectTypeOf((pool: Workpool, ctx: MutationCtx) =>
    pool.enqueueMutation(ctx, mutation, { value: 1 }, options),
  ).returns.toEqualTypeOf<Promise<WorkId>>();
  expectTypeOf((pool: Workpool, ctx: MutationCtx) =>
    pool.enqueueQuery(ctx, query, { value: 1 }, options),
  ).returns.toEqualTypeOf<Promise<WorkId>>();
  expectTypeOf((pool: Workpool, ctx: MutationCtx) =>
    pool.enqueueActionBatch(ctx, action, [{ value: 1 }], options),
  ).returns.toEqualTypeOf<Promise<WorkId[]>>();
  expectTypeOf((pool: Workpool, ctx: MutationCtx) =>
    pool.enqueueMutationBatch(ctx, mutation, [{ value: 1 }], options),
  ).returns.toEqualTypeOf<Promise<WorkId[]>>();
  expectTypeOf((pool: Workpool, ctx: MutationCtx) =>
    pool.enqueueQueryBatch(ctx, query, [{ value: 1 }], options),
  ).returns.toEqualTypeOf<Promise<WorkId[]>>();
});

test("onFailure preserves context typing", () => {
  expectTypeOf((pool: Workpool, ctx: MutationCtx) => {
    void pool.enqueueAction(
      ctx,
      action,
      { value: 1 },
      {
        onFailure: onComplete,
        // @ts-expect-error The reused completion handler requires a string label.
        context: { label: 1 },
      },
    );
  }).returns.toBeVoid();
});

test("onComplete and onFailure are mutually exclusive", () => {
  // @ts-expect-error The completion options cannot be combined.
  const options: EnqueueOptions = { onComplete, onFailure: onComplete };
  expectTypeOf(options).toMatchTypeOf<EnqueueOptions>();
});

test("callback options can each be disabled with null", () => {
  expectTypeOf({ onComplete: null }).toMatchTypeOf<EnqueueOptions>();
  expectTypeOf({ onFailure: null }).toMatchTypeOf<EnqueueOptions>();
});

test("generated enqueue types match the component validators", () => {
  expectTypeOf<
    FunctionArgs<WorkpoolComponent["lib"]["enqueue"]>
  >().toEqualTypeOf<FunctionArgs<typeof api.lib.enqueue>>();
  expectTypeOf<
    FunctionArgs<WorkpoolComponent["lib"]["enqueueBatch"]>
  >().toEqualTypeOf<FunctionArgs<typeof api.lib.enqueueBatch>>();
});
