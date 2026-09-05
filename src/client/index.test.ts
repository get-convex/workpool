import {
  makeFunctionReference,
  type GenericDataModel,
  type GenericMutationCtx,
} from "convex/server";
import { type Infer, v } from "convex/values";
import { expectTypeOf, test } from "vitest";
import {
  type EnqueueOptions,
  type OnCompleteArgs,
  type WorkId,
  type Workpool,
  vOnCompleteArgs,
} from "./index.js";

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
