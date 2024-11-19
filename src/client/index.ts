/* eslint-disable @typescript-eslint/no-explicit-any */
import {
  createFunctionHandle,
  DefaultFunctionArgs,
  Expand,
  FunctionReference,
  FunctionVisibility,
  GenericActionCtx,
  GenericDataModel,
  GenericMutationCtx,
  GenericQueryCtx,
  getFunctionName,
} from "convex/server";
import { GenericId } from "convex/values";
import { api } from "../component/_generated/api";

export class WorkPool {
  constructor(
    private component: UseApi<typeof api>,
    private workers: number
  ) {}
  async enqueue<Args extends DefaultFunctionArgs>(
    ctx: RunMutationCtx,
    fn: FunctionReference<
      "action" | "mutation",
      FunctionVisibility,
      Args,
      null
    >,
    fnArgs: Args
  ): Promise<string> {
    const fnHandle = await createFunctionHandle(fn);
    return await ctx.runMutation(this.component.lib.enqueue, {
      fnHandle,
      fnName: getFunctionName(fn),
      fnArgs,
      // XXX we should be able to infer this from the function reference
      fnType: "mutation",
      workers: this.workers,
    });
  }
  async cancel(ctx: RunMutationCtx, id: string): Promise<void> {
    await ctx.runMutation(this.component.lib.cancel, { id });
  }
  async status(
    ctx: RunQueryCtx,
    id: string
  ): Promise<
    | { kind: "pending" }
    | { kind: "inProgress" }
    | { kind: "success" }
    | { kind: "error"; error: string }
  > {
    return await ctx.runQuery(this.component.lib.status, { id });
  }
}

/* Type utils follow */

type RunQueryCtx = {
  runQuery: GenericQueryCtx<GenericDataModel>["runQuery"];
};
type RunMutationCtx = {
  runMutation: GenericMutationCtx<GenericDataModel>["runMutation"];
};
type RunActionCtx = {
  runAction: GenericActionCtx<GenericDataModel>["runAction"];
};

export type OpaqueIds<T> =
  T extends GenericId<infer _T>
    ? string
    : T extends (infer U)[]
      ? OpaqueIds<U>[]
      : T extends object
        ? { [K in keyof T]: OpaqueIds<T[K]> }
        : T;

export type UseApi<API> = Expand<{
  [mod in keyof API]: API[mod] extends FunctionReference<
    infer FType,
    "public",
    infer FArgs,
    infer FReturnType,
    infer FComponentPath
  >
    ? FunctionReference<
        FType,
        "internal",
        OpaqueIds<FArgs>,
        OpaqueIds<FReturnType>,
        FComponentPath
      >
    : UseApi<API[mod]>;
}>;
