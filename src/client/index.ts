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
} from "convex/server";
import { GenericId } from "convex/values";
import { api } from "../component/_generated/api";

type WorkId<ReturnType> = string & { __returnType: ReturnType };

export class WorkPool {
  constructor(
    private component: UseApi<typeof api>,
    private options: {
      maxParallelism: number,
    }
  ) {}
  async enqueueAction<Args extends DefaultFunctionArgs, ReturnType>(
    ctx: RunMutationCtx,
    fn: FunctionReference<'action', FunctionVisibility, Args, ReturnType>,
    fnArgs: Args,
  ): Promise<WorkId<ReturnType>> {
    const handle = await createFunctionHandle(fn);
    const id = await ctx.runMutation(this.component.public.enqueue, {
      handle,
      maxParallelism: this.options.maxParallelism,
      fnArgs,
      fnType: "action",
    });
    return id as WorkId<ReturnType>;
  }
  async enqueueMutation<Args extends DefaultFunctionArgs, ReturnType>(
    ctx: RunMutationCtx,
    fn: FunctionReference<'mutation', FunctionVisibility, Args, ReturnType>,
    fnArgs: Args,
  ): Promise<WorkId<ReturnType>> {
    const handle = await createFunctionHandle(fn);
    const id = await ctx.runMutation(this.component.public.enqueue, {
      handle,
      maxParallelism: this.options.maxParallelism,
      fnArgs,
      fnType: "mutation",
    });
    return id as WorkId<ReturnType>;
  }
  async tryResult<ReturnType>(
    ctx: RunQueryCtx,
    id: WorkId<ReturnType>,
  ): Promise<ReturnType | undefined> {
    const { result } = await ctx.runQuery(this.component.public.result, { id });
    return result;
  }
  async pollResult<ReturnType>(
    ctx: RunQueryCtx & RunActionCtx,
    id: WorkId<ReturnType>,
    timeoutMs: number,
  ): Promise<ReturnType> {
    const start = Date.now();
    while (true) {
      const { result } = await ctx.runQuery(this.component.public.result, { id });
      if (result !== undefined) {
        return result;
      }
      if (Date.now() - start > timeoutMs) {
        throw new Error(`Timeout waiting for result of work ${id}`);
      }
      await new Promise<void>((resolve) => setTimeout(resolve, 50));
    }
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
