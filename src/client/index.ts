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
} from "convex/server";
import { GenericId } from "convex/values";
import { api } from "../component/_generated/api";

export type WorkId<ReturnType> = string & { __returnType: ReturnType };

export class WorkPool {
  constructor(
    private component: UseApi<typeof api>,
    private options: {
      maxParallelism: number,
      actionTimeoutMs?: number,
      mutationTimeoutMs?: number,
      unknownTimeoutMs?: number,
      // When there is something to do, wait this long between loop iterations,
      // to allow more work to accumulate.
      debounceMs?: number,
      // When something is happening, wait this long to check if anything has
      // been canceled or failed unexpectedly.
      fastHeartbeatMs?: number,
      // When nothing is happening, wait this long to check if there is new work.
      slowHeartbeatMs?: number,
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
      options: this.options,
      fnArgs,
      fnType: "action",
      runAtTime: Date.now(),
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
      options: this.options,
      fnArgs,
      fnType: "mutation",
      runAtTime: Date.now(),
    });
    return id as WorkId<ReturnType>;
  }
  // Unknown is if you don't know at runtime whether it's an action or mutation,
  // which can happen if it comes from `runAt` or `runAfter`.
  async enqueueUnknown<Args extends DefaultFunctionArgs>(
    ctx: RunMutationCtx,
    fn: FunctionReference<'action' | 'mutation', FunctionVisibility, Args, null>,
    fnArgs: Args,
    runAtTime: number,
  ): Promise<WorkId<null>> {
    const handle = await createFunctionHandle(fn);
    const id = await ctx.runMutation(this.component.public.enqueue, {
      handle,
      options: this.options,
      fnArgs,
      fnType: 'unknown',
      runAtTime,
    });
    return id as WorkId<null>;
  }
  async cancel(ctx: RunMutationCtx, id: WorkId<any>): Promise<void> {
    await ctx.runMutation(this.component.public.cancel, { id });
  }
  async status<ReturnType>(
    ctx: RunQueryCtx,
    id: WorkId<ReturnType>,
  ): Promise<
    { kind: "pending" } | { kind: "inProgress" } | { kind: "success", result: ReturnType } | { kind: "error", error: string }
  > {
    return await ctx.runQuery(this.component.public.status, { id });
  }
  async tryResult<ReturnType>(
    ctx: RunQueryCtx,
    id: WorkId<ReturnType>,
  ): Promise<ReturnType | undefined> {
    const status = await this.status(ctx, id);
    if (status.kind === "success") {
      return status.result;
    }
    if (status.kind === "error") {
      throw new Error(status.error);
    }
    return undefined;
  }
  async pollResult<ReturnType>(
    ctx: RunQueryCtx & RunActionCtx,
    id: WorkId<ReturnType>,
    timeoutMs: number,
  ): Promise<ReturnType> {
    const start = Date.now();
    while (true) {
      const result = await this.tryResult(ctx, id);
      if (result !== undefined) {
        return result;
      }
      if (Date.now() - start > timeoutMs) {
        throw new Error(`Timeout waiting for result of work ${id}`);
      }
      await new Promise<void>((resolve) => setTimeout(resolve, 50));
    }
  }
  ctx<DataModel extends GenericDataModel>(
    ctx: GenericActionCtx<DataModel>,
  ): GenericActionCtx<DataModel> {
    return {
      runAction: (async (action: any, args: any) => {
        const workId = await this.enqueueAction(ctx, action, args);
        return this.pollResult(ctx, workId, 30*1000);
      }) as any,
      runMutation: (async (mutation: any, args: any) => {
        const workId = await this.enqueueMutation(ctx, mutation, args);
        return this.pollResult(ctx, workId, 30*1000);
      }) as any,
      scheduler: {
        runAfter: async (delay: number, fn: any, args: any) => {
          await this.enqueueUnknown(ctx, fn, args, Date.now() + delay);
        },
        runAt: async (time: number, fn: any, args: any) => {
          await this.enqueueUnknown(ctx, fn, args, time);
        },
        cancel: async (id: any) => {
          await this.cancel(ctx, id);
        }
      } as any,
      auth: ctx.auth,
      storage: ctx.storage,
      vectorSearch: ctx.vectorSearch.bind(ctx),
      runQuery: ctx.runQuery.bind(ctx),
    };
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
