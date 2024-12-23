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
import { LogLevel } from "../component/logging";

export type WorkId<ReturnType> = string & { __returnType: ReturnType };

export class WorkPool {
  constructor(
    private component: UseApi<typeof api>,
    // TODO(emma) reduce the number of options. consider removing the timeout options.
    // consider removing the debounceMs option and the heartbeats.
    private options: {
      /** How many actions/mutations can be running at once within this pool.
       * Min 1, Max 300.
       */
      maxParallelism: number;
      /** How long an action can run before the pool considers it to be timed out.
       * The action itself might time out earlier.
       * Default 15 minutes.
       */
      actionTimeoutMs?: number;
      /** How long a mutation can run before the pool considers it to be timed out.
       * The mutation itself might time out earlier.
       * Default 30 seconds.
       */
      mutationTimeoutMs?: number;
      /** How long a function started by `enqueueUnknown` or `runAt` or `runAfter`
       * can run before the pool considers it to be timed out.
       * The function itself might time out earlier.
       * Default 15 minutes.
       */
      unknownTimeoutMs?: number;
      /** When there is something to do, wait this long between loop iterations,
       * to allow more work to accumulate.
       * Default 50ms.
       */
      debounceMs?: number;
      /** When something is running, wait this long to check if anything has
       * been canceled or failed unexpectedly.
       * Default 10s.
       */
      fastHeartbeatMs?: number;
      /** When nothing is happening, wait this long to check if there is new work
       * that we missed.
       * Default 2 hours.
       */
      slowHeartbeatMs?: number;
      /** How much to log.
       * Default WARN.
       */
      logLevel?: LogLevel;
      /** How long to keep completed work in the database, for access by `status`,
       * `tryResult`, and `pollResult`.
       * Default 1 day.
       */
      ttl?: number;
    }
  ) {}
  async enqueueAction<Args extends DefaultFunctionArgs, ReturnType>(
    ctx: RunMutationCtx,
    fn: FunctionReference<"action", FunctionVisibility, Args, ReturnType>,
    fnArgs: Args
  ): Promise<WorkId<ReturnType>> {
    const fnHandle = await createFunctionHandle(fn);
    const id = await ctx.runMutation(this.component.lib.enqueue, {
      fnHandle,
      fnName: getFunctionName(fn),
      fnArgs,
      fnType: "action",
      runAtTime: Date.now(),
      options: this.options,
    });
    return id as WorkId<ReturnType>;
  }
  async enqueueMutation<Args extends DefaultFunctionArgs, ReturnType>(
    ctx: RunMutationCtx,
    fn: FunctionReference<"mutation", FunctionVisibility, Args, ReturnType>,
    fnArgs: Args
  ): Promise<WorkId<ReturnType>> {
    const fnHandle = await createFunctionHandle(fn);
    const id = await ctx.runMutation(this.component.lib.enqueue, {
      fnHandle,
      fnName: getFunctionName(fn),
      fnArgs,
      fnType: "mutation",
      runAtTime: Date.now(),
      options: this.options,
    });
    return id as WorkId<ReturnType>;
  }
  // Unknown is if you don't know at runtime whether it's an action or mutation,
  // which can happen if it comes from `runAt` or `runAfter`.
  async enqueueUnknown<Args extends DefaultFunctionArgs>(
    ctx: RunMutationCtx,
    fn: FunctionReference<
      "action" | "mutation",
      FunctionVisibility,
      Args,
      null
    >,
    fnArgs: Args,
    runAtTime: number
  ): Promise<WorkId<null>> {
    const fnHandle = await createFunctionHandle(fn);
    const id = await ctx.runMutation(this.component.lib.enqueue, {
      fnHandle,
      fnName: getFunctionName(fn),
      fnArgs,
      fnType: "unknown",
      runAtTime,
      options: this.options,
    });
    return id as WorkId<null>;
  }
  async cancel(ctx: RunMutationCtx, id: WorkId<any>): Promise<void> {
    await ctx.runMutation(this.component.lib.cancel, { id });
  }
  async status<ReturnType>(
    ctx: RunQueryCtx,
    id: WorkId<ReturnType>
  ): Promise<
    | { kind: "pending" }
    | { kind: "inProgress" }
    | { kind: "success"; result: ReturnType }
    | { kind: "error"; error: string }
  > {
    return await ctx.runQuery(this.component.lib.status, { id });
  }
  async tryResult<ReturnType>(
    ctx: RunQueryCtx,
    id: WorkId<ReturnType>
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
  // TODO(emma) consider removing. Apps can do this with `tryResult` if they want, and this is a tight resource-intensive loop.
  async pollResult<ReturnType>(
    ctx: RunQueryCtx & RunActionCtx,
    id: WorkId<ReturnType>,
    timeoutMs: number
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
  // TODO(emma): just make this a wrapper around the scheduler.
  // don't need to do the runAction/runMutation here.
  // Also we can consider deleting this method entirely; just make them use
  // enqueueMutation and enqueueAction.
  ctx<DataModel extends GenericDataModel>(
    ctx: GenericActionCtx<DataModel>
  ): GenericActionCtx<DataModel> {
    return {
      runAction: (async (action: any, args: any) => {
        const workId = await this.enqueueAction(ctx, action, args);
        return this.pollResult(ctx, workId, 30 * 1000);
      }) as any,
      runMutation: (async (mutation: any, args: any) => {
        const workId = await this.enqueueMutation(ctx, mutation, args);
        return this.pollResult(ctx, workId, 30 * 1000);
      }) as any,
      scheduler: {
        runAfter: async (delay: number, fn: any, args: any) =>
          this.enqueueUnknown(ctx, fn, args, Date.now() + delay),
        runAt: async (time: number, fn: any, args: any) =>
          this.enqueueUnknown(ctx, fn, args, time),
        cancel: async (id: any) => this.cancel(ctx, id),
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
