import {
  createFunctionHandle,
  DefaultFunctionArgs,
  Expand,
  FunctionReference,
  FunctionVisibility,
  GenericDataModel,
  GenericMutationCtx,
  GenericQueryCtx,
  getFunctionName,
} from "convex/server";
import { GenericId } from "convex/values";
import { api } from "../component/_generated/api";
import { LogLevel } from "../component/logging";
import { completionStatus, type CompletionStatus } from "../component/schema";
export { completionStatus, type CompletionStatus };

export type WorkId = string;

export class Workpool {
  constructor(
    private component: UseApi<typeof api>,
    private options: {
      /** How many actions/mutations can be running at once within this pool.
       * Min 1, Max 300.
       */
      maxParallelism: number;
      /** How much to log.
       * Default WARN.
       * With INFO, you can see events for started and completed work, which can
       * be parsed.
       * With DEBUG, you can see timers and internal events for work being
       * scheduled.
       */
      logLevel?: LogLevel;
      /** How long to keep completed work in the database, for access by `status`.
       * Default 1 day.
       */
      statusTtl?: number;
    }
  ) {}
  async enqueueAction<Args extends DefaultFunctionArgs, ReturnType>(
    ctx: RunMutationCtx,
    fn: FunctionReference<"action", FunctionVisibility, Args, ReturnType>,
    fnArgs: Args
  ): Promise<WorkId> {
    const fnHandle = await createFunctionHandle(fn);
    const id = await ctx.runMutation(this.component.lib.enqueue, {
      fnHandle,
      fnName: getFunctionName(fn),
      fnArgs,
      fnType: "action",
      options: this.options,
    });
    return id as WorkId;
  }
  async enqueueMutation<Args extends DefaultFunctionArgs, ReturnType>(
    ctx: RunMutationCtx,
    fn: FunctionReference<"mutation", FunctionVisibility, Args, ReturnType>,
    fnArgs: Args
  ): Promise<WorkId> {
    const fnHandle = await createFunctionHandle(fn);
    const id = await ctx.runMutation(this.component.lib.enqueue, {
      fnHandle,
      fnName: getFunctionName(fn),
      fnArgs,
      fnType: "mutation",
      options: this.options,
    });
    return id as WorkId;
  }
  async cancel(ctx: RunMutationCtx, id: WorkId): Promise<void> {
    await ctx.runMutation(this.component.lib.cancel, { id });
  }
  async status(
    ctx: RunQueryCtx,
    id: WorkId
  ): Promise<
    | { kind: "pending" }
    | { kind: "inProgress" }
    | { kind: "completed"; completionStatus: CompletionStatus }
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
