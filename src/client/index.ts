import {
  createFunctionHandle,
  DefaultFunctionArgs,
  FunctionHandle,
  FunctionReference,
  FunctionType,
  FunctionVisibility,
  GenericDataModel,
  GenericMutationCtx,
  internalMutationGeneric,
  RegisteredMutation,
} from "convex/server";
import { Infer, v, Validator, VAny, VString } from "convex/values";
import { Mounts } from "../component/_generated/api.js";
import { DEFAULT_LOG_LEVEL, type LogLevel } from "../component/logging.js";
import {
  Config,
  DEFAULT_MAX_PARALLELISM,
  OnComplete,
  vResultValidator,
  type RetryBehavior,
  RunResult,
  OnCompleteArgs as SharedOnCompleteArgs,
  Status,
} from "../component/shared.js";
import {
  RunMutationCtx,
  RunQueryCtx,
  safeFunctionName,
  UseApi,
} from "./utils.js";
export { vResultValidator, type RunResult, type RetryBehavior };
export { retryBehavior as vRetryBehavior } from "../component/shared.js";
export { logLevel as vLogLevel, type LogLevel } from "../component/logging.js";
export type WorkId = string & { __isWorkId: true };
export const vWorkIdValidator = v.string() as VString<WorkId>;
export {
  /** @deprecated Use `vWorkIdValidator` instead. */
  vWorkIdValidator as workIdValidator,
  /** @deprecated Use `vResultValidator` instead. */
  vResultValidator as resultValidator,
};
/** Equivalent to `vOnCompleteArgs(<your-context-validator>)`. */
export const vOnComplete = vOnCompleteArgs(v.any());
/** @deprecated Use `vOnCompleteArgs()` instead. */
export const vOnCompleteValidator = vOnCompleteArgs;

// Attempts will run with delay [0, 250, 500, 1000, 2000] (ms)
export const DEFAULT_RETRY_BEHAVIOR: RetryBehavior = {
  maxAttempts: 5,
  initialBackoffMs: 250,
  base: 2,
};

// UseApi<api> for jump to definition
export type WorkpoolComponent = UseApi<Mounts>;

export class Workpool {
  /**
   * Initializes a Workpool.
   *
   * Note: if you want different pools, you need to *create different instances*
   * of Workpool in convex.config.ts. It isn't sufficient to have different
   * instances of this class.
   *
   * @param component - The component to use, like `components.workpool` from
   *   `./_generated/api.ts`.
   * @param options - The {@link WorkpoolOptions} for the Workpool.
   */
  constructor(
    public component: WorkpoolComponent,
    public options: WorkpoolOptions
  ) {}

  /**
   * Enqueues an action to be run.
   *
   * @param ctx - The mutation or action context that can call ctx.runMutation.
   * @param fn - The action to run, like `internal.example.myAction`.
   * @param fnArgs - The arguments to pass to the action.
   * @param options - The options for the action to specify retry behavior,
   *   onComplete handling, and scheduling via `runAt` or `runAfter`.
   * @returns The ID of the work that was enqueued.
   */
  async enqueueAction<Args extends DefaultFunctionArgs, ReturnType>(
    ctx: RunMutationCtx,
    fn: FunctionReference<"action", FunctionVisibility, Args, ReturnType>,
    fnArgs: Args,
    options?: RetryOption & EnqueueOptions
  ): Promise<WorkId> {
    const retryBehavior = getRetryBehavior(
      this.options.defaultRetryBehavior,
      this.options.retryActionsByDefault,
      options?.retry
    );
    return enqueue(this.component, ctx, "action", fn, fnArgs, {
      retryBehavior,
      ...this.options,
      ...options,
    });
  }

  /**
   * Enqueues a batch of actions to be run.
   * Each action will be run independently, and the onComplete handler will
   * be called for each action.
   *
   * @param ctx - The mutation or action ctx that can call ctx.runMutation.
   * @param fn - The action to run, like `internal.example.myAction`.
   * @param argsArray - The arguments to pass to the action.
   * @param options - The options for the actions to specify retry behavior,
   *   onComplete handling, and scheduling via `runAt` or `runAfter`.
   * @returns The IDs of the work that was enqueued.
   */
  async enqueueActionBatch<Args extends DefaultFunctionArgs, ReturnType>(
    ctx: RunMutationCtx,
    fn: FunctionReference<"action", FunctionVisibility, Args, ReturnType>,
    argsArray: Array<Args>,
    options?: RetryOption & EnqueueOptions
  ): Promise<WorkId[]> {
    const retryBehavior = getRetryBehavior(
      this.options.defaultRetryBehavior,
      this.options.retryActionsByDefault,
      options?.retry
    );
    return enqueueBatch(this.component, ctx, "action", fn, argsArray, {
      retryBehavior,
      ...this.options,
      ...options,
    });
  }

  /**
   * Enqueues a mutation to be run.
   *
   * Note: mutations are not retried by the workpool. Convex automatically
   * retries them on database conflicts and transient failures.
   * Because they're deterministic, external retries don't provide any benefit.
   *
   * @param ctx - The mutation or action context that can call ctx.runMutation.
   * @param fn - The mutation to run, like `internal.example.myMutation`.
   * @param fnArgs - The arguments to pass to the mutation.
   * @param options - The options for the mutation to specify onComplete handling
   *   and scheduling via `runAt` or `runAfter`.
   */
  async enqueueMutation<Args extends DefaultFunctionArgs, ReturnType>(
    ctx: RunMutationCtx,
    fn: FunctionReference<"mutation", FunctionVisibility, Args, ReturnType>,
    fnArgs: Args,
    options?: EnqueueOptions
  ): Promise<WorkId> {
    return enqueue(this.component, ctx, "mutation", fn, fnArgs, {
      ...this.options,
      ...options,
    });
  }
  /**
   * Enqueues a batch of mutations to be run.
   * Each mutation will be run independently, and the onComplete handler will
   * be called for each mutation.
   *
   * @param ctx - The mutation or action context that can call ctx.runMutation.
   * @param fn - The mutation to run, like `internal.example.myMutation`.
   * @param argsArray - The arguments to pass to the mutations.
   * @param options - The options for the mutations to specify onComplete handling
   *   and scheduling via `runAt` or `runAfter`.
   */
  async enqueueMutationBatch<Args extends DefaultFunctionArgs, ReturnType>(
    ctx: RunMutationCtx,
    fn: FunctionReference<"mutation", FunctionVisibility, Args, ReturnType>,
    argsArray: Array<Args>,
    options?: EnqueueOptions
  ): Promise<WorkId[]> {
    return enqueueBatch(this.component, ctx, "mutation", fn, argsArray, {
      ...this.options,
      ...options,
    });
  }

  /**
   * Enqueues a query to be run.
   * Usually not what you want, but it can be useful during workflows.
   * The query is run in a mutation and the result is returned to the caller,
   * so it can conflict if other mutations are writing the value.
   *
   * @param ctx - The mutation or action context that can call ctx.runMutation.
   * @param fn - The query to run, like `internal.example.myQuery`.
   * @param fnArgs - The arguments to pass to the query.
   * @param options - The options for the query to specify onComplete handling
   *   and scheduling via `runAt` or `runAfter`.
   */
  async enqueueQuery<Args extends DefaultFunctionArgs, ReturnType>(
    ctx: RunMutationCtx,
    fn: FunctionReference<"query", FunctionVisibility, Args, ReturnType>,
    fnArgs: Args,
    options?: EnqueueOptions
  ): Promise<WorkId> {
    return enqueue(this.component, ctx, "query", fn, fnArgs, {
      ...this.options,
      ...options,
    });
  }

  /**
   * Enqueues a batch of queries to be run.
   * Each query will be run independently, and the onComplete handler will
   * be called for each query.
   *
   * @param ctx - The mutation or action context that can call ctx.runMutation.
   * @param fn - The query to run, like `internal.example.myQuery`.
   * @param argsArray - The arguments to pass to the queries.
   * @param options - The options for the queries to specify onComplete handling
   *   and scheduling via `runAt` or `runAfter`.
   */
  async enqueueQueryBatch<Args extends DefaultFunctionArgs, ReturnType>(
    ctx: RunMutationCtx,
    fn: FunctionReference<"query", FunctionVisibility, Args, ReturnType>,
    argsArray: Array<Args>,
    options?: EnqueueOptions
  ): Promise<WorkId[]> {
    return enqueueBatch(this.component, ctx, "query", fn, argsArray, {
      ...this.options,
      ...options,
    });
  }

  /**
   * Cancels a work item. If it's already started, it will be allowed to finish
   * but will not be retried.
   *
   * @param ctx - The mutation or action context that can call ctx.runMutation.
   * @param id - The ID of the work to cancel.
   */
  async cancel(ctx: RunMutationCtx, id: WorkId): Promise<void> {
    await ctx.runMutation(this.component.lib.cancel, {
      id,
      logLevel: this.options.logLevel ?? DEFAULT_LOG_LEVEL,
    });
  }
  /**
   * Cancels all pending work items. See {@link cancel}.
   *
   * @param ctx - The mutation or action context that can call ctx.runMutation.
   */
  async cancelAll(
    ctx: RunMutationCtx,
    options?: { limit?: number }
  ): Promise<void> {
    await ctx.runMutation(this.component.lib.cancelAll, {
      logLevel: this.options.logLevel ?? DEFAULT_LOG_LEVEL,
      ...options,
    });
  }
  /**
   * Gets the status of a work item.
   *
   * @param ctx - The query context that can call ctx.runQuery.
   * @param id - The ID of the work to get the status of.
   * @returns The status of the work item. One of:
   * - `{ state: "pending", previousAttempts: number }`
   * - `{ state: "running", previousAttempts: number }`
   * - `{ state: "finished" }`
   */
  async status(ctx: RunQueryCtx, id: WorkId): Promise<Status> {
    return ctx.runQuery(this.component.lib.status, { id });
  }

  /**
   * Gets the status of a batch of work items.
   *
   * @param ctx - The query context that can call ctx.runQuery.
   * @param ids - The IDs of the work to get the status of.
   * @returns The status of the work items.
   */
  async statusBatch(ctx: RunQueryCtx, ids: WorkId[]): Promise<Status[]> {
    return ctx.runQuery(this.component.lib.statusBatch, { ids });
  }

  /**
   * Defines a mutation that will be run after a work item completes.
   * You can pass this to a call to enqueue* like so:
   * ```ts
   * export const myOnComplete = workpool.defineOnComplete({
   *   context: v.literal("myContextValue"), // optional
   *   handler: async (ctx, {workId, context, result}) => {
   *     // ... do something with the result
   *   },
   * });
   *
   * // in some other function:
   * const workId = await workpool.enqueueAction(ctx, internal.foo.bar, {
   *   // ... args to action
   * }, {
   *   onComplete: internal.foo.myOnComplete,
   * });
   * ```
   */
  defineOnComplete<
    DataModel extends GenericDataModel,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    V extends Validator<any, any, any> = VAny<any, "optional">,
  >({
    context,
    handler,
  }: {
    context?: V;
    handler: (
      ctx: GenericMutationCtx<DataModel>,
      args: {
        workId: WorkId;
        context: Infer<V>;
        result: RunResult;
      }
    ) => Promise<void>;
  }): RegisteredMutation<"internal", OnCompleteArgs, null> {
    return internalMutationGeneric({
      args: vOnCompleteArgs(context),
      handler,
    });
  }
}

/**
 * Returns a validator to use for the onComplete mutation.
 * To be used like:
 * ```ts
 * export const myOnComplete = internalMutation({
 *   args: vOnCompleteArgs(v.string()),
 *   handler: async (ctx, {workId, context, result}) => {
 *     // context has been validated as a string
 *     // ... do something with the result
 *   },
 * });
 * @param context - The context validator. If not provided, it will be `v.any()`.
 * @returns The validator for the onComplete mutation.
 */
export function vOnCompleteArgs<
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  V extends Validator<any, "required", any> = VAny,
>(context?: V) {
  return v.object({
    workId: vWorkIdValidator,
    context: (context ?? v.optional(v.any())) as V,
    result: vResultValidator,
  });
}

export type RetryOption = {
  /** Whether to retry the action if it fails.
   * If false, the action won’t be retried.
   * If true, it will use the default retry behavior.
   * If custom behavior is provided, it will retry using that behavior.
   * If unset, it will use the Workpool's configured default.
   */
  retry?: boolean | RetryBehavior;
};

export type WorkpoolOptions = {
  /** How many actions/mutations can be running at once within this pool.
   * Min 1, Suggested max: 100 on Pro, 20 on the free plan.
   */
  maxParallelism?: number;
  /** How much to log. This is updated on each call to `enqueue*`,
   * `status`, or `cancel*`.
   * Default is REPORT, which logs warnings, errors, and a periodic report.
   * With INFO, you can also see events for started and completed work.
   * Stats generated can be parsed by tools like
   * [Axiom](https://axiom.co) for monitoring.
   * With DEBUG, you can see timers and internal events for work being
   * scheduled.
   */
  logLevel?: LogLevel;
} & WorkpoolRetryOptions;

export type WorkpoolRetryOptions = {
  /** Default retry behavior for enqueued actions.
   * See {@link RetryBehavior}.
   */
  defaultRetryBehavior?: RetryBehavior;
  /** Whether to retry actions that fail by default. Default: false.
   * NOTE: Only enable this if your actions are idempotent.
   * See the docs (README.md) for more details.
   */
  retryActionsByDefault?: boolean;
};

export type EnqueueOptions = {
  /**
   * The name of the function. By default, if you pass in api.foo.bar.baz,
   * it will use "foo/bar:baz" as the name. If you pass in a function handle,
   * it will use the function handle directly.
   */
  name?: string;
  /**
   * A mutation to run after the function succeeds, fails, or is canceled.
   * The context type is for your use, feel free to provide a validator for it.
   * e.g.
   * ```ts
   * export const completion = workpool.defineOnComplete({
   *   context: v.string(),
   *   handler: async (ctx, {workId, context, result}) => {
   *     // context has been validated as a string
   *     // ... do something with the result
   *   },
   * });
   * ```
   * or more manually:
   * ```ts
   * export const completion = internalMutation({
   *  args: vOnCompleteArgs(v.string()),
   *  handler: async (ctx, args) => {
   *    console.log(args.result, "Got Context back -> ", args.context, Date.now() - args.context);
   *  },
   * });
   * ```
   */
  onComplete?: FunctionReference<
    "mutation",
    FunctionVisibility,
    OnCompleteArgs
  > | null;

  /**
   * A context object to pass to the `onComplete` mutation.
   * Useful for passing data from the enqueue site to the onComplete site.
   */
  context?: unknown;
} & (
  | {
      /**
       * The time (ms since epoch) to run the action at.
       * If not provided, the action will be run as soon as possible.
       * Note: this is advisory only. It may run later.
       */
      runAt?: number;
    }
  | {
      /**
       * The number of milliseconds to run the action after.
       * If not provided, the action will be run as soon as possible.
       * Note: this is advisory only. It may run later.
       */
      runAfter?: number;
    }
);

export type OnCompleteArgs = {
  /**
   * The ID of the work that completed.
   */
  workId: WorkId;
  /**
   * The context object passed when enqueuing the work.
   * Useful for passing data from the enqueue site to the onComplete site.
   */
  context: unknown;
  /**
   * The result of the run that completed.
   */
  result: RunResult;
};

// ensure OnCompleteArgs satisfies SharedOnCompleteArgs
const _ = {} as OnCompleteArgs satisfies SharedOnCompleteArgs;
const _2 = {} as OnCompleteArgs satisfies Infer<
  ReturnType<typeof vOnCompleteArgs<VAny>>
>;

//
// Helper functions
//

function getRetryBehavior(
  defaultRetryBehavior: RetryBehavior | undefined,
  retryActionsByDefault: boolean | undefined,
  retryOverride: boolean | RetryBehavior | undefined
): RetryBehavior | undefined {
  const defaultRetry = defaultRetryBehavior ?? DEFAULT_RETRY_BEHAVIOR;
  const retryByDefault = retryActionsByDefault ?? false;
  if (retryOverride === true) {
    return defaultRetry;
  }
  if (retryOverride === false) {
    return undefined;
  }
  return retryOverride ?? (retryByDefault ? defaultRetry : undefined);
}

async function enqueueArgs(
  fn:
    | FunctionReference<FunctionType, FunctionVisibility>
    | FunctionHandle<FunctionType, DefaultFunctionArgs>,
  opts:
    | (EnqueueOptions & Partial<Config> & { retryBehavior?: RetryBehavior })
    | undefined
) {
  const [fnHandle, fnName] =
    typeof fn === "string" && fn.startsWith("function://")
      ? [fn, opts?.name ?? fn]
      : [await createFunctionHandle(fn), opts?.name ?? safeFunctionName(fn)];
  const onComplete: OnComplete | undefined = opts?.onComplete
    ? {
        fnHandle: await createFunctionHandle(opts.onComplete),
        context: opts.context,
      }
    : undefined;
  return {
    fnHandle,
    fnName,
    onComplete,
    runAt: getRunAt(opts),
    retryBehavior: opts?.retryBehavior,
    config: {
      logLevel: opts?.logLevel ?? DEFAULT_LOG_LEVEL,
      maxParallelism: opts?.maxParallelism ?? DEFAULT_MAX_PARALLELISM,
    },
  };
}

function getRunAt(
  options:
    | {
        runAt?: number;
        runAfter?: number;
      }
    | undefined
): number {
  if (!options) {
    return Date.now();
  }
  if (options.runAt !== undefined) {
    return options.runAt;
  }
  if (options.runAfter !== undefined) {
    return Date.now() + options.runAfter;
  }
  return Date.now();
}

export async function enqueueBatch<
  FnType extends FunctionType,
  Args extends DefaultFunctionArgs,
  ReturnType,
>(
  component: UseApi<Mounts>,
  ctx: RunMutationCtx,
  fnType: FnType,
  fn: FunctionReference<FnType, FunctionVisibility, Args, ReturnType>,
  fnArgsArray: Array<Args>,
  options: EnqueueOptions & {
    retryBehavior?: RetryBehavior;
    maxParallelism?: number;
    logLevel?: LogLevel;
  }
): Promise<WorkId[]> {
  const { config, ...defaults } = await enqueueArgs(fn, options);
  const ids = await ctx.runMutation(component.lib.enqueueBatch, {
    items: fnArgsArray.map((fnArgs) => ({
      ...defaults,
      fnArgs,
      fnType,
    })),
    config,
  });
  return ids as WorkId[];
}

export async function enqueue<
  FnType extends FunctionType,
  Args extends DefaultFunctionArgs,
  ReturnType,
>(
  component: UseApi<Mounts>,
  ctx: RunMutationCtx,
  fnType: FnType,
  fn: FunctionReference<FnType, FunctionVisibility, Args, ReturnType>,
  fnArgs: Args,
  options: EnqueueOptions & {
    retryBehavior?: RetryBehavior;
    maxParallelism?: number;
    logLevel?: LogLevel;
  }
): Promise<WorkId> {
  const id = await ctx.runMutation(component.lib.enqueue, {
    ...(await enqueueArgs(fn, options)),
    fnArgs,
    fnType,
  });
  return id as WorkId;
}
