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
export {
  vResultValidator,
  type RunResult,
  type RetryBehavior,
  type OnComplete,
};
export {
  retryBehavior as vRetryBehavior,
  onComplete as vOnComplete,
} from "../component/shared.js";
export { logLevel as vLogLevel, type LogLevel } from "../component/logging.js";
export type WorkId = string & { __isWorkId: true };
export const vWorkIdValidator = v.string() as VString<WorkId>;
export {
  /** @deprecated Use `vWorkIdValidator` instead. */
  vWorkIdValidator as workIdValidator,
  /** @deprecated Use `vResultValidator` instead. */
  vResultValidator as resultValidator,
};

// Attempts will run with delay [0, 250, 500, 1000, 2000] (ms)
export const DEFAULT_RETRY_BEHAVIOR: RetryBehavior = {
  maxAttempts: 5,
  initialBackoffMs: 250,
  base: 2,
};

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
    private component: UseApi<Mounts>, // UseApi<api> for jump to definition
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
    options?: RetryOption & CallbackOptions & SchedulerOptions & NameOption
  ): Promise<WorkId> {
    const retryBehavior = getRetryBehavior(
      this.options.defaultRetryBehavior,
      this.options.retryActionsByDefault,
      options?.retry
    );
    const onComplete: OnComplete | undefined = options?.onComplete
      ? {
          fnHandle: await createFunctionHandle(options.onComplete),
          context: options.context,
        }
      : undefined;
    const id = await ctx.runMutation(this.component.lib.enqueue, {
      ...(await defaultEnqueueArgs(fn, options?.name, this.options)),
      fnArgs,
      fnType: "action",
      runAt: getRunAt(options),
      onComplete,
      retryBehavior,
    });
    return id as WorkId;
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
    options?: CallbackOptions & SchedulerOptions & NameOption
  ): Promise<WorkId> {
    const onComplete: OnComplete | undefined = options?.onComplete
      ? {
          fnHandle: await createFunctionHandle(options.onComplete),
          context: options.context,
        }
      : undefined;
    const id = await ctx.runMutation(this.component.lib.enqueue, {
      ...(await defaultEnqueueArgs(fn, options?.name, this.options)),
      fnArgs,
      fnType: "mutation",
      runAt: getRunAt(options),
      onComplete,
    });
    return id as WorkId;
  }

  async enqueueQuery<Args extends DefaultFunctionArgs, ReturnType>(
    ctx: RunMutationCtx,
    fn: FunctionReference<"query", FunctionVisibility, Args, ReturnType>,
    fnArgs: Args,
    options?: CallbackOptions & SchedulerOptions & NameOption
  ): Promise<WorkId> {
    const onComplete: OnComplete | undefined = options?.onComplete
      ? {
          fnHandle: await createFunctionHandle(options.onComplete),
          context: options.context,
        }
      : undefined;
    const id = await ctx.runMutation(this.component.lib.enqueue, {
      ...(await defaultEnqueueArgs(fn, options?.name, this.options)),
      fnArgs,
      fnType: "query",
      runAt: getRunAt(options),
      onComplete,
    });
    return id as WorkId;
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
  async cancelAll(ctx: RunMutationCtx): Promise<void> {
    await ctx.runMutation(this.component.lib.cancelAll, {
      logLevel: this.options.logLevel ?? DEFAULT_LOG_LEVEL,
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
    V extends Validator<any, "required", any> = VAny,
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
      args: vOnCompleteValidator(context),
      handler,
    });
  }
}

/**
 * Returns a validator to use for the onComplete mutation.
 * To be used like:
 * ```ts
 * export const myOnComplete = internalMutation({
 *   args: vOnCompleteValidator(v.string()),
 *   handler: async (ctx, {workId, context, result}) => {
 *     // context has been validated as a string
 *     // ... do something with the result
 *   },
 * });
 * @param context - The context validator. If not provided, it will be `v.any()`.
 * @returns The validator for the onComplete mutation.
 */
export function vOnCompleteValidator<
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  V extends Validator<any, "required", any> = VAny,
>(context?: V) {
  return v.object({
    workId: vWorkIdValidator,
    context: context ?? v.any(),
    result: vResultValidator,
  });
}

export type NameOption = {
  /**
   * The name of the function. By default, if you pass in api.foo.bar.baz,
   * it will use "foo/bar:baz" as the name. If you pass in a function handle,
   * it will use the function handle directly.
   */
  name?: string;
};

export type RetryOption = {
  /** Whether to retry the action if it fails.
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
export type SchedulerOptions =
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
    };

export type CallbackOptions = {
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
   *  args: vOnCompleteValidator(v.string()),
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
};

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

async function defaultEnqueueArgs(
  fn:
    | FunctionReference<FunctionType, FunctionVisibility>
    | FunctionHandle<FunctionType, DefaultFunctionArgs>,
  name: string | undefined,
  { logLevel, maxParallelism }: Partial<Config>
) {
  const [fnHandle, fnName] =
    typeof fn === "string" && fn.startsWith("function://")
      ? [fn, name ?? fn]
      : [await createFunctionHandle(fn), name ?? safeFunctionName(fn)];
  return {
    fnHandle,
    fnName,
    config: {
      logLevel: logLevel ?? DEFAULT_LOG_LEVEL,
      maxParallelism: maxParallelism ?? DEFAULT_MAX_PARALLELISM,
    },
  };
}

function getRunAt(options?: SchedulerOptions): number {
  if (!options) {
    return Date.now();
  }
  if ("runAt" in options && options.runAt !== undefined) {
    return options.runAt;
  }
  if ("runAfter" in options && options.runAfter !== undefined) {
    return Date.now() + options.runAfter;
  }
  return Date.now();
}
