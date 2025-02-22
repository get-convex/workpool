import {
  createFunctionHandle,
  DefaultFunctionArgs,
  FunctionReference,
  FunctionVisibility,
  getFunctionName,
} from "convex/server";
import { v, VString } from "convex/values";
import { api } from "../component/_generated/api.js";
import {
  OnComplete,
  runResult as runResultValidator,
  RunResult,
  type LogLevel,
  type RetryBehavior,
  OnCompleteArgs as SharedOnCompleteArgs,
  Status,
  logLevel,
  Config,
} from "../component/shared.js";
import { RunMutationCtx, RunQueryCtx, UseApi } from "./utils.js";
import { DEFAULT_LOG_LEVEL } from "../component/logging.js";
import { DEFAULT_MAX_PARALLELISM } from "../component/kick.js";
export { runResultValidator, type RunResult };

// Attempts will run with delay [0, 250, 500, 1000, 2000] (ms)
export const DEFAULT_RETRY_BEHAVIOR: RetryBehavior = {
  maxAttempts: 5,
  initialBackoffMs: 250,
  base: 2,
};
export type WorkId = string & { __isWorkId: true };
export const workIdValidator = v.string() as VString<WorkId>;

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
   * @param options - The options for the Workpool.
   */
  constructor(
    private component: UseApi<typeof api>,
    private options: {
      /** How many actions/mutations can be running at once within this pool.
       * Min 1, Max 300.
       */
      maxParallelism?: number;
      /** How much to log. This is updated on each call to `enqueue*`,
       * `status`, or `cancel*`.
       * Default is WARN.
       * With INFO, you can see events for started and completed work, which can
       * be parsed by tools like [Axiom](https://axiom.co) for monitoring.
       * With DEBUG, you can see timers and internal events for work being
       * scheduled.
       */
      logLevel?: LogLevel;
      /** Default retry behavior for enqueued actions. */
      defaultRetryBehavior?: RetryBehavior;
      /** Whether to retry actions that fail by default. Default: false.
       * NOTE: Only do this if your actions are idempotent.
       * See the docs (README.md) for more details.
       */
      retryActionsByDefault?: boolean;
    }
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
    options?: {
      /** Whether to retry the action if it fails.
       * If true, it will use the default retry behavior.
       * If custom behavior is provided, it will retry using that behavior.
       * If unset, it will use the Workpool's configured default.
       */
      retry?: boolean | RetryBehavior;
    } & CallbackOptions &
      SchedulerOptions
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
      ...(await defaultEnqueueArgs(fn, this.options)),
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
    options?: CallbackOptions & SchedulerOptions
  ): Promise<WorkId> {
    const id = await ctx.runMutation(this.component.lib.enqueue, {
      ...(await defaultEnqueueArgs(fn, this.options)),
      fnArgs,
      fnType: "mutation",
      runAt: getRunAt(options),
    });
    return id as WorkId;
  }
  async cancel(ctx: RunMutationCtx, id: WorkId): Promise<void> {
    await ctx.runMutation(this.component.lib.cancel, {
      id,
      logLevel: this.options.logLevel ?? getDefaultLogLevel(),
    });
  }
  async cancelAll(ctx: RunMutationCtx): Promise<void> {
    await ctx.runMutation(this.component.lib.cancelAll, {
      logLevel: this.options.logLevel ?? getDefaultLogLevel(),
    });
  }
  async status(ctx: RunQueryCtx, id: WorkId): Promise<Status> {
    return ctx.runQuery(this.component.lib.status, { id });
  }
}

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
  fn: FunctionReference<"action" | "mutation", FunctionVisibility>,
  { logLevel, maxParallelism }: Partial<Config>
) {
  return {
    fnHandle: await createFunctionHandle(fn),
    fnName: getFunctionName(fn),
    config: {
      logLevel: logLevel ?? getDefaultLogLevel(),
      maxParallelism: maxParallelism ?? DEFAULT_MAX_PARALLELISM,
    },
  };
}

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
   * export const completion = internalMutation({
   *  args: {
   *    workId: workIdValidator,
   *    context: v.any(),
   *    result: runResult,
   *  },
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

function getDefaultLogLevel(): LogLevel {
  if (process.env.WORKPOOL_LOG_LEVEL) {
    if (
      !logLevel.members
        .map((m) => m.value as string)
        .includes(process.env.WORKPOOL_LOG_LEVEL)
    ) {
      console.warn(
        `Invalid log level (${process.env.WORKPOOL_LOG_LEVEL}), defaulting to "INFO"`
      );
    } else {
      return process.env.WORKPOOL_LOG_LEVEL as LogLevel;
    }
  }
  return DEFAULT_LOG_LEVEL;
}
