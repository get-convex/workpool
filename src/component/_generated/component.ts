/* eslint-disable */
/**
 * Generated `ComponentApi` utility.
 *
 * THIS CODE IS AUTOMATICALLY GENERATED.
 *
 * To regenerate, run `npx convex dev`.
 * @module
 */

import type { FunctionReference } from "convex/server";

/**
 * A utility for referencing a Convex component's exposed API.
 *
 * Useful when expecting a parameter like `components.myComponent`.
 * Usage:
 * ```ts
 * async function myFunction(ctx: QueryCtx, component: ComponentApi) {
 *   return ctx.runQuery(component.someFile.someQuery, { ...args });
 * }
 * ```
 */
export type ComponentApi<Name extends string | undefined = string | undefined> =
  {
    config: {
      update: FunctionReference<
        "mutation",
        "internal",
        {
          logLevel?: "DEBUG" | "TRACE" | "INFO" | "REPORT" | "WARN" | "ERROR";
          maxParallelism?: number;
        },
        any,
        Name
      >;
    };
    lib: {
      cancel: FunctionReference<
        "mutation",
        "internal",
        {
          id: string;
          logLevel?: "DEBUG" | "TRACE" | "INFO" | "REPORT" | "WARN" | "ERROR";
        },
        any,
        Name
      >;
      cancelAll: FunctionReference<
        "mutation",
        "internal",
        {
          before?: number;
          limit?: number;
          logLevel?: "DEBUG" | "TRACE" | "INFO" | "REPORT" | "WARN" | "ERROR";
        },
        any,
        Name
      >;
      enqueue: FunctionReference<
        "mutation",
        "internal",
        {
          config: {
            logLevel?: "DEBUG" | "TRACE" | "INFO" | "REPORT" | "WARN" | "ERROR";
            maxParallelism?: number;
          };
          fnArgs: any;
          fnHandle: string;
          fnName: string;
          fnType: "action" | "mutation" | "query";
          onComplete?: { context?: any; fnHandle: string };
          retryBehavior?: {
            base: number;
            initialBackoffMs: number;
            maxAttempts: number;
          };
          runAt: number;
        },
        string,
        Name
      >;
      enqueueBatch: FunctionReference<
        "mutation",
        "internal",
        {
          config: {
            logLevel?: "DEBUG" | "TRACE" | "INFO" | "REPORT" | "WARN" | "ERROR";
            maxParallelism?: number;
          };
          items: Array<{
            fnArgs: any;
            fnHandle: string;
            fnName: string;
            fnType: "action" | "mutation" | "query";
            onComplete?: { context?: any; fnHandle: string };
            retryBehavior?: {
              base: number;
              initialBackoffMs: number;
              maxAttempts: number;
            };
            runAt: number;
          }>;
        },
        Array<string>,
        Name
      >;
      status: FunctionReference<
        "query",
        "internal",
        { id: string },
        | { previousAttempts: number; state: "pending" }
        | { previousAttempts: number; state: "running" }
        | { state: "finished" },
        Name
      >;
      statusBatch: FunctionReference<
        "query",
        "internal",
        { ids: Array<string> },
        Array<
          | { previousAttempts: number; state: "pending" }
          | { previousAttempts: number; state: "running" }
          | { state: "finished" }
        >,
        Name
      >;
    };
    batch: {
      cancel: FunctionReference<
        "mutation",
        "internal",
        { taskId: string },
        any,
        Name
      >;
      claimBatch: FunctionReference<
        "mutation",
        "internal",
        { slot: number; limit: number },
        Array<{ _id: string; args: any; attempt: number; name: string }>,
        Name
      >;
      claimByIds: FunctionReference<
        "mutation",
        "internal",
        { taskIds: Array<string> },
        Array<{ _id: string; args: any; attempt: number; name: string }>,
        Name
      >;
      listPending: FunctionReference<
        "query",
        "internal",
        { slot: number; limit: number },
        Array<string>,
        Name
      >;
      complete: FunctionReference<
        "mutation",
        "internal",
        { result: any; taskId: string },
        any,
        Name
      >;
      completeBatch: FunctionReference<
        "mutation",
        "internal",
        { items: Array<{ result: any; taskId: string }> },
        Array<{
          fnHandle: string;
          workId: string;
          context?: any;
          result:
            | { kind: "success"; returnValue: any }
            | { kind: "failed"; error: string }
            | { kind: "canceled" };
        }>,
        Name
      >;
      configure: FunctionReference<
        "mutation",
        "internal",
        { claimTimeoutMs: number; executorHandle: string; maxWorkers: number },
        any,
        Name
      >;
      countPending: FunctionReference<"query", "internal", { slot?: number }, number, Name>;
      enqueue: FunctionReference<
        "mutation",
        "internal",
        {
          args: any;
          name: string;
          slot: number;
          onComplete?: { context?: any; fnHandle: string };
          batchConfig?: {
            claimTimeoutMs: number;
            executorHandle: string;
            maxWorkers: number;
          };
          retryBehavior?: {
            base: number;
            initialBackoffMs: number;
            maxAttempts: number;
          };
        },
        string,
        Name
      >;
      enqueueBatch: FunctionReference<
        "mutation",
        "internal",
        {
          batchConfig?: {
            claimTimeoutMs: number;
            executorHandle: string;
            maxWorkers: number;
          };
          tasks: Array<{
            args: any;
            name: string;
            slot: number;
            onComplete?: { context?: any; fnHandle: string };
            retryBehavior?: {
              base: number;
              initialBackoffMs: number;
              maxAttempts: number;
            };
          }>;
        },
        Array<string>,
        Name
      >;
      executorDone: FunctionReference<
        "mutation",
        "internal",
        { startMore: boolean; slot: number },
        any,
        Name
      >;
      fail: FunctionReference<
        "mutation",
        "internal",
        { error: string; taskId: string },
        any,
        Name
      >;
      failBatch: FunctionReference<
        "mutation",
        "internal",
        { items: Array<{ error: string; taskId: string }> },
        Array<{
          fnHandle: string;
          workId: string;
          context?: any;
          result:
            | { kind: "success"; returnValue: any }
            | { kind: "failed"; error: string }
            | { kind: "canceled" };
        }>,
        Name
      >;
      releaseClaims: FunctionReference<
        "mutation",
        "internal",
        { taskIds: Array<string> },
        any,
        Name
      >;
      status: FunctionReference<
        "query",
        "internal",
        { taskId: string },
        | { attempt: number; state: "pending" }
        | { attempt: number; state: "running" }
        | { state: "finished" },
        Name
      >;
      sweepStaleClaims: FunctionReference<
        "mutation",
        "internal",
        {},
        number,
        Name
      >;
      dispatchOnCompleteBatch: FunctionReference<
        "mutation",
        "internal",
        {
          items: Array<{
            fnHandle: string;
            workId: string;
            context?: any;
            result:
              | { kind: "success"; returnValue: any }
              | { kind: "failed"; error: string }
              | { kind: "canceled" };
          }>;
        },
        any,
        Name
      >;
      resetConfig: FunctionReference<
        "mutation",
        "internal",
        {},
        any,
        Name
      >;
      resetTasks: FunctionReference<
        "mutation",
        "internal",
        {},
        { deleted: number; more: boolean },
        Name
      >;
    };
  };
