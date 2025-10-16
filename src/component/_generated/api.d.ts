/* eslint-disable */
/**
 * Generated `api` utility.
 *
 * THIS CODE IS AUTOMATICALLY GENERATED.
 *
 * To regenerate, run `npx convex dev`.
 * @module
 */

import type { FunctionReference } from "convex/server";

/**
 * A utility for referencing Convex functions in your app's public API.
 *
 * Usage:
 * ```js
 * const myFunctionReference = api.myModule.myFunction;
 * ```
 */
export declare const api: {
  lib: {
    enqueue: FunctionReference<
      "mutation",
      "public",
      {
        config: {
          logLevel: "DEBUG" | "TRACE" | "INFO" | "REPORT" | "WARN" | "ERROR";
          maxParallelism: number;
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
      Id<"work">
    >;
    enqueueBatch: FunctionReference<
      "mutation",
      "public",
      {
        config: {
          logLevel: "DEBUG" | "TRACE" | "INFO" | "REPORT" | "WARN" | "ERROR";
          maxParallelism: number;
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
      Array<Id<"work">>
    >;
    cancel: FunctionReference<
      "mutation",
      "public",
      {
        id: Id<"work">;
        logLevel: "DEBUG" | "TRACE" | "INFO" | "REPORT" | "WARN" | "ERROR";
      },
      any
    >;
    cancelAll: FunctionReference<
      "mutation",
      "public",
      {
        before?: number;
        limit?: number;
        logLevel: "DEBUG" | "TRACE" | "INFO" | "REPORT" | "WARN" | "ERROR";
      },
      any
    >;
    status: FunctionReference<
      "query",
      "public",
      { id: Id<"work"> },
      | { previousAttempts: number; state: "pending" }
      | { previousAttempts: number; state: "running" }
      | { state: "finished" }
    >;
    statusBatch: FunctionReference<
      "query",
      "public",
      { ids: Array<Id<"work">> },
      Array<
        | { previousAttempts: number; state: "pending" }
        | { previousAttempts: number; state: "running" }
        | { state: "finished" }
      >
    >;
  };
};

/**
 * A utility for referencing Convex functions in your app's internal API.
 *
 * Usage:
 * ```js
 * const myFunctionReference = internal.myModule.myFunction;
 * ```
 */
export declare const internal: {
  complete: {
    complete: FunctionReference<
      "mutation",
      "internal",
      {
        jobs: Array<{
          attempt: number;
          runResult:
            | { kind: "success"; returnValue: any }
            | { error: string; kind: "failed" }
            | { kind: "canceled" };
          workId: Id<"work">;
        }>;
      },
      any
    >;
  };
  crons: {
    recover: FunctionReference<"mutation", "internal", {}, any>;
  };
  danger: {
    clearPending: FunctionReference<
      "mutation",
      "internal",
      { before?: number; cursor?: string; olderThan?: number },
      any
    >;
    clearOldWork: FunctionReference<
      "mutation",
      "internal",
      { before?: number; cursor?: string; olderThan?: number },
      any
    >;
  };
  kick: {
    forceKick: FunctionReference<"mutation", "internal", {}, any>;
  };
  loop: {
    main: FunctionReference<
      "mutation",
      "internal",
      { generation: bigint; segment: bigint },
      any
    >;
    updateRunStatus: FunctionReference<
      "mutation",
      "internal",
      { generation: bigint; segment: bigint },
      any
    >;
  };
  recovery: {
    recover: FunctionReference<
      "mutation",
      "internal",
      {
        jobs: Array<{
          attempt: number;
          scheduledId: Id<"_scheduled_functions">;
          started: number;
          workId: Id<"work">;
        }>;
      },
      any
    >;
  };
  stats: {
    calculateBacklogAndReport: FunctionReference<
      "mutation",
      "internal",
      {
        cursor: string;
        endSegment: bigint;
        logLevel: "DEBUG" | "TRACE" | "INFO" | "REPORT" | "WARN" | "ERROR";
        report: {
          canceled: number;
          completed: number;
          failed: number;
          lastReportTs: number;
          retries: number;
          succeeded: number;
        };
        running: number;
        startSegment: bigint;
      },
      any
    >;
    diagnostics: FunctionReference<"query", "internal", {}, any>;
  };
  worker: {
    runMutationWrapper: FunctionReference<
      "mutation",
      "internal",
      {
        attempt: number;
        fnArgs: any;
        fnHandle: string;
        fnType: "query" | "mutation";
        logLevel: "DEBUG" | "TRACE" | "INFO" | "REPORT" | "WARN" | "ERROR";
        workId: Id<"work">;
      },
      any
    >;
    runActionWrapper: FunctionReference<
      "action",
      "internal",
      {
        attempt: number;
        fnArgs: any;
        fnHandle: string;
        logLevel: "DEBUG" | "TRACE" | "INFO" | "REPORT" | "WARN" | "ERROR";
        workId: Id<"work">;
      },
      any
    >;
  };
};

export declare const components: {};
