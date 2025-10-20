/* eslint-disable */
/**
 * Generated `api` utility.
 *
 * THIS CODE IS AUTOMATICALLY GENERATED.
 *
 * To regenerate, run `npx convex dev`.
 * @module
 */

import type * as complete from "../complete.js";
import type * as crons from "../crons.js";
import type * as danger from "../danger.js";
import type * as kick from "../kick.js";
import type * as lib from "../lib.js";
import type * as logging from "../logging.js";
import type * as loop from "../loop.js";
import type * as recovery from "../recovery.js";
import type * as shared from "../shared.js";
import type * as stats from "../stats.js";
import type * as worker from "../worker.js";

import type {
  ApiFromModules,
  FilterApi,
  FunctionReference,
} from "convex/server";

/**
 * A utility for referencing Convex functions in your app's API.
 *
 * Usage:
 * ```js
 * const myFunctionReference = api.myModule.myFunction;
 * ```
 */
declare const fullApi: ApiFromModules<{
  complete: typeof complete;
  crons: typeof crons;
  danger: typeof danger;
  kick: typeof kick;
  lib: typeof lib;
  logging: typeof logging;
  loop: typeof loop;
  recovery: typeof recovery;
  shared: typeof shared;
  stats: typeof stats;
  worker: typeof worker;
}>;
export type Mounts = {
  lib: {
    cancel: FunctionReference<
      "mutation",
      "public",
      {
        id: string;
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
      string
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
      Array<string>
    >;
    status: FunctionReference<
      "query",
      "public",
      { id: string },
      | { previousAttempts: number; state: "pending" }
      | { previousAttempts: number; state: "running" }
      | { state: "finished" }
    >;
    statusBatch: FunctionReference<
      "query",
      "public",
      { ids: Array<string> },
      Array<
        | { previousAttempts: number; state: "pending" }
        | { previousAttempts: number; state: "running" }
        | { state: "finished" }
      >
    >;
  };
};
// For now fullApiWithMounts is only fullApi which provides
// jump-to-definition in component client code.
// Use Mounts for the same type without the inference.
declare const fullApiWithMounts: typeof fullApi;

export declare const api: FilterApi<
  typeof fullApiWithMounts,
  FunctionReference<any, "public">
>;
export declare const internal: FilterApi<
  typeof fullApiWithMounts,
  FunctionReference<any, "internal">
>;

export declare const components: {};
