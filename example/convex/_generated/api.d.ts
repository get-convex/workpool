/* eslint-disable */
/**
 * Generated `api` utility.
 *
 * THIS CODE IS AUTOMATICALLY GENERATED.
 *
 * To regenerate, run `npx convex dev`.
 * @module
 */

import type * as crons from "../crons.js";
import type * as example from "../example.js";

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
  crons: typeof crons;
  example: typeof example;
}>;
declare const fullApiWithMounts: typeof fullApi;

export declare const api: FilterApi<
  typeof fullApiWithMounts,
  FunctionReference<any, "public">
>;
export declare const internal: FilterApi<
  typeof fullApiWithMounts,
  FunctionReference<any, "internal">
>;

export declare const components: {
  workpool: {
    lib: {
      cancel: FunctionReference<
        "mutation",
        "internal",
        { id: string; logLevel?: "DEBUG" | "INFO" | "WARN" | "ERROR" },
        any
      >;
      enqueue: FunctionReference<
        "mutation",
        "internal",
        {
          config: {
            logLevel?: "DEBUG" | "INFO" | "WARN" | "ERROR";
            maxParallelism: number;
          };
          fnArgs: any;
          fnHandle: string;
          fnName: string;
          fnType: "action" | "mutation";
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
      status: FunctionReference<
        "query",
        "internal",
        { id: string },
        | { attempt: number; state: "pending" }
        | { attempt: number; state: "running" }
        | { state: "done" }
      >;
    };
  };
  lowpriWorkpool: {
    lib: {
      cancel: FunctionReference<
        "mutation",
        "internal",
        { id: string; logLevel?: "DEBUG" | "INFO" | "WARN" | "ERROR" },
        any
      >;
      enqueue: FunctionReference<
        "mutation",
        "internal",
        {
          config: {
            logLevel?: "DEBUG" | "INFO" | "WARN" | "ERROR";
            maxParallelism: number;
          };
          fnArgs: any;
          fnHandle: string;
          fnName: string;
          fnType: "action" | "mutation";
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
      status: FunctionReference<
        "query",
        "internal",
        { id: string },
        | { attempt: number; state: "pending" }
        | { attempt: number; state: "running" }
        | { state: "done" }
      >;
    };
  };
  highPriWorkpool: {
    lib: {
      cancel: FunctionReference<
        "mutation",
        "internal",
        { id: string; logLevel?: "DEBUG" | "INFO" | "WARN" | "ERROR" },
        any
      >;
      enqueue: FunctionReference<
        "mutation",
        "internal",
        {
          config: {
            logLevel?: "DEBUG" | "INFO" | "WARN" | "ERROR";
            maxParallelism: number;
          };
          fnArgs: any;
          fnHandle: string;
          fnName: string;
          fnType: "action" | "mutation";
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
      status: FunctionReference<
        "query",
        "internal",
        { id: string },
        | { attempt: number; state: "pending" }
        | { attempt: number; state: "running" }
        | { state: "done" }
      >;
    };
  };
};
