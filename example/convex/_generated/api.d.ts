/* eslint-disable */
/**
 * Generated `api` utility.
 *
 * THIS CODE IS AUTOMATICALLY GENERATED.
 *
 * To regenerate, run `npx convex dev`.
 * @module
 */

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
    public: {
      cancel: FunctionReference<"mutation", "internal", { id: string }, any>;
      cleanup: FunctionReference<
        "mutation",
        "internal",
        { maxAgeMs: number },
        any
      >;
      enqueue: FunctionReference<
        "mutation",
        "internal",
        {
          fnArgs: any;
          fnType: "action" | "mutation" | "unknown";
          handle: string;
          options: {
            actionTimeoutMs?: number;
            completedWorkMaxAgeMs?: number;
            debounceMs?: number;
            fastHeartbeatMs?: number;
            logLevel?: "DEBUG" | "INFO" | "WARN" | "ERROR";
            maxParallelism: number;
            mutationTimeoutMs?: number;
            slowHeartbeatMs?: number;
            unknownTimeoutMs?: number;
          };
          runAtTime: number;
        },
        string
      >;
      startMainLoop: FunctionReference<"mutation", "internal", {}, any>;
      status: FunctionReference<
        "query",
        "internal",
        { id: string },
        | { kind: "pending" }
        | { kind: "inProgress" }
        | { kind: "success"; result: any }
        | { error: string; kind: "error" }
      >;
    };
  };
};
