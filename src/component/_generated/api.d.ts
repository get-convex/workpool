/* eslint-disable */
/**
 * Generated `api` utility.
 *
 * THIS CODE IS AUTOMATICALLY GENERATED.
 *
 * To regenerate, run `npx convex dev`.
 * @module
 */

import type * as lib from "../lib.js";
import type * as logging from "../logging.js";

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
  lib: typeof lib;
  logging: typeof logging;
}>;
export type Mounts = {
  lib: {
    cancel: FunctionReference<"mutation", "public", { id: string }, any>;
    cleanup: FunctionReference<"mutation", "public", { maxAgeMs: number }, any>;
    enqueue: FunctionReference<
      "mutation",
      "public",
      {
        fnArgs: any;
        fnHandle: string;
        fnType: "action" | "mutation" | "unknown";
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
    startMainLoop: FunctionReference<"mutation", "public", {}, any>;
    status: FunctionReference<
      "query",
      "public",
      { id: string },
      | { kind: "pending" }
      | { kind: "inProgress" }
      | { kind: "success"; result: any }
      | { error: string; kind: "error" }
    >;
    stopCleanup: FunctionReference<"mutation", "public", {}, any>;
    stopMainLoop: FunctionReference<"mutation", "public", {}, any>;
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

export declare const components: {
  crons: {
    public: {
      del: FunctionReference<
        "mutation",
        "internal",
        { identifier: { id: string } | { name: string } },
        null
      >;
      get: FunctionReference<
        "query",
        "internal",
        { identifier: { id: string } | { name: string } },
        {
          args: Record<string, any>;
          functionHandle: string;
          id: string;
          name?: string;
          schedule:
            | { kind: "interval"; ms: number }
            | { cronspec: string; kind: "cron" };
        } | null
      >;
      list: FunctionReference<
        "query",
        "internal",
        {},
        Array<{
          args: Record<string, any>;
          functionHandle: string;
          id: string;
          name?: string;
          schedule:
            | { kind: "interval"; ms: number }
            | { cronspec: string; kind: "cron" };
        }>
      >;
      register: FunctionReference<
        "mutation",
        "internal",
        {
          args: Record<string, any>;
          functionHandle: string;
          name?: string;
          schedule:
            | { kind: "interval"; ms: number }
            | { cronspec: string; kind: "cron" };
        },
        string
      >;
    };
  };
};
