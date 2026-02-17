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
import type * as test_run from "../test/run.js";
import type * as test_scenarios_bigArgs from "../test/scenarios/bigArgs.js";
import type * as test_work from "../test/work.js";

import type {
  ApiFromModules,
  FilterApi,
  FunctionReference,
} from "convex/server";

declare const fullApi: ApiFromModules<{
  crons: typeof crons;
  example: typeof example;
  "test/run": typeof test_run;
  "test/scenarios/bigArgs": typeof test_scenarios_bigArgs;
  "test/work": typeof test_work;
}>;

/**
 * A utility for referencing Convex functions in your app's public API.
 *
 * Usage:
 * ```js
 * const myFunctionReference = api.myModule.myFunction;
 * ```
 */
export declare const api: FilterApi<
  typeof fullApi,
  FunctionReference<any, "public">
>;

/**
 * A utility for referencing Convex functions in your app's internal API.
 *
 * Usage:
 * ```js
 * const myFunctionReference = internal.myModule.myFunction;
 * ```
 */
export declare const internal: FilterApi<
  typeof fullApi,
  FunctionReference<any, "internal">
>;

export declare const components: {
  smallPool: import("@convex-dev/workpool/_generated/component.js").ComponentApi<"smallPool">;
  bigPool: import("@convex-dev/workpool/_generated/component.js").ComponentApi<"bigPool">;
  serializedPool: import("@convex-dev/workpool/_generated/component.js").ComponentApi<"serializedPool">;
  testWorkpool: import("@convex-dev/workpool/_generated/component.js").ComponentApi<"testWorkpool">;
};
