/* eslint-disable */
/**
 * Generated `api` utility.
 *
 * THIS CODE IS AUTOMATICALLY GENERATED.
 *
 * To regenerate, run `npx convex dev`.
 * @module
 */

import type * as batchActions from "../batchActions.js";
import type * as crons from "../crons.js";
import type * as example from "../example.js";
import type * as pipeline from "../pipeline.js";
import type * as setup from "../setup.js";
import type * as standardActions from "../standardActions.js";
import type * as test from "../test.js";

import type {
  ApiFromModules,
  FilterApi,
  FunctionReference,
} from "convex/server";

declare const fullApi: ApiFromModules<{
  batchActions: typeof batchActions;
  crons: typeof crons;
  example: typeof example;
  pipeline: typeof pipeline;
  setup: typeof setup;
  standardActions: typeof standardActions;
  test: typeof test;
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
  standardPool: import("@convex-dev/workpool/_generated/component.js").ComponentApi<"standardPool">;
  batchPool: import("@convex-dev/workpool/_generated/component.js").ComponentApi<"batchPool">;
};
