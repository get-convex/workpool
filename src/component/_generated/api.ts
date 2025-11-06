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
import { anyApi, componentsGeneric } from "convex/server";

const fullApi: ApiFromModules<{
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
}> = anyApi as any;

/**
 * A utility for referencing Convex functions in your app's public API.
 *
 * Usage:
 * ```js
 * const myFunctionReference = api.myModule.myFunction;
 * ```
 */
export const api: FilterApi<
  typeof fullApi,
  FunctionReference<any, "public">
> = anyApi as any;

/**
 * A utility for referencing Convex functions in your app's internal API.
 *
 * Usage:
 * ```js
 * const myFunctionReference = internal.myModule.myFunction;
 * ```
 */
export const internal: FilterApi<
  typeof fullApi,
  FunctionReference<any, "internal">
> = anyApi as any;

export const components = componentsGeneric() as unknown as {};
