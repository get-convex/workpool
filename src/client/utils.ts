import {
  type Expand,
  type FunctionArgs,
  type FunctionReference,
  type FunctionReturnType,
  type FunctionType,
  type FunctionVisibility,
  getFunctionAddress,
  getFunctionName,
} from "convex/server";
import type { GenericId, Value } from "convex/values";

/* Type utils follow */

export type RunQueryCtx = {
  runQuery: <Query extends FunctionReference<"query", "internal">>(
    query: Query,
    args: FunctionArgs<Query>
  ) => Promise<FunctionReturnType<Query>>;
};
export type RunMutationCtx = RunQueryCtx & {
  runMutation: <Mutation extends FunctionReference<"mutation", "internal">>(
    mutation: Mutation,
    args: FunctionArgs<Mutation>
  ) => Promise<FunctionReturnType<Mutation>>;
};

export type OpaqueIds<T> =
  T extends GenericId<string>
    ? string
    : T extends (infer U)[]
      ? OpaqueIds<U>[]
      : T extends Record<string, Value | undefined>
        ? { [K in keyof T]: OpaqueIds<T[K]> }
        : T;

export type UseApi<API> = Expand<{
  [mod in keyof API]: API[mod] extends FunctionReference<
    infer FType,
    "public",
    infer FArgs,
    infer FReturnType
  >
    ? FunctionReference<
        FType,
        "internal",
        OpaqueIds<FArgs>,
        OpaqueIds<FReturnType>
      >
    : UseApi<API[mod]>;
}>;

export function safeFunctionName(
  f: FunctionReference<FunctionType, FunctionVisibility>
) {
  const address = getFunctionAddress(f);
  return (
    address.name ||
    address.reference ||
    address.functionHandle ||
    getFunctionName(f)
  );
}
