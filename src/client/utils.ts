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
import type { GenericId } from "convex/values";

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
  T extends GenericId<infer _T>
    ? string
    : T extends (infer U)[]
      ? OpaqueIds<U>[]
      : T extends object
        ? { [K in keyof T]: OpaqueIds<T[K]> }
        : T;

export type UseApi<API> = Expand<{
  [mod in keyof API]: API[mod] extends FunctionReference<
    infer FType,
    "public",
    infer FArgs,
    infer FReturnType,
    infer FComponentPath
  >
    ? FunctionReference<
        FType,
        "internal",
        OpaqueIds<FArgs>,
        OpaqueIds<FReturnType>,
        FComponentPath
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
