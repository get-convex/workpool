import {
  type FunctionArgs,
  type FunctionReference,
  type FunctionReturnType,
  type FunctionType,
  type FunctionVisibility,
  getFunctionAddress,
  getFunctionName,
} from "convex/server";

/* Type utils follow */

export type RunQueryCtx = {
  runQuery: <Query extends FunctionReference<"query", "internal">>(
    query: Query,
    args: FunctionArgs<Query>,
  ) => Promise<FunctionReturnType<Query>>;
};
export type RunMutationCtx = RunQueryCtx & {
  runMutation: <Mutation extends FunctionReference<"mutation", "internal">>(
    mutation: Mutation,
    args: FunctionArgs<Mutation>,
  ) => Promise<FunctionReturnType<Mutation>>;
};

export function safeFunctionName(
  f: FunctionReference<FunctionType, FunctionVisibility>,
) {
  const address = getFunctionAddress(f);
  return (
    address.name ||
    address.reference ||
    address.functionHandle ||
    getFunctionName(f)
  );
}
