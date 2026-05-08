import {
  type FunctionReference,
  type FunctionReturnType,
  type OptionalRestArgs,
  getFunctionAddress,
} from "convex/server";
import { convexToJson, jsonToConvex } from "convex/values";

declare const Convex: {
  asyncSyscall: (op: string, jsonArgs: string) => Promise<string>;
};

/**
 * Run a query without creating a read dependency. Concurrent writes to the
 * data the query reads will NOT cause the calling mutation to retry via OCC.
 *
 * Tradeoff: a concurrent transaction that hasn't yet committed at snapshot
 * time may insert data this query won't see. If missing such inserts could
 * break correctness, use ctx.runQuery (which takes a dependency) instead.
 */
export async function runSnapshotQuery<
  Query extends FunctionReference<"query", "public" | "internal">,
>(
  query: Query,
  ...args: OptionalRestArgs<Query>
): Promise<FunctionReturnType<Query>> {
  const queryArgs = (args[0] ?? {}) as Record<string, unknown>;
  const syscallArgs = {
    udfType: "snapshotQuery",
    args: convexToJson(queryArgs as never),
    ...getFunctionAddress(query),
  };
  const resultStr = await Convex.asyncSyscall(
    "1.0/runUdf",
    JSON.stringify(syscallArgs),
  );
  return jsonToConvex(JSON.parse(resultStr)) as FunctionReturnType<Query>;
}
