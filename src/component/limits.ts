import type { TransactionLimits } from "convex/server";
import type { MutationCtx } from "./_generated/server.js";

// Leave space for the work/payload reads and deletes, the completion record,
// and waking the loop or scheduling failure completion. Byte reserves include
// room for a large document crossing the nested call's limit.
const COMPLETION_RESERVE = {
  bytesRead: 3 * 1024 * 1024,
  bytesWritten: 2 * 1024 * 1024,
  databaseQueries: 32,
  documentsRead: 32,
  documentsWritten: 16,
  functionsScheduled: 4,
  scheduledFunctionArgsBytes: 1024 * 1024,
} satisfies Required<TransactionLimits>;

export async function completionTransactionLimits(
  ctx: MutationCtx,
): Promise<TransactionLimits> {
  const metrics = await ctx.meta.getTransactionMetrics();
  const limits: TransactionLimits = {};
  for (const key of Object.keys(
    COMPLETION_RESERVE,
  ) as (keyof TransactionLimits)[]) {
    limits[key] = Math.max(0, metrics[key].remaining - COMPLETION_RESERVE[key]);
  }
  return limits;
}
