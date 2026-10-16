import type { Doc } from "./_generated/dataModel.js";
import type { QueryCtx } from "./_generated/server.js";

/** Follow the queue pointer, which may reference an entry already deleted. */
export async function findPendingStart(ctx: QueryCtx, work: Doc<"work">) {
  const doc = work.pendingStartId
    ? await ctx.db.get("pendingStart", work.pendingStartId)
    : null;
  return doc?.workId === work._id ? doc : null;
}
