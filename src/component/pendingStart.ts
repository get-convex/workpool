import type { Doc, Id } from "./_generated/dataModel.js";
import type { MutationCtx, QueryCtx } from "./_generated/server.js";

/** Members of a packed or legacy single-work queue document. */
export function memberIds(
  doc: Pick<Doc<"pendingStart">, "workIds" | "workId">,
): Id<"work">[] {
  return doc.workIds ?? (doc.workId ? [doc.workId] : []);
}

/** Follow the pointer and reject stale membership. */
export async function findPendingStart(ctx: QueryCtx, work: Doc<"work">) {
  const doc = work.pendingStartId
    ? await ctx.db.get("pendingStart", work.pendingStartId)
    : null;
  return doc && memberIds(doc).includes(work._id) ? doc : null;
}

/** Remove only these members, deleting the document when empty. */
export async function removeFromPendingStart(
  ctx: MutationCtx,
  doc: Doc<"pendingStart">,
  workIds: Id<"work">[],
) {
  const remaining = memberIds(doc).filter((id) => !workIds.includes(id));
  if (remaining.length === 0) {
    await ctx.db.delete("pendingStart", doc._id);
  } else {
    await ctx.db.patch("pendingStart", doc._id, {
      workIds: remaining,
      workId: undefined,
    });
  }
}
