import { defineTable, type TableDefinition } from "convex/server";
import {
  v,
  type ObjectType,
  type PropertyValidators,
  type VCommitTs,
  type VFloat64,
  type VObject,
} from "convex/values";

type QueueFieldValidators = {
  segment: VCommitTs;
  runAt: VFloat64<number | undefined, "optional">;
};

export type QueueTableDefinition<Fields extends PropertyValidators> =
  TableDefinition<
    VObject<
      ObjectType<Fields & QueueFieldValidators>,
      Fields & QueueFieldValidators
    >,
    { segment: ["segment", "_creationTime"] }
  >;

/**
 * Define a queue lane: your payload fields plus the ordering machinery.
 *
 * `segment` is when an entry becomes eligible to process, in nanoseconds since
 * the epoch — the scale a commit timestamp uses.
 *
 * Entries that are ready as soon as they land store `db.vars.commitTs`, which
 * resolves at commit time. That's what makes an index on this field scannable
 * with a cursor that never has to be rewound: an entry can't appear behind a
 * commit timestamp we've already read past. (`_creationTime` can't do this — it
 * is assigned when the mutation *starts*, so a slow mutation's row can land
 * behind rows that already committed and were read.)
 *
 * Entries that shouldn't be processed until later store that time directly,
 * which sorts after everything committed before then. `v.commitTs()` accepts
 * any int64, so both live in one field and one index. See `insertItem` for how
 * the two cases (and the unsafe gap between them) are chosen at enqueue.
 *
 * Chain further `.index(...)` calls on the result for payload-field lookups.
 */
export function queueTable<Fields extends PropertyValidators>(
  fields: Fields,
): QueueTableDefinition<Fields> {
  const table = defineTable({
    ...fields,
    segment: v.commitTs(),
    runAt: v.optional(v.number()),
  });
  // TS can't resolve field paths through the generic spread, so assert the
  // index call; the declared return type carries the real index shape.
  return table.index("segment", [
    "segment" as never,
  ]) as unknown as QueueTableDefinition<Fields>;
}
