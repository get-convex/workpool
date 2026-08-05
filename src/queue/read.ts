import type {
  DocumentByName,
  GenericDataModel,
  GenericDatabaseReader,
  NamedTableInfo,
  OrderedQuery,
  QueryInitializer,
} from "convex/server";
import { endOfMs, fromTimestamp } from "./time.js";
import type { QueueTables } from "./types.js";

// The shape every `queueTable` shares, used to type the index scan internally.
// The public signatures stay in terms of the host's data model; the casts in
// and out are sound because `queueTable` always defines this field and index.
type SegmentIndexInfo = {
  document: { segment: bigint };
  fieldPaths: "segment";
  indexes: { segment: ["segment", "_creationTime"] };
  searchIndexes: Record<string, never>;
  vectorIndexes: Record<string, never>;
};

/**
 * The index scan a worker uses to read a lane: everything past `cursor`,
 * oldest first, in commit order. Bound it by `eligibleBefore` (usually
 * `endOfMs(now)`) on lanes that can hold future-scheduled items; leave it off
 * for lanes whose items are always ready on commit. Chain
 * `.filter`/`.take`/`.paginate` on the result.
 */
export function queueQuery<
  DataModel extends GenericDataModel,
  Table extends QueueTables<DataModel>,
>(
  db: GenericDatabaseReader<DataModel>,
  table: Table,
  opts: { cursor: bigint; eligibleBefore?: bigint },
): OrderedQuery<NamedTableInfo<DataModel, Table>> {
  const init = db.query(table) as unknown as QueryInitializer<SegmentIndexInfo>;
  const query = init.withIndex("segment", (q) => {
    const past = q.gte("segment", opts.cursor);
    return opts.eligibleBefore === undefined
      ? past
      : past.lt("segment", opts.eligibleBefore);
  });
  return query as unknown as OrderedQuery<NamedTableInfo<DataModel, Table>>;
}

/** Everything eligible at `now`, oldest first, past the cursor. */
export async function peekQueue<
  DataModel extends GenericDataModel,
  Table extends QueueTables<DataModel>,
>(
  db: GenericDatabaseReader<DataModel>,
  table: Table,
  opts: { cursor: bigint; limit: number; now?: number },
): Promise<Array<DocumentByName<DataModel, Table>>> {
  return queueQuery(db, table, {
    cursor: opts.cursor,
    eligibleBefore: endOfMs(opts.now ?? Date.now()),
  }).take(opts.limit);
}

/**
 * How long until the earliest future-scheduled item becomes eligible, or
 * undefined if nothing is scheduled past `now`. Feed into the work query's
 * idle `timeoutMs` hint so the loop wakes when the work comes due.
 */
export async function nextWakeup<
  DataModel extends GenericDataModel,
  Table extends QueueTables<DataModel>,
>(
  db: GenericDatabaseReader<DataModel>,
  table: Table,
  now: number,
): Promise<number | undefined> {
  const next = await queueQuery(db, table, { cursor: endOfMs(now) }).first();
  if (next === null) {
    return undefined;
  }
  return fromTimestamp(next.segment as bigint) - now;
}
