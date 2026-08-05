import type {
  DocumentByName,
  GenericDataModel,
  GenericDatabaseWriter,
} from "convex/server";
import type { CommitTsPlaceholder, GenericId } from "convex/values";
import { toTimestamp } from "./time.js";
import type { QueueTables } from "./types.js";

/** Split a batch into items startable at `now` vs visible but not yet due. */
export function splitDue<Item extends { runAt?: number }>(
  items: Item[],
  now: number,
): { due: Item[]; notDue: Array<Item & { runAt: number }> } {
  const isDue = (item: Item) => item.runAt === undefined || item.runAt <= now;
  return {
    due: items.filter(isDue),
    notDue: items.filter(
      (item): item is Item & { runAt: number } => !isDue(item),
    ),
  };
}

/**
 * Moves entries that aren't due yet to sort at their `runAt` instead of the
 * commit timestamp they were enqueued with, so the cursor can pass them and
 * they don't come back until they're actually due. Only safe to compute from
 * the clock in the worker mutation: that transaction writes the cursor too,
 * so a time past `now` can't end up behind it.
 */
export async function promoteNotDue<
  DataModel extends GenericDataModel,
  Table extends QueueTables<DataModel>,
>(
  db: GenericDatabaseWriter<DataModel>,
  table: Table,
  notDue: Array<{ _id: GenericId<Table>; runAt: number }>,
): Promise<void> {
  await Promise.all(
    notDue.map(async ({ _id, runAt }) => {
      // A concurrent writer (e.g. a cancelation) may have removed it.
      if (!(await db.get(table, _id))) return;
      await db.patch(table, _id, {
        // Round up to the next whole millisecond: the cursor has already
        // reached `endOfMs(now)`, so a `runAt` part-way through the current
        // millisecond would truncate to a timestamp behind it.
        segment: toTimestamp(Math.ceil(runAt)),
      } as Partial<DocumentByName<DataModel, Table>>);
    }),
  );
}

/**
 * Take an item off its lane: delete it and return it, or return null if a
 * concurrent writer (e.g. a cancelation handled earlier in the same batch)
 * already removed it. Consume every item the cursor will advance past —
 * nothing rescans the lane behind the cursor, so a row left there would never
 * be read again.
 */
export async function consumeItem<
  DataModel extends GenericDataModel,
  Table extends QueueTables<DataModel>,
>(
  db: GenericDatabaseWriter<DataModel>,
  table: Table,
  id: GenericId<Table>,
): Promise<DocumentByName<DataModel, Table> | null> {
  const item = await db.get(table, id);
  if (item === null) {
    return null;
  }
  await db.delete(table, id);
  return item;
}

/**
 * Where a lane's cursor moves after processing a batch read in commit order:
 * the furthest segment in the leading run of handled items. Omit `handled`
 * when the whole batch was processed (the last entry is the furthest read).
 * When capacity cuts a batch short, pass the ids actually handled — stopping
 * at the first item left alone is what keeps it from being skipped.
 */
export function advanceCursor<Id extends string>(
  items: Array<{ _id: Id; segment: bigint | CommitTsPlaceholder }>,
  prev: bigint,
  handled?: ReadonlySet<Id>,
): bigint {
  let cursor = prev;
  for (const item of items) {
    if (handled !== undefined && !handled.has(item._id)) break;
    cursor = item.segment as bigint;
  }
  return cursor;
}
