import type {
  DocumentByName,
  GenericDataModel,
  GenericDatabaseWriter,
  WithoutSystemFields,
} from "convex/server";
import type { GenericId } from "convex/values";
import { boundScheduledTime, SAFE_FUTURE_MS, toTimestamp } from "./time.js";
import type { QueueLogger, QueuePayload, QueueTables } from "./types.js";

/**
 * Insert an item into a queue lane, ordered so a scanning cursor can never
 * lose it.
 *
 * Ready now: ordered by this transaction's commit timestamp. Far enough out
 * that no commit latency could put it behind the worker's cursor: ordered by
 * its start time directly. In between: ordered by the commit timestamp so it
 * can't be lost, with `runAt` telling the worker to move it forward once it
 * can do so safely (see `promoteNotDue`) — one extra write, only for
 * near-future work.
 */
export async function insertItem<
  DataModel extends GenericDataModel,
  Table extends QueueTables<DataModel>,
>(
  db: GenericDatabaseWriter<DataModel>,
  table: Table,
  payload: QueuePayload<DataModel, Table>,
  opts?: { runAt?: number; console?: QueueLogger },
): Promise<GenericId<Table>> {
  const runAt =
    opts?.runAt === undefined
      ? undefined
      : boundScheduledTime(opts.runAt, opts.console);
  const delayMs = runAt === undefined ? 0 : runAt - Date.now();
  return db.insert(table, {
    ...payload,
    segment: delayMs > SAFE_FUTURE_MS ? toTimestamp(runAt!) : db.vars.commitTs,
    ...(delayMs > 0 ? { runAt } : {}),
  } as unknown as WithoutSystemFields<DocumentByName<DataModel, Table>>);
}

/**
 * Insert an item from the worker mutation itself — the transaction that owns
 * the lane's cursor. A future time can go straight into `segment` however
 * near it is: this transaction also writes the cursor, so a time past now is
 * certain to sort ahead of it. No `runAt` field needed — `segment` already is
 * the start time. Use for retries/backoff scheduled by the worker.
 */
export async function insertFromWorker<
  DataModel extends GenericDataModel,
  Table extends QueueTables<DataModel>,
>(
  db: GenericDatabaseWriter<DataModel>,
  table: Table,
  payload: QueuePayload<DataModel, Table>,
  opts?: { runAt?: number },
): Promise<GenericId<Table>> {
  return db.insert(table, {
    ...payload,
    segment:
      opts?.runAt === undefined ? db.vars.commitTs : toTimestamp(opts.runAt),
  } as unknown as WithoutSystemFields<DocumentByName<DataModel, Table>>);
}
