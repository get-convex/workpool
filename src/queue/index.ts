/**
 * A durable, scheduling-aware queue lane on Convex tables, extracted from
 * workpool. Designed to pair with `@convex-dev/batch-worker`, which drives
 * the loop that consumes it.
 *
 * A *lane* is a table (see `queueTable`) ordered by a commit-timestamp
 * `segment` field, scanned by a worker holding a cursor that never rewinds:
 *
 * - Producers call `insertItem` (optionally with a future `runAt`).
 * - The worker's work query reads eligible items with `queueQuery`/`peekQueue`
 *   past its cursor and uses `nextWakeup` for its idle hints.
 * - The worker mutation calls `splitDue`, processes the due items (consuming
 *   them with `consumeItem`), pushes the rest forward with `promoteNotDue`,
 *   and persists the cursor returned by `advanceCursor` — all in one
 *   transaction, which is what makes the whole scheme safe.
 *
 * The host owns the tables (this is a library, not a component), so items can
 * be deleted out-of-band (cancelation) at any time: readers simply won't see
 * them, and the consume/promote helpers tolerate the disappearance.
 */
export {
  boundScheduledTime,
  endOfMs,
  fromTimestamp,
  SAFE_FUTURE_MS,
  toTimestamp,
} from "./time.js";
export { queueTable } from "./table.js";
export { insertFromWorker, insertItem } from "./enqueue.js";
export { nextWakeup, peekQueue, queueQuery } from "./read.js";
export {
  advanceCursor,
  consumeItem,
  promoteNotDue,
  splitDue,
} from "./consume.js";
export type {
  QueueItem,
  QueueLogger,
  QueuePayload,
  QueueTables,
} from "./types.js";
