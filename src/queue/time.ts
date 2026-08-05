import type { QueueLogger } from "./types.js";

const SECOND = 1000;
const MINUTE = 60 * SECOND;
const YEAR = 365 * 24 * 60 * MINUTE;

// A commit timestamp is nanoseconds since the epoch, so a wall-clock time
// converts into the same ordering as one. This is what lets a single index hold
// both "ready as soon as it commits" and "not before this time".
const NS_PER_MS = 1_000_000n;

/**
 * A wall-clock time on the commit-timestamp scale. `Date.now()` is whole
 * milliseconds in Convex, so nothing is lost converting it; the floor only
 * rounds a caller-supplied fractional `runAt`, and never upward, so work can't
 * be held past its time.
 */
export function toTimestamp(ms: number): bigint {
  return BigInt(Math.floor(ms)) * NS_PER_MS;
}

/** Back to whole milliseconds, the resolution `Date.now()` reports. */
export function fromTimestamp(timestamp: bigint): number {
  return Number(timestamp / NS_PER_MS);
}

/**
 * The exclusive upper bound on entries eligible at `ms` — the end of that
 * millisecond, not the start of it.
 *
 * The clock and the timestamps it gets compared against have different
 * resolutions: `Date.now()` is whole milliseconds, but a commit timestamp is
 * not. A row committing during millisecond `now` lands *above*
 * `toTimestamp(now)` (a transaction's timestamp measures ~0.2ms above its own
 * `Date.now()`), so bounding there would skip everything that committed this
 * millisecond — thousands of rows under load. Scheduled times are whole
 * milliseconds, so extending to the end of the millisecond can't let
 * not-yet-due work through.
 */
export function endOfMs(ms: number): bigint {
  return toTimestamp(Math.floor(ms) + 1);
}

/**
 * How far ahead a caller-supplied `runAt` has to be before we can write it
 * straight into the ordering field.
 *
 * Writing a wall-clock time there is only safe if the value is guaranteed to
 * land after every commit timestamp the worker has already read. The time is
 * chosen when the enqueue *starts*, so the margin has to cover however long
 * that transaction then takes to commit. Five minutes is far longer than any
 * mutation can run, so a `runAt` beyond it cannot land in the past. Anything
 * nearer is written as `db.vars.commitTs` plus a `runAt` field, and the worker
 * moves it forward itself — see `promoteNotDue`.
 */
export const SAFE_FUTURE_MS = 5 * MINUTE;

/** Clamp a scheduled time to a sane window around now. */
export function boundScheduledTime(ms: number, console?: QueueLogger): number {
  const log = console ?? globalThis.console;
  if (ms < Date.now() - YEAR) {
    log.error("scheduled time is too old, defaulting to now", ms);
    return Date.now();
  }
  if (ms > Date.now() + 4 * YEAR) {
    log.error(
      "scheduled time is too far in the future, defaulting to 1 year from now",
      ms,
    );
    return Date.now() + YEAR;
  }
  return ms;
}
