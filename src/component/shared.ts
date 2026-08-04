import type { Infer } from "convex/values";

import { v } from "convex/values";
import { type Logger, logLevel } from "./logging.js";

export const fnType = v.union(
  v.literal("action"),
  v.literal("mutation"),
  v.literal("query"),
);

export const DEFAULT_MAX_PARALLELISM = 10;
/** The batch-worker queue name. A single workpool instance uses one queue. */
export const WORKER_NAME = "main";
const SEGMENT_MS = 100;
export const SECOND = 1000;
export const MINUTE = 60 * SECOND;
export const HOUR = 60 * MINUTE;
export const DAY = 24 * HOUR;
export const YEAR = 365 * DAY;

export function toSegment(ms: number): bigint {
  return BigInt(Math.floor(ms / SEGMENT_MS));
}

export function getCurrentSegment(): bigint {
  return toSegment(Date.now());
}

export function getNextSegment(): bigint {
  return toSegment(Date.now()) + 1n;
}

export function fromSegment(segment: bigint): number {
  return Number(segment) * SEGMENT_MS;
}

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
 * land after every commit timestamp the loop has already read. The time is
 * chosen when the enqueue *starts*, so the margin has to cover however long
 * that transaction then takes to commit. Five minutes is far longer than any
 * mutation can run, so a `runAt` beyond it cannot land in the past. Anything
 * nearer is written as `db.vars.commitTs` plus a `runAt` field, and the loop
 * moves it forward itself — see `promoteScheduled` in loop.ts.
 */
export const SAFE_FUTURE_MS = 5 * MINUTE;

export const vConfig = v.object({
  maxParallelism: v.number(),
  logLevel,
});
export type Config = Infer<typeof vConfig>;

export const retryBehavior = v.object({
  maxAttempts: v.number(),
  initialBackoffMs: v.number(),
  base: v.number(),
});
export type RetryBehavior = {
  /**
   * The maximum number of attempts to make. 2 means one retry.
   */
  maxAttempts: number;
  /**
   * The initial backoff time in milliseconds. 100 means wait 100ms before the
   * first retry.
   */
  initialBackoffMs: number;
  /**
   * The base for the backoff. 2 means double the backoff each time.
   * e.g. if the initial backoff is 100ms, and the base is 2, then the first
   * retry will wait 200ms, the second will wait 400ms, etc.
   */
  base: number;
};
// Attempts will run with delay [0, 250, 500, 1000, 2000] (ms)
export const DEFAULT_RETRY_BEHAVIOR: RetryBehavior = {
  maxAttempts: 5,
  initialBackoffMs: 250,
  base: 2,
};
// This ensures that the type satisfies the schema.
const _ = {} as RetryBehavior satisfies Infer<typeof retryBehavior>;

export const vResult = v.union(
  v.object({
    kind: v.literal("success"),
    returnValue: v.any(),
  }),
  v.object({
    kind: v.literal("failed"),
    error: v.string(),
  }),
  v.object({
    kind: v.literal("canceled"),
  }),
);
export type RunResult = Infer<typeof vResult>;

export const vOnCompleteFnContext = v.object({
  fnHandle: v.string(), // mutation
  context: v.optional(v.any()),
});

export type OnCompleteArgs = {
  /**
   * The ID of the work that completed.
   */
  workId: string;
  /**
   * The context object passed when enqueuing the work.
   * Useful for passing data from the enqueue site to the onComplete site.
   */
  context: unknown;
  /**
   * The result of the run that completed.
   */
  result: RunResult;
};

export const status = v.union(
  v.union(
    v.object({
      state: v.literal("pending"),
      previousAttempts: v.number(),
    }),
    v.object({
      state: v.literal("running"),
      previousAttempts: v.number(),
    }),
    v.object({
      state: v.literal("finished"),
    }),
  ),
);
export type Status = Infer<typeof status>;

export function boundScheduledTime(ms: number, console: Logger): number {
  if (ms < Date.now() - YEAR) {
    console.error("scheduled time is too old, defaulting to now", ms);
    return Date.now();
  }
  if (ms > Date.now() + 4 * YEAR) {
    console.error(
      "scheduled time is too far in the future, defaulting to 1 year from now",
      ms,
    );
    return Date.now() + YEAR;
  }
  return ms;
}
