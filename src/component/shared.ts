import type { Infer, Validator, VAny } from "convex/values";

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

// ── 100ms segment buckets ────────────────────────────────────────────────
// The unit versions ≤ 0.4.9 ordered the pending queues by. Kept for exactly
// two purposes: pacing the periodic stuck-job check (`lastRecovery`), and
// decoding the buckets those versions left in `pendingStart.segment` (see
// `legacyRunAt`). Never use these for ordering keys — keys are nanoseconds on
// the commit-timestamp scale (see `toTimestamp`).

export function toSegment(ms: number): bigint {
  return BigInt(Math.floor(ms / SEGMENT_MS));
}

export function getCurrentSegment(): bigint {
  return toSegment(Date.now());
}

export function fromSegment(segment: bigint): number {
  return Number(segment) * SEGMENT_MS;
}

// A commit timestamp is nanoseconds since the epoch, so a wall-clock time
// converts into the same ordering as one. This is what lets a single index hold
// both "ready as soon as it commits" and "not before this time".
const NS_PER_MS = 1_000_000n;

/**
 * A wall-clock time on the commit-timestamp scale, preserving any fractional
 * milliseconds exactly: the whole and fractional parts convert separately, so
 * no precision is lost multiplying a large float. Round-trips through
 * `fromTimestamp`.
 */
export function toTimestamp(ms: number): bigint {
  const whole = Math.floor(ms);
  return BigInt(whole) * NS_PER_MS + BigInt(Math.round((ms - whole) * 1e6));
}

/**
 * Back to (possibly fractional) milliseconds, dividing the whole and
 * remainder parts separately so large values don't lose precision.
 */
export function fromTimestamp(timestamp: bigint): number {
  return Number(timestamp / NS_PER_MS) + Number(timestamp % NS_PER_MS) / 1e6;
}

/**
 * A start time as an ordering value: rounded up to the next whole millisecond,
 * the first one in which the work is due for the whole millisecond's duration.
 * Rounding down would start a fractionally-scheduled entry before its time
 * (due-ness is visibility, and the read bound moves in whole milliseconds).
 * If sub-millisecond earliness stops mattering — e.g. once near-future starts
 * are clamped to "now" — this could carry the fraction instead.
 */
export function dueTimestamp(runAt: number): bigint {
  return toTimestamp(Math.ceil(runAt));
}

/**
 * The exclusive upper bound on entries eligible at `ms` — the end of that
 * millisecond, not the start of it.
 */
export function endOfMs(ms: number): bigint {
  return toTimestamp(Math.floor(ms) + 1);
}

// Nanoseconds for the year 2000: far above any 100ms bucket an older version
// could have written (~1.8e10 today, ~1.9e10 even four years out) and far below
// any timestamp this one can produce, since `boundScheduledTime` keeps
// scheduled times within a few years of now. Nothing real lands in between.
const MIN_TIMESTAMP = toTimestamp(Date.UTC(2000, 0, 1));

/**
 * The start time a `pendingStart` written before commit-timestamp ordering
 * represents, or undefined if its `segment` is already a timestamp.
 *
 * Those entries stored `max(toSegment(runAt), toSegment(now))` and had no
 * `runAt` field, so the bucket is the only record of when the work should
 * start — and it may still be in the future. Reading it back as a time lets the
 * loop treat such an entry like any other scheduled one, rather than seeing a
 * value far below the eligibility bound and starting it immediately.
 */
export function legacyRunAt(segment: bigint): number | undefined {
  return segment < MIN_TIMESTAMP ? fromSegment(segment) : undefined;
}

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

export const vResult = vRunResult(v.any());
export function vRunResult<RV extends Validator<any, any, any> = VAny>(
  returnValue: RV,
) {
  return v.union(
    v.object({
      kind: v.literal("success"),
      returnValue,
    }),
    v.object({
      kind: v.literal("failed"),
      error: v.string(),
    }),
    v.object({
      kind: v.literal("canceled"),
    }),
  );
}
export type RunResult<Returns = unknown> =
  | {
      kind: "success";
      /**
       * The return value of the run, if it succeeded.
       */
      returnValue: Returns;
    }
  | {
      kind: "failed";
      /**
       * The error message of the run, if it failed.
       */
      error: string;
    }
  | {
      kind: "canceled";
    };

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
