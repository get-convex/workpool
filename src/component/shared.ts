import type { Infer, Validator, VAny } from "convex/values";

import { jsonToConvex, v } from "convex/values";
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

// Decode the 100ms buckets used before commit-timestamp ordering.
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
  return (
    BigInt(whole) * NS_PER_MS +
    BigInt(Math.round((ms - whole) * Number(NS_PER_MS)))
  );
}

/**
 * Back to (possibly fractional) milliseconds, dividing the whole and
 * remainder parts separately so large values don't lose precision.
 */
export function fromTimestamp(timestamp: bigint): number {
  return (
    Number(timestamp / NS_PER_MS) +
    Number(timestamp % NS_PER_MS) / Number(NS_PER_MS)
  );
}

declare const Convex: {
  syscall: (op: string, jsonArgs: string) => string;
};

/**
 * The snapshot this transaction reads at, in nanoseconds on the
 * commit-timestamp clock. Everything stamped at or below it is visible here,
 * and everything that commits later is stamped above it.
 *
 * TODO(convex): replace with `ctx.meta.getSnapshotTs()` once a released
 * `convex` exposes it; this is the syscall it wraps.
 */
export function snapshotTs(): bigint {
  const json = Convex.syscall("1.0/getSnapshotTs", "{}");
  return jsonToConvex(JSON.parse(json)) as bigint;
}

export function maxBigint(a: bigint, b: bigint): bigint {
  return a > b ? a : b;
}

/** Work is eligible when either the snapshot or the wall clock reaches its key. */
export function eligibilityBound(): bigint {
  return maxBigint(snapshotTs(), toTimestamp(Date.now()));
}

// Separates legacy 100ms buckets from timestamps within the scheduling bounds.
export const MIN_TIMESTAMP = toTimestamp(Date.UTC(2000, 0, 1));

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
  excludeKinds: v.optional(
    v.array(
      v.union(v.literal("success"), v.literal("failed"), v.literal("canceled")),
    ),
  ),
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
