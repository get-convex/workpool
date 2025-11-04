import type { Infer } from "convex/values";

import { v } from "convex/values";
import { type Logger, logLevel } from "./logging.js";

export const fnType = v.union(
  v.literal("action"),
  v.literal("mutation"),
  v.literal("query"),
);

export const DEFAULT_MAX_PARALLELISM = 10;
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

export const config = v.object({
  maxParallelism: v.number(),
  logLevel,
});
export type Config = Infer<typeof config>;

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

export const vResultValidator = v.union(
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
export type RunResult = Infer<typeof vResultValidator>;

export const onComplete = v.object({
  fnHandle: v.string(), // mutation
  context: v.optional(v.any()),
});
export type OnComplete = Infer<typeof onComplete>;

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

/**
 * Returns the smaller of two bigint values.
 */
export function min<T extends bigint>(a: T, b: T): T {
  return a > b ? b : a;
}

/**
 * Returns the larger of two bigint values.
 */
export function max<T extends bigint>(a: T, b: T): T {
  return a < b ? b : a;
}
