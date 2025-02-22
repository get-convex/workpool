import { Infer } from "convex/values";

import { v } from "convex/values";
import { Logger, logLevel } from "./logging";

// TODO: p95 of mainLoop
const SEGMENT_MS = 125;

export function toSegment(ms: number): bigint {
  return BigInt(Math.floor(ms / SEGMENT_MS));
}

export function currentSegment(): bigint {
  return toSegment(Date.now());
}

export function nextSegment(): bigint {
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
// This ensures that the type satisfies the schema.
const _ = {} as RetryBehavior satisfies Infer<typeof retryBehavior>;

export const runResult = v.union(
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
  })
);
export type RunResult = Infer<typeof runResult>;

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
      attempt: v.number(),
    }),
    v.object({
      state: v.literal("running"),
      attempt: v.number(),
    }),
    v.object({
      state: v.literal("finished"),
    })
  )
);
export type Status = Infer<typeof status>;
