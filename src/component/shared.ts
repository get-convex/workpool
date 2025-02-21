import { Infer } from "convex/values";

import { v } from "convex/values";

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

export const logLevel = v.union(
  v.literal("DEBUG"),
  v.literal("INFO"),
  v.literal("WARN"),
  v.literal("ERROR")
);
export type LogLevel = Infer<typeof logLevel>;

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
export type RetryBehavior = Infer<typeof retryBehavior>;

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
