import { Infer } from "convex/values";

import { v } from "convex/values";

// TODO: p95 of mainLoop
const WHEEL_SEGMENT_MS = 125;

export function toWheelSegment(ms: number): bigint {
  return BigInt(Math.floor(ms / WHEEL_SEGMENT_MS));
}

export function currentWheelSegment(): bigint {
  return toWheelSegment(Date.now());
}

export function nextWheelSegment(): bigint {
  return toWheelSegment(Date.now()) + 1n;
}

export function fromWheelSegment(segment: bigint): number {
  return Number(segment) * WHEEL_SEGMENT_MS;
}

export const completionStatus = v.union(
  v.literal("success"),
  v.literal("error"),
  v.literal("canceled"),
  v.literal("timeout")
);
export type CompletionStatus = Infer<typeof completionStatus>;

export const logLevel = v.union(
  v.literal("DEBUG"),
  v.literal("INFO"),
  v.literal("WARN"),
  v.literal("ERROR")
);
export type LogLevel = Infer<typeof logLevel>;

export const config = v.object({
  maxParallelism: v.number(),
  logLevel: v.optional(logLevel),
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
    type: v.literal("success"),
    returnValue: v.any(),
  }),
  v.object({
    type: v.literal("failed"),
    error: v.string(),
  }),
  v.object({
    type: v.literal("canceled"),
  }),
  v.object({
    type: v.literal("timeout"),
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
   * The ID of the run that completed.
   */
  runId: string;
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
