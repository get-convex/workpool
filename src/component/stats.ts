import { Id } from "./_generated/dataModel";

/**
 * Record stats about work execution. Intended to be queried by Axiom or Datadog.
 */

export function recordStarted(workId: Id<"pendingWork">, fnName: string, enqueuedAt: number, scheduledAt: number) {
  console.log({ workId, fnName, enqueuedAt, scheduledAt, startedAt: Date.now() });
}

export function recordCompleted(workId: Id<"pendingWork">, completedAt: number, status: "success" | "failure") {
  console.log({ workId, completedAt, status });
}
