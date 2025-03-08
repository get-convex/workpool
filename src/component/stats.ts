import { v } from "convex/values";
import { Doc } from "./_generated/dataModel.js";
import { internalQuery } from "./_generated/server.js";
import { DEFAULT_MAX_PARALLELISM } from "./shared.js";
import { Logger } from "./logging.js";

/**
 * Record stats about work execution. Intended to be queried by Axiom or Datadog.
 */

/**
 * Sample axiom dashboard query:

workpool
| extend parsed_message = iff(
	isnotnull(parse_json(trim("'", tostring(["data.message"])))),
	parse_json(trim("'", tostring(["data.message"]))),
	parse_json('{}')
)
| extend lagSinceEnqueued = parsed_message["lagSinceEnqueued"]
| extend fnName = parsed_message["fnName"]
| summarize avg(todouble(lagSinceEnqueued)) by bin_auto(_time), tostring(fnName)

 */

export function recordStarted(
  console: Logger,
  work: Doc<"work">,
  lagMs: number
) {
  console.event("started", {
    workId: work._id,
    fnName: work.fnName,
    enqueuedAt: work._creationTime,
    startedAt: Date.now(),
    startLag: lagMs,
  });
}

export function recordCompleted(
  console: Logger,
  work: Doc<"work">,
  status: "success" | "failed" | "canceled" | "retrying"
) {
  console.event("completed", {
    workId: work._id,
    fnName: work.fnName,
    completedAt: Date.now(),
    attempts: work.attempts,
    status,
  });
}

export function recordReport(console: Logger, state: Doc<"internalState">) {
  const { completed, succeeded, failed, retries, canceled } = state.report;
  const withoutRetries = completed - retries;
  console.event("report", {
    completed,
    succeeded,
    failed,
    retries,
    canceled,
    failureRate: completed ? (failed + retries) / completed : 0,
    permanentFailureRate: withoutRetries ? failed / withoutRetries : 0,
  });
}

/**
 * Warning: this should not be used from a mutation, as it will cause conflicts.
 * Use this to debug or diagnose your queue length when it's backed up.
 */
export const queueLength = internalQuery({
  args: {},
  returns: v.number(),
  handler: async (ctx) => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return (ctx.db.query("pendingStart") as any).count();
  },
});

/**
 * Warning: this should not be used from a mutation, as it will cause conflicts.
 * Use this while developing to see the state of the queue.
 */
export const diagnostic = internalQuery({
  args: {},
  returns: v.any(),
  handler: async (ctx) => {
    const global = await ctx.db.query("globals").unique();
    const internalState = await ctx.db.query("internalState").unique();
    const inProgressWork = internalState?.running.length ?? 0;
    const maxParallelism = global?.maxParallelism ?? DEFAULT_MAX_PARALLELISM;
    /* eslint-disable @typescript-eslint/no-explicit-any */
    const pendingStart = await (ctx.db.query("pendingStart") as any).count();
    const pendingCompletion = await (
      ctx.db.query("pendingCompletion") as any
    ).count();
    const pendingCancelation = await (
      ctx.db.query("pendingCancelation") as any
    ).count();
    const runStatus = await ctx.db.query("runStatus").unique();
    /* eslint-enable @typescript-eslint/no-explicit-any */
    return {
      canceling: pendingCancelation,
      waiting: pendingStart,
      running: inProgressWork - pendingCompletion,
      completing: pendingCompletion,
      spareCapacity: maxParallelism - inProgressWork,
      runStatus: runStatus?.state.kind,
      generation: internalState?.generation,
    };
  },
});
