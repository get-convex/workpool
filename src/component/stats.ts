import { v } from "convex/values";
import { Doc } from "./_generated/dataModel";
import { query } from "./_generated/server";

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

export function recordStarted(work: Doc<"work">): string {
  return JSON.stringify({
    workId: work._id,
    event: "started",
    fnName: work.fnName,
    enqueuedAt: work._creationTime,
    startedAt: Date.now(),
    lagSinceEnqueued: Date.now() - work._creationTime,
  });
}

export function recordCompleted(
  work: Doc<"work">,
  status: "success" | "error" | "canceled" | "timeout"
): string {
  return JSON.stringify({
    workId: work._id,
    event: "completed",
    fnName: work.fnName,
    completedAt: Date.now(),
    status,
    lagSinceEnqueued: Date.now() - work._creationTime,
  });
}

/**
 * Warning: this should not be used from a mutation, as it will cause conflicts.
 * Use this to debug or diagnose your queue length when it's backed up.
 */
export const queueLength = query({
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
export const debugCounts = query({
  args: {},
  returns: v.any(),
  handler: async (ctx) => {
    /* eslint-disable @typescript-eslint/no-explicit-any */
    return {
      pendingStart: await (ctx.db.query("pendingStart") as any).count(),
      inProgressWork: await (ctx.db.query("inProgressWork") as any).count(),
      pendingCompletion: await (
        ctx.db.query("pendingCompletion") as any
      ).count(),
      pendingCancelation: await (
        ctx.db.query("pendingCancelation") as any
      ).count(),
    };
    /* eslint-enable @typescript-eslint/no-explicit-any */
  },
});
