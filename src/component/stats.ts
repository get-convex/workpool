import { Doc } from "./_generated/dataModel";

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

export function recordStarted(work: Doc<"work">) {
  console.log(
    JSON.stringify({
      workId: work._id,
      event: "started",
      fnName: work.fnName,
      enqueuedAt: work._creationTime,
      startedAt: Date.now(),
      lagSinceEnqueued: Date.now() - work._creationTime,
    })
  );
}

export function recordCompleted(
  work: Doc<"work">,
  status: "success" | "error" | "canceled" | "timeout"
) {
  console.log(
    JSON.stringify({
      workId: work._id,
      event: "completed",
      fnName: work.fnName,
      completedAt: Date.now(),
      status,
      lagSinceEnqueued: Date.now() - work._creationTime,
    })
  );
}
