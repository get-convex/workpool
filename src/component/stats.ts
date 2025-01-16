import { Id } from "./_generated/dataModel";

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

// // XXX reintroduce stats

// export function recordStarted(
//   workId: Id<"pendingWork">,
//   fnName: string,
//   enqueuedAt: number,
//   runAtTime: number
// ) {
//   console.log(
//     JSON.stringify({
//       workId,
//       event: "started",
//       fnName,
//       enqueuedAt,
//       runAtTime,
//       startedAt: Date.now(),
//       lagSinceEnqueued: Date.now() - enqueuedAt,
//     })
//   );
// }

// export function recordCompleted(
//   workId: Id<"pendingWork">,
//   status: "success" | "failure" | "canceled"
// ) {
//   console.log(JSON.stringify({ workId, completedAt: Date.now(), status }));
// }
