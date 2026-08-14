# Changelog

## Unreleased

- Orders the pending-work queues by commit timestamp (`v.commitTs()`, Convex ≥
  1.43). A commit timestamp is assigned when the transaction commits, so nothing
  can appear behind a cursor the main loop has already read past. That removes
  the 15-second cursor rewind buffer and the once-a-minute full rescan that
  0.4.7 added to cope with out-of-order inserts — the loop now reads only rows
  it hasn't seen, which improves throughput and tail latency for saturated
  pools.
- The `segment` fields keep their name, but now hold nanoseconds rather than
  100ms buckets. Work scheduled to start later stores its start time there
  directly, so ready and scheduled work share an ordering, scheduled work sorts
  above the loop's read bound until it's due, and a bulk enqueue of scheduled
  work never sits in front of ready work.
- A scheduled start time can commit _behind_ the loop's cursor (when the enqueue
  takes longer to commit than the delay), where the ordered scan would never see
  it. Every entry keyed by a wall-clock time also records its commit timestamp
  in a `scheduledAt` field; the loop sweeps that index in commit order — which
  nothing can land behind — inspects each entry exactly once, and directly
  starts the rare entry the cursor passed over. No entry is ever rewritten.
- The loop's cursor never advances past the newest commit timestamp it has
  observed, so a scheduled entry starting at its wall-clock time can't push the
  cursor ahead of a racing enqueue's commit — the design makes no assumptions
  about how wall clocks relate to the commit timestamp clock.
- Upgrading in place is safe, including for work that hasn't come due yet. A
  `pendingStart` an older version wrote holds a 100ms bucket — eight orders of
  magnitude below a nanosecond timestamp — so the loop recognizes it, reads the
  bucket back as the time the work should start, and either starts it or re-keys
  it as a timestamp and leaves it alone until then. This is a one-time
  re-ordering of every queued entry, 64 per loop iteration, so right after the
  push a large backlog of scheduled work adds processing overhead and can
  briefly delay work that's ready now. Queued completions and cancelations sort
  first and drain immediately, which is what they want.
- This change is not backwards compatible. It requires `convex` 1.43 or later,
  and downgrading a workpool that has run this version is not supported: an
  older version would read a nanosecond timestamp as a 100ms bucket far in the
  future and never start the work.

## 0.4.9

- Runs actions and queries in batches of up to 32 from a single scheduled
  action, instead of scheduling them individually. This reduces scheduled
  function use and reduces the number of actions used (previously each action
  had another action wrapping it).

## 0.4.8

- Reduces "generation mismatch" errors and unnecessary "kick"s from healthchecks

## 0.4.7

- Reduces database conflict retries (OCC conflicts) from enqueuing or completing
  work while tasks are being dispatched, improving throughput for workpools at
  scale (uses a "snapshot query").
- Changes the out-of-order commit buffer to 15 seconds from 30 to reduce the
  number of "tombstones" read while finding new work, but checks for older work
  once a minute.
- Fixes a race condition where a task could get recovered twice if the scheduler
  is many minutes behind.
- Allows throwing NonRetryableError to prevent retries.
- Limits the batch size of starting work to 64: this doesn't limit how many
  in-flight tasks there can be, just many can be started from one main loop
  iteration. This enables setting a much higher parallelism limit on larger
  deployments, without risking reading too much data in one transaction /
  slowing down transactions.

## 0.4.6

- Fails gracefully if the work being started has already been deleted. It will
  delete the pendingStart entry and continue

## 0.4.5

- Reverts recovering work from the scheduler for now

## 0.4.4

- Improves `register` type for `convex-test@0.0.43` compatibility

## 0.4.3

- Attempts to first run the completion handler inline in runActionWrapper to log
  errors, and then schedule it if it fails.
- Cools down status changes in the workpool so it stays running and polls for 5s
  in 100ms increments to avoid conflicts
- Doesn't kick the main loop from completion unless it's scheduled & saturated
- Recovers and re-enqueues work that may be stuck retrying with long backoffs in
  the scheduler

## 0.4.2

- Schedules recovery in batches when there are many old jobs in flight

## 0.4.1

- Logs the scheduled function ID in the "started" event for better debugging of
  delayed / slow executions.

## 0.4.0

- Stores args & onComplete.context separately in "payloads" when they are
  > 8kb, and enforces < 1MB for args+context storage.
- Breaks up batch enqueue calls based on args & context sizes.
- Iterates through completions, recovery, cancelation, etc. to avoid reading too
  much data.
- Lazily loads args before executing functions, if they were >8kb.
- Note: the schema is backwards-compatible, but if you want to go back to an
  older version of the code, you'll need to either use 0.3.2 or clear out any
  work items that are using "payloads"

## 0.3.2

- Adds forwards-compatible schema for upcoming args storage in "payloads"
- Renames "recover" to "healthcheck"
- Fix report generation when maxParallelism is 0

## 0.3.1

- Only warn if the limit is set to >100
- Allow setting maxParallelism to 0 to pause the workpool
- Allow updating configs like maxParallelism directly via function calls, and
  allow enqueueing without specifying maxParallelism, to inherit the current
  config. Note: if configs are specified on the Workpool class, each call will
  overwrite the current config.

## 0.3.0

- Move definition of retry default next to retry type.
- Adds /test and /\_generated/component.js entrypoints
- Drops commonjs support
- Improves source mapping for generated files
- Changes to a statically generated component API

## 0.2.19

- Expose a /test entrypoint to make testing registration easier.
- Update the packaging structure.
- Allow using static type generation and passing onComplete handlers without
  type errors from the branded string being stripped.
- Allow limiting how many jobs are canceled at once.

## 0.2.18

- Add batch enqueue and status functions.
- Improved the vOnCompleteArgs type helper to replace vOnCompleteValidator
- Reduce contention if the main loop is about to run.
- Passing a context is optional in the helper function
- Stop storing the return value in the pendingCompletions table, as success
  always passes the value directly to the call today.
- You can enqueue a function handle (e.g. to call a Component function directly
- Allows running workpool functions directly in a Workflow
