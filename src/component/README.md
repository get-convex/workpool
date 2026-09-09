# Workpool: implementation notes and high-level architecture

- `segment`: a nanosecond timestamp. Immediate work uses its enqueue commit
  timestamp; scheduled work uses its start time.
- Cursors: inclusive positions in the pending queues that skip deleted rows. The
  incoming cursor never passes the snapshot. A separate `scanTs` cursor recovers
  near-term scheduled enqueues that commit behind it.
- `generation`: batch-worker's counter that permits only one active loop chain.
- Tombstones: deleted rows that remain in storage until vacuumed. Advancing
  cursors avoids repeatedly reading them.

## Data state machine

```mermaid
flowchart LR
    Client -->|enqueue| pendingStart
    Client -->|cancel| pendingCancelation
    complete --> |success or failure| pendingCompletion
    pendingCompletion -->|retry| pendingStart
    pendingStart --> workerRunning["worker running"]
    workerRunning -->|worker finished| complete
    workerRunning --> |recovery| complete
    successfulCancel["AND"]@{shape: delay} --> |canceled| complete
    pendingStart --> successfulCancel
    pendingCancelation --> successfulCancel
```

Notably:

- The pending\* states are written by outside sources.
- The main loop federates changes to/from "running"
- Canceling only impacts pending and retrying jobs.

## Loop scheduling

The loop lifecycle is owned by `@convex-dev/batch-worker`. Workpool provides a
`getBatch` query and a `run` worker mutation; batch-worker owns running/idle
state, generation checks, cooldown polling, timeout wakeups, and monitor-based
restart if the loop dies.

When the pool is saturated (`running.length >= maxParallelism`), `getBatch` uses
a 10-second idle cooldown instead of the normal 2 seconds. This keeps
batch-worker's status `running` for longer at full throttle, so most enqueue
pings are no-ops rather than racing an idle transition.

## Storage and reads

- Store one queue document per work item and delete it when the work starts or
  cancels.
- Read queues through bounded index ranges and point-read work documents.
- Keep the incoming cursor at or below the transaction snapshot so later commits
  remain reachable. Re-key future legacy buckets during upgrade.
- Limit the sweep's boundary read to entries behind the incoming cursor, so
  large batches of future work are not repeatedly scanned.
