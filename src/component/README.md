# Workpool: implementation notes and high-level architecture

Concepts:

- `segment`: A slice of time to process work. All work is bucketed into one.
  This enables us to batch work and avoid database conflicts.
- `generation`: A monotonically increasing counter owned by batch-worker to
  ensure only one loop chain runs for a worker at a time. The old workpool
  generation field is deprecated and only accepted for pre-migration docs.
- "Retention" is used to refer to situations where a query might have to read
  over a lot of "tombstones" - deleted data that hasn't been vacuumed from the
  underlying database yet. If there are frequent deletions, scanning across them
  can delay a query. Because of our delete-heavy queuing strategy, we have to be
  careful. Strategies are below.
- Cursors: A pointer to the last processed place in a table. In our case, they
  might allow data to be written before them if out-of-order writes happen, so
  we need to account for finding those "missed" writes on some granularity. We
  choose to wait until there isn't any immediate work to do before those scans.
  They help avoid retention issues.

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

Workpool still avoids unproductive enqueue wakeups while saturated: if
`internalState.running.length >= maxParallelism`, enqueue skips `ping`. Sources
that can free capacity or change existing work (`complete`, `retry`, `cancel`,
manual kicks, and maxParallelism increases) still ping.

## Retention optimization strategy

- Producers (Client, Worker, Recovery) write to a future "segment".
- Consumers (`run`) read the current segment.
  - On conflicts, producers will write to progressively higher segments, while
    the main loop will continue to read the segment originally called with. This
    means conflicts are less likely on each retry.
- Patch singletons to avoid tombstones.
- Use segements & cursors to bound reads to latest data.
  - Do scans outside of the critical path (during load).
- Do point reads otherwise.
