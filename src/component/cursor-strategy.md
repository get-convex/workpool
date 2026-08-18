# Commit-timestamp cursors for scheduled work

Workpool drains pending work by scanning an ordered index with a cursor. This
change avoids tombstone re-scans without losing out-of-order commits, while
running work in a reasonable order. It orders the index by a hybrid of commit
timestamp and desired execution time, with a second cursored index on commits
using wall-clock time to check for out-of-order entries.

## Benefit

Using a benchmark of 5000 mutation tasks (20ms each) with max parallelism of
200, this had ~20% better throughput and ~20% lower latency than the previous
rewind-and-rescan approach, with per-iteration cost no longer scaling with
recent load.

## The tombstone problem

Continuously scanning for the first entries in a table, then deleting them,
produces a mountain of tombstones (markers of deleted documents) that need to be
scanned on subsequent queries.

We solve this by keeping a cursor per index region we walk, jumping ahead to
where new entries actually are.

## The out-of-order commit problem

A cursor is only safe if nothing can appear behind it. A sort key chosen from
the wall clock is chosen when the writing transaction starts, but the row only
appears when it commits, so a slow enqueue can insert a row keyed behind a
cursor that already moved past it, orphaning the row.

The previous design compensated by scanning 15 seconds behind the cursor on
every query and rescanning the table once a minute.

A commit timestamp, on the other hand, is guaranteed to always move forward: any
row not yet visible to a read must have committed later, so its key is above any
cursor derived from rows being read.

But a commit timestamp doesn’t always correspond directly to when the work
should run.

## Blending commit time and target time

One column, `segment` (nanoseconds), holds either clock, chosen at enqueue time:

| When should it run     | `segment`          | `scheduledAt`      |
| ---------------------- | ------------------ | ------------------ |
| Now (`runAfter <= 0`)  | `db.vars.commitTs` | `-`                |
| Later (`runAfter > 0`) | the start time     | `db.vars.commitTs` |
| After retry backoff    | the start time     | `db.vars.commitTs` |

Scheduled entries are keyed at their start time, so they sort above the loop's
read bound until due. `scheduledAt` records the commit stamp of every
wall-clock-keyed entry; its absence marks a key as an observed commit timestamp
(see the ceiling below). A bulk enqueue of delayed work naturally sorts after
current work, and no entry needs to be rewritten.

Retries get written by the loop, so they can write the start time
transactionally with advancing the cursor, ensuring they will never be missed by
writing them ≥ the cursor value — they can't commit out of order, so they don't
need the sweep's protection. They still carry `scheduledAt` for its other
meaning: their key is a wall-clock time, and without the stamp it would be
counted as an observed commit timestamp when computing the cursor ceiling. The
sweep verifies each retry once and passes.

## Reading jobs in order

Two indexes: `[segment]` (when it’s due) and `[scheduledAt]` (when a future job
was enqueued).

**The sweep** walks `[scheduledAt]` in commit order, inspecting each
wall-clock-based entry, up to 1024 per iteration. Out-of-order entries are rare,
so the common case is a single pass:

- `segment >= segment cursor`: safe, the **segment scan** will read it.
- `segment < segment cursor`: missed by cursor. Prioritized to start next. It
  was committed by an enqueue that lost the race (commit latency exceeded its
  delay). It is necessarily overdue, as the cursor never passes the current
  time, and was ordered before anything in the segment scan.

Each entry is inspected once ever: the sweep cursor advances a whole commit
stamp at a time (`gt` — a batch enqueue shares one stamp, and a bare stamp can't
split the group), and only past stamps whose missed entries were all started.
Inclusive advancement would instead re-read the boundary stamp's group every
iteration until its entries come due — unbounded for far-future work. A stamp
group larger than one batch (a huge batch enqueue) is paged through to its end
within the same iteration, so the batch size is purely a read budget.

**The segment scan** reads the `[segment]` index for “due” items, from the
incoming cursor until “now.” Skipped if all available slots were taken by the
sweep, otherwise does a `take` on for remaining slots.

## Cursor rules

The incoming cursor's bounds are plain values, computed where the data is:

- **Floor**: the cursor as read. Every `segment` the loop writes (retries,
  re-keyed legacy entries) is raised to at least the floor, so nothing lands
  where the loop already read past. Safe because the write and the cursor
  advance share a transaction; at enqueue time this comparison would be racy.
- **Ceiling**: the highest commit timestamp observed while building the batch —
  completion/cancelation keys, ready entries' keys, sweep stamps, and the
  previous run's own commit stamp (recorded in `internalState.lastCommitTs`; the
  next run reads that document, so its snapshot is at least that recent). The
  cursor never advances past the ceiling: a wall-clock key can exceed every
  commit stamp that exists, and a commit racing this run would land behind a
  cursor set there. Nothing anywhere relates wall clocks to the commit clock.
- **Advance**: to the last entry of the leading run the iteration fully handled
  (stopping at the first entry left alone, e.g. by capacity), capped by the
  ceiling and by the lowest `segment` written this transaction (the cursor's
  final position isn't known when a retry is written), never backwards.

Cursors are inclusive (`gte`): a commit timestamp is unique per transaction, not
per row, so a batch enqueue writes N rows sharing one key, and an exclusive
bound would drop the rest of the group whenever capacity cut it short.

The ceiling means a cursor that starts wall-keyed work rests at the freshest
observed commit stamp rather than the wall key. It catches up one iteration
later from the loop's own recorded stamp; the lag window contains only
tombstones of entries already started, so it costs a few re-reads, never delayed
work.

## Scenarios

**Burst of delayed work.** 10k jobs enqueued with a 60s delay, then one ready
job. The ready job starts on the first iteration; the delayed jobs sit above the
read bound, unrewritten, while the sweep verifies them ~1024 per iteration. A
1s-delayed task enqueued after the burst surfaces at its own start time,
unaffected by the backlog.

**Saturated pool, scheduled work comes due.** The entry becomes visible to the
segment scan at its start time and competes in eligibility order as capacity
frees. The sweep keeps verifying new enqueues even while saturated; only an
out-of-order entry (which needs a start slot) can hold the sweep cursor, and the
pool being saturated means the incoming cursor isn't moving, so nothing new
becomes out-of-order behind it.

**Self-scheduling hourly cron.** Writes its start time as `segment`; the sweep
reads it once to verify it committed in order, and it's untouched until due.

**Small action retry backoff.** Retries are written from the loop's own
transaction, raised to the cursor floor, so even a zero-millisecond backoff
lands readable and starts on the next iteration.

**An enqueue loses the race.** `runAfter(1000)` on an enqueue that takes 1.5s to
commit lands its entry behind the cursor. The sweep finds it by its commit
stamp, sees its `segment` below the cursor, and starts it directly — no rewrite,
at most one sweep-batch behind.

## Upgrading

- Entries written by 0.4.9 hold 100ms buckets, recognized by magnitude and
  re-keyed on first read: a one-time re-ordering of the queued backlog, 64 per
  iteration, right after the push.
- Work docs written by older versions lack `pendingStartId`. Their work runs
  normally (the scan doesn't use the pointer), but until it next starts,
  `status` reports it as `"pending"` — queued is the longer-lived of the two
  states it could be in — and canceling it takes effect immediately while its
  queue entry is only cleared when it comes due.
- Downgrading is not supported: an older version reads a nanosecond timestamp as
  a bucket far in the future and never starts the work.
