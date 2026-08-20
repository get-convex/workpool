# Benchmarking the workpool

Harnesses in `example/convex/test/`, all driving a real deployment:

| entry point                      | measures                                               |
| -------------------------------- | ------------------------------------------------------ |
| `test/scenarios/throughput`      | end-to-end throughput and completion latency           |
| `test/latency:default`           | when tasks _start_, vs. when they were due             |
| `test/latency:pairs`             | a scheduled task against a `commitTs` sibling          |
| `test/latency:backlog`           | a late scheduled entry behind a bulk sharing one stamp |
| `test/scheduling:default`        | delayed + retried work, end to end                     |
| `test/cleanup:start` / `:counts` | empty the bookkeeping tables (see below)               |

Both can run against either component: `"pool": "new"` is this branch's
`testWorkpool`, `"pool": "old"` is the published baseline mounted as
`oldWorkpool`. Comparing the two in one run is the only way to attribute a
change to the code rather than to the deployment.

```sh
npx convex dev --once   # push the component before measuring anything
```

## Throughput

```sh
npx convex run test/scenarios/throughput:default '{
  "taskCount": 5000, "batchSize": 100, "interBatchMs": 50,
  "maxParallelism": 200, "taskDurationMs": 20,
  "taskType": "mutation", "pool": "new"
}'
```

Paired A/B against the baseline, alternating which arm leads each rep:

```sh
REPS=3 ./.context/bench-commitTs.sh
```

## Start latency and ordering

```sh
npx convex run test/latency:default '{
  "cell": "demo", "pool": "new",
  "groups": [{"delayMs": 3000, "count": 600}, {"delayMs": 0, "count": 2000}],
  "chunkSize": 25, "maxParallelism": 200
}'
```

Every task records its own start clock, so the returned rows support start
lateness (`startedAt - runAt`), start ordering, and per-delay-class breakdowns.
Options beyond the above: `holdOps` and `nestedCalls` stretch the enqueuing
transaction, `interChunkMs` spreads enqueues out, `settleMs` bounds the wait.

To ask whether the _ordering path_ costs anything, use `pairs` instead. It
enqueues, in one transaction, a control task ordered by `db.vars.commitTs`
alongside scheduled ones. The control shares the commit, so both become visible
at the same instant, and when the transaction takes longer to commit than a
delay that task is already overdue on landing — leaving any difference in start
time attributable to the path rather than the wait.

```sh
npx convex run test/latency:pairs '{
  "cell": "demo", "pool": "new", "count": 40,
  "soonDelays": [200], "nestedCalls": 800, "maxParallelism": 200
}'
```

`backlog` puts a bulk of scheduled work sharing one commit stamp ahead of such a
pair — the shape that once stalled the sweep:

```sh
npx convex run test/latency:backlog '{
  "cell": "demo", "pool": "new", "bulkCount": 2000, "bulkDelayMs": 60000,
  "soonDelayMs": 100, "nestedCalls": 800, "maxParallelism": 200
}'
```

Both need concurrent ready-now work to be meaningful — see the pool note below.

## Comparing a code change against itself

`.context/exp-consts.py` edits a constant in `loop.ts`, deploys, measures, and
repeats — on the same component, in mirrored order, with cleanup between runs:

```sh
python3 .context/exp-consts.py mainbatch    # MAIN_BATCH_SIZE 64/128/256
```

It refuses to run on a dirty `src/component` and restores via `git checkout`, so
its own edits can't clobber work in flight. Findings from the runs already done
are in `.context/README.md`.

## Getting numbers you can trust

Everything below was learned the hard way while producing
`.context/scheduling-experiments.md` and `.context/commitTs-benchmark.md`.

**Warm up, and discard it.** The first runs against a deployment are reliably
the slowest — one sequence went 20.9s → 19.1s → 17.0s → 16.1s before flattening,
a ~20% drift that dwarfs most effects being measured. Three discarded runs is
usually enough. A mirrored order (a,b,c,c,b,a) does _not_ rescue you here: the
drift decays rather than being linear, so it doesn't cancel.

**Prefer paired runs.** Deployment-level noise moves both arms together; one rep
of the throughput benchmark had both arms 15% slow while the ratio between them
held to within a point. Trust the ratio over the absolute.

**Clear the bookkeeping between runs.** This one matters more than everything
else here:

```sh
npx convex run test/cleanup:start     # then poll test/cleanup:counts
```

`tasks` and `latencyTasks` gain a row per measured task, written from _inside_
the measured path, and nothing reads them across runs. Left alone they reached
659,075 and 87,680 rows in one session — so every run paid a different, growing
index-maintenance cost on a table incidental to the thing being measured.
Clearing between runs cut within-variant spread from 59% of the mean to 20%, and
to 3–5% once warm. That is the difference between seeing a 6% effect and
concluding there wasn't one.

**Establish the noise floor before believing an effect.** Run the same
configuration several times and look at the spread; that's the smallest
difference the rig can see. A three-way comparison coming out non-monotonic (the
middle variant fastest or slowest) is a reliable tell that you're reading noise,
since no real effect can produce it.

Resist "the infrastructure is flaky" as an explanation. It was reached once here
and it was wrong — requests genuinely were being dropped, but the spread was
accumulated state, and the conclusion was unfalsifiable as stated. Look for
something growing between runs first.

**A transaction is one sample.** Every entry enqueued in a single transaction
shares a commit stamp and a start time. Measuring 200 entries at once gives one
Bernoulli trial, not 200 — the first pass at the out-of-order experiment
produced all-or-nothing 200/0 results for exactly that reason. Use `chunkSize`.

**Isolate a code change on one component.** Deploy variant A to `testWorkpool`
and measure, then deploy variant B to the _same_ component and measure again.
Running variant A on `testWorkpool` against variant B on `oldWorkpool` conflates
the change with each component's accumulated table history; that confound once
reversed a result entirely.

**A saturated pool hides scheduling effects.** With a backlog the loop's cursor
lags wall-clock, so anything that depends on the cursor tracking the present
(out-of-order landing, say) simply won't happen. Keep the pool drained — high
`maxParallelism`, light load — when that's what you're measuring.

**`Date.now()` is frozen inside a mutation.** A wall-clock spin loop never
terminates; it burns the read limit and fails. To stretch a transaction, count
operations instead (`holdOps`) or nest mutation calls (`nestedCalls`, which
doesn't consume the read/write budget), and time it from an action, where the
clock is real.

**A saturated or idle pool hides anything cursor-related.** With a backlog the
loop's cursor lags wall-clock; with an idle pool it doesn't move at all. Either
way a scheduled entry is never behind it, so out-of-order landing simply cannot
happen and the measurement comes back clean for the wrong reason. Those cells
need light concurrent ready-now work at high `maxParallelism`.

## Reading a slow loop

`npx convex logs --jsonl --success` is the fastest way to see where loop time
goes; each entry carries execution time alongside documents read and written.

The signature worth recognising: **execution time up while read and write volume
is down means round trips, not work.** That is how the packing regression was
found — a sequential `await ctx.db.get()` inside a per-entry loop cost one round
trip per entry, 64 per iteration, for a flat ~150ms. Batch point reads with
`Promise.all`.
