# Benchmarking the workpool

Two harnesses live in `example/convex/test/`, both driving a real deployment:

| harness                     | measures                                     |
| --------------------------- | -------------------------------------------- |
| `test/scenarios/throughput` | end-to-end throughput and completion latency |
| `test/latency`              | when tasks _start_, vs. when they were due   |

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

Analysis drivers for the experiments already run live in `.context/`:
`exp1.py`/`exp1b.py`/`exp1c.py` (out-of-order landing), `exp234.py` (ordering,
lateness, new-vs-old), `exp-consts.py` (A/B a constant in the run loop).

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
