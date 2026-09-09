# Benchmarking the workpool

Recorded results: [2026-09-08, revision `8186121`](./benchmarks/2026-09-08.md).

Harnesses in `example/convex/test/`, all driving a real deployment:

| entry point                            | measures                                        | pools     |
| -------------------------------------- | ----------------------------------------------- | --------- |
| `test/scenarios/throughput:default`    | end-to-end throughput and completion latency    | new / old |
| `test/scenarios/burstyBatches:default` | concurrent enqueue waves, or a light trickle    | new / old |
| `test/scenarios/sustained:default`     | paced arrivals with variable-duration actions   | new / old |
| `test/scenarios/noisyNeighbor:run`     | slow/failing neighbors, per-class outcomes      | new / old |
| `test/latency:default`                 | when tasks _start_, vs. when they were due      | new / old |
| `test/latency:pairs`                   | scheduled tasks against an immediate sibling    | new / old |
| `test/latency:backlog`                 | a late entry behind bulk scheduled work         | new / old |
| `test/scheduling:default`              | delayed work and retry attempts                 | new only  |
| `test/cleanup:start` / `:counts`       | clear app bookkeeping; does not drain workpools | shared    |

`"pool": "new"` is this checkout's `testWorkpool`; `"pool": "old"` is the
published package mounted as `oldWorkpool`. Check the installed baseline rather
than relying on dashboard labels or previous reports:

```sh
node -p 'require("./node_modules/@convex-dev/workpool-old/package.json").version'
npm run build:codegen && npx convex dev --once
```

Run commands from the repo root against a dedicated dev deployment. The example
imports the component through its `dist` exports, so **build before deploying**.
A failed deploy means the previous design is still running. In particular,
pre-snapshot experimental deployments can retain the removed `lastCommitTs`
field in `internalState`, causing schema validation to fail. Resolve that state
compatibility issue before measuring; do not treat a failed push as a new build.

Archive the commit, any source diff, dependency versions, deployment, exact
arguments, raw outputs, and run order. Paired runs on one deployment help
control time-varying noise, but different components still have different
storage histories. They compare complete designs, not an isolated source change.

## Throughput

```sh
npx convex run test/scenarios/throughput:default '{
  "taskCount": 5000, "batchSize": 100, "interBatchMs": 50,
  "maxParallelism": 200, "taskType": "mutation", "pool": "new"
}'
```

Repeat with `"pool": "old"` for the baseline. For actions, use
`"taskType": "action", "taskDurationMs": 20`. **`taskDurationMs` is ignored for
mutations**, even though the current harness includes it in the returned
parameters and log message. The mutation workload above performs no artificial
delay or database workload beyond the completion recorder.

Discard warmups, then measure at least three pairs, alternating `old,new`,
`new,old`, `old,new`. Clear bookkeeping and wait for both components to drain
between arms. Save JSON before cleanup. Report individual runs, per-pair ratios,
and the spread, not just the fastest run.

`completedCount` currently counts terminal callbacks, including failures and
cancelations: `markTaskCompleted` does not save `result.kind`. Check runtime
errors as well as `timedOut`, `status`, and the exact expected count. A CLI exit
code of zero alone does not establish that a benchmark finished successfully.

## Start latency and ordering

```sh
npx convex run test/latency:default '{
  "cell": "demo", "pool": "new",
  "groups": [{"delayMs": 3000, "count": 600}, {"delayMs": 0, "count": 2000}],
  "chunkSize": 25, "maxParallelism": 200
}'
```

Every task records its own start clock, supporting start lateness
(`startedAt - runAt`), start ordering, and per-delay-class breakdowns. `holdOps`
and `nestedCalls` stretch the enqueuing transaction, `interChunkMs` spreads
enqueues out, and `settleMs` bounds the additional wait.

`pairs` enqueues an immediate control alongside scheduled tasks in one
transaction. On the new pool the control is ordered by `db.vars.commitTs`; the
old pool uses its own immediate ordering. Siblings become visible together. When
a scheduled task is already overdue at commit, their start-time difference is
useful for comparing scheduling paths, though execution noise and capacity still
contribute.

```sh
npx convex run test/latency:pairs '{
  "cell": "pairs-demo", "pool": "new", "count": 40,
  "soonDelays": [200], "nestedCalls": 800, "maxParallelism": 200
}'
```

`backlog` puts bulk scheduled work sharing one commit stamp ahead of such a
pair:

```sh
npx convex run test/latency:backlog '{
  "cell": "backlog-demo", "pool": "new", "bulkCount": 2000, "bulkDelayMs": 60000,
  "soonDelayMs": 100, "nestedCalls": 800, "maxParallelism": 200
}'
```

For a behind-cursor experiment, both need light concurrent ready-now work — see
the pool note below. The commands above do **not** generate that traffic. A
completed pair without evidence that the incoming cursor passed the scheduled
key is a latency sample, not proof the sweep ran. `committedAt` in the pair rows
is the action's clock after the enqueue RPC returned, not the database commit
timestamp; a worker can start before that observation.

`backlog` returns when its two probes start, while the bulk may still be waiting
for `bulkDelayMs`. Wait for that work to finish before cleaning up or running
another cell. Use distinct `cell` names: `resetCell` deletes probe rows but does
not cancel their queued tasks.

The delayed/retry smoke check is separate and only supports the new pool:

```sh
npx convex run test/scheduling:default
npx convex run test/scheduling:default '{"delayMs":310000}'
```

The second crosses the five-minute threshold where enqueues omit `scanTs`. Its
lateness is approximate: the harness starts its timer before calling the
enqueuing mutation. Three recorded attempts do not independently prove the final
retry's terminal result.

If the long-delay CLI call errors or times out, check the existing run before
retrying. The recorded 310-second run returned a generic CLI error even though
the server action completed successfully; the cause was not established. Read
persisted probes with `npx convex run test/scheduling:probes`, inspect the
server action's completion/error logs, and check `testWorkpool` queue and
running state using the drain checklist below. If work is still active, wait for
it to settle. Archive that evidence before cleanup or another run: rerunning the
command resets the probes and enqueues new delayed/retry work without canceling
the previous work.

## Comparing a code change against itself

Use the same component for both variants, with a build and successful deploy for
each source change, discarded warmups, cleanup, and balanced run order. Keep
edits in isolated checkouts so restoring a variant cannot overwrite other work.
Do not change constants during a comparison of the current design against the
published baseline.

Historical `.context/bench-commitTs.sh`, `.context/exp-consts.py`, and
experiment reports are workspace artifacts, not tracked tooling available in a
fresh clone. They are not the authoritative procedure: their warmup counts/order
differ from their comments, and restoring with `git checkout` can erase
concurrent edits.

## Getting numbers you can trust

**Warm up, and discard it.** Earlier experiments showed substantial warmup
drift. Start with three discarded runs per workload and pool, and check whether
timings stabilize; three is a heuristic, not a guarantee. Balanced ordering does
not automatically cancel nonlinear drift.

**Prefer paired runs.** Report the within-pair throughput ratio as well as
absolute measurements. Deployment-level variation can move both arms together. A
ratio does not remove differences in component storage history.

**Drain, archive, then clear bookkeeping between runs.** On this dedicated
benchmark deployment:

```sh
npx convex run test/cleanup:start
npx convex run test/cleanup:counts  # repeat until every count is zero
```

Cleanup deletes **all** `tasks`, `latencyTasks`, `runs`, `schedulingProbes`, and
`data`, including dashboard history. It does not clear `counters` or component
tables, cancel tasks, or wait for workers. First verify each component has no
`work`, `pendingStart`, `pendingCompletion`, `pendingCancelation`, `payload`, or
running entries. Then wait for all cleanup counts to reach zero; a missing/error
response is not zero. Concurrent runs and delayed leftovers can repopulate
tables after cleanup.

Bookkeeping writes are inside the measured path, so accumulated table state is a
confound. Cleanup bounds live rows; it does not reset storage history or prove
that the two components have identical costs.

**Establish the noise floor before believing an effect.** Repeat identical
configurations and inspect the spread before interpreting a small effect.
Non-monotonic results are not proof of noise: batching, contention, and latency
tradeoffs can produce a real optimum. Investigate accumulated state, offered
load, failures, and resource limits before assigning a cause.

**A transaction is one sample for commit ordering.** Entries in one enqueue
share a commit stamp and frozen enqueue clock. Their worker start times can
differ because workers run in separate transactions. Treat a chunk as one
correlated trial for out-of-order landing, and use `chunkSize` to obtain
multiple transactions. Those transactions still share deployment-level noise.

**`Date.now()` is frozen inside a mutation.** A wall-clock spin loop cannot
measure duration there. To stretch a transaction, count operations (`holdOps`)
or call `nestedNoop` repeatedly (`nestedCalls`), and time it from an action,
where the clock advances. These remain subject to function execution limits;
nested calls that do database work consume budget.

**Control pool load for cursor experiments.** A saturated pool's incoming cursor
can lag, and an idle pool's cursor may not advance. Neither reliably produces
the behind-cursor condition. Use light concurrent ready-now work at high
`maxParallelism`, and verify cursor advancement and available capacity during
the slow enqueue. High parallelism alone is insufficient.

## Reading a slow loop

```sh
npx convex logs --jsonl --success
```

Entries carry execution time and documents/bytes read and written. Execution
time rising while read/write volume falls is a reason to inspect sequential
round trips, not a diagnosis by itself. Check OCC and execution statistics too.
A past packing regression came from sequential per-entry point reads;
independent reads can be batched with `Promise.all`.

Both current mounts use a nested `batchWorker`. Include
`testWorkpool/batchWorker` and `oldWorkpool/batchWorker` when comparing loop
executions; counting only the parent component misses loop scheduling.
