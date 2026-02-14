# Batch Execution Mode: Design Choices & Tradeoffs Analysis

## Overview

The batch execution mode is an alternative to the standard workpool that replaces "1 Convex action per task" with "N long-lived executor actions, each running M concurrent handlers via Promise.all." The goal is dramatically higher throughput for I/O-bound workloads (e.g., API calls to LLMs).

**Standard mode:** Enqueue → scheduler starts 1 action per task → action runs → onComplete scheduled
**Batch mode:** Enqueue to DB table → executor claims batch → runs handlers as plain async functions → batch-reports results

---

## Choice 1: Long-lived Executor Actions (vs. 1 Action Per Task)

### What it does
Instead of scheduling a separate Convex action for each task, a fixed pool of "executor" actions run continuously (up to the 10-min action timeout). Each executor claims work from a DB queue, runs handlers as plain async functions, and reports results.

### Pros
- **Eliminates cold start overhead.** Standard mode pays cold start cost per task. Batch mode pays it once per executor (~10-20s on dev deployments).
- **Amortizes mutation cost.** Results reported in batches of 200 instead of 1-at-a-time, reducing mutation count from O(N) to O(N/200).
- **High concurrency.** Each executor runs up to 1000 concurrent handlers via Promise.all. With 20 executors, that's 20,000 concurrent tasks using only 20 action slots.
- **No scheduler queue bottleneck.** Standard mode goes through Convex's scheduler for every task start. Batch mode bypasses this entirely.

### Cons
- **Handlers aren't real Convex actions.** They run inside an action but don't have individual function identity, can't be individually canceled by Convex runtime, and don't appear as separate entries in the Convex dashboard.
- **No per-handler timeout.** A hanging handler blocks its slot in the executor's concurrency pool until the executor's soft deadline. In standard mode, each action has its own 10-min timeout.
- **Action slot consumption.** 20 executors permanently occupy 20 action slots for the full 10-min lifetime, even during idle polling. Standard mode releases slots between tasks.
- **Complexity.** ~1500 lines of new code with executor lifecycle, claim management, watchdog, etc. Standard mode is simpler.
- **10-min ceiling.** Executor must exit before the Convex action timeout. Handlers that take close to 10 minutes can't be supported.

### Alternatives
- **Micro-batching:** Instead of long-lived executors, schedule short-lived actions that each process a small batch (e.g., 10 tasks). Lower complexity, still some cold start savings.
- **Action pooling at Convex platform level:** If Convex added native support for "warm" action pools, this entire approach would be unnecessary.
- **Streaming/WebSocket executors:** Keep executors alive via external process instead of relying on action timeout cycling.

---

## Choice 2: Slot-Based Partitioning

### What it does
Each task is assigned a random `slot` (0..maxWorkers-1) at enqueue time. Each executor only claims tasks from its own slot. This eliminates OCC conflicts between executors competing for the same index range.

### Pros
- **Zero OCC conflicts between executors.** Each executor reads/writes a disjoint partition of the index. No contention on claim mutations.
- **Simple implementation.** Random assignment at enqueue time, filtered query at claim time.
- **Scales linearly.** Adding more workers doesn't increase contention.

### Cons
- **Uneven distribution.** Random assignment can lead to hot slots and cold slots, especially with small task counts. If 100 tasks are enqueued and maxWorkers=20, some slots may get 8 tasks and others 2.
- **Slot can't be reassigned.** If an executor for slot 3 crashes and a task is on slot 3, it must wait for a new slot-3 executor (or the watchdog sweep). Other idle executors can't help.
- **Coupling to maxWorkers.** Changing maxWorkers requires rehashing. Tasks enqueued with old maxWorkers won't distribute to new slots.
- **Load imbalance with varying task durations.** If slow tasks concentrate on one slot by chance, that executor falls behind while others are idle.

### Alternatives
- **Work stealing:** Executors claim from their own slot first, then steal from other slots if idle. Eliminates load imbalance at the cost of re-introducing some OCC risk.
- **Consistent hashing:** Hash task ID or name to slot. More predictable distribution but same fundamental tradeoff.
- **Single queue with optimistic locking:** Let executors compete for any task. Accept some OCC failures as the cost of perfect load balancing. Works well at moderate scale.
- **Range partitioning on readyAt:** Each executor owns a time range. Avoids the random imbalance problem.

---

## Choice 3: Two-Step Claiming (Query + Mutation)

### What it does
Claiming is split into two steps: (1) `listPending` — a read-only query that returns task IDs without OCC implications, then (2) `claimByIds` — a mutation that does point reads (db.get) on those specific IDs and marks them claimed.

### Pros
- **Avoids index range read conflicts.** The query step has no OCC implications. The mutation step only touches specific documents, not index ranges.
- **Tolerant of stale reads.** If a task was claimed between the query and mutation, claimByIds simply skips it (it checks status).
- **Complementary to slot partitioning.** Together they virtually eliminate OCC on the hot path.

### Cons
- **Two round trips.** Doubles the latency of claiming vs. a single mutation. For an executor claiming every ~200ms, this adds ~100ms per claim cycle.
- **Race window.** Between listPending and claimByIds, another process could cancel/claim the same task. The code handles this, but it means claimed batches may be smaller than requested.
- **Complexity.** The old `claimBatch` (single mutation) is still in the codebase, marked deprecated. Two code paths to maintain.

### Alternatives
- **Single mutation with index range (the old way).** Simpler, one round trip, but OCC-prone when onComplete handlers enqueue new tasks into the same index range.
- **Cursor-based claiming.** Store a cursor in the config doc. Each executor advances the cursor atomically. Avoids range scans but introduces contention on the cursor document.
- **Queue with FIFO guarantees.** If Convex had a native queue primitive, two-step claiming wouldn't be needed.

---

## Choice 4: Executor-Side onComplete Dispatch

### What it does
When `completeBatch`/`failBatch` mutations run, they return onComplete handler data to the executor action. The executor then dispatches these via `dispatchOnCompleteBatch` (batches of 10 items per mutation, 10 concurrent). This bypasses the scheduler entirely for onComplete.

### Pros
- **Dramatically higher onComplete throughput.** Instead of scheduling N individual mutations via `ctx.scheduler.runAfter`, it batches them into mutations of 10, dispatched 10-at-a-time from the action. 100x fewer scheduled functions.
- **Lower latency.** onComplete handlers run immediately instead of waiting in the scheduler queue.
- **Atomicity within a batch.** All onComplete handlers in one `dispatchOnCompleteBatch` call execute in the same mutation transaction. Their writes (including re-enqueue for pipeline stages) are atomic.

### Cons
- **All-or-nothing per batch.** If one onComplete handler in a batch throws, its writes are rolled back but the mutation continues (try/catch). However, this means partial success within a batch — some handlers' writes committed, one didn't.
  - Actually, looking at the code: each handler runs via `ctx.runMutation(fnHandle)` which is a separate sub-transaction. So a failure in one doesn't roll back others. But the error is swallowed with a log.
- **Lost onComplete on executor crash.** If the executor crashes after completeBatch returns onComplete data but before dispatching it, those onComplete handlers are lost. The task is already deleted from the DB.
- **Backpressure complexity.** The drainer has its own concurrency control (ONCOMPLETE_CONCURRENCY=5, ONCOMPLETE_BATCH_SIZE=50), buffer management, and retry logic for transient errors. This is a mini-scheduler inside the executor.
- **Re-enqueue from onComplete creates same-TX dependency.** When an onComplete handler calls `batch.enqueue()`, it inserts into the same `batchTasks` table. Since this runs inside `dispatchOnCompleteBatch` (a component mutation calling user mutations via function handles), the enqueue is in the same transaction. This works but is subtle.

### Alternatives
- **Scheduler-based onComplete (the standard mode way).** Simpler, no data loss risk, but ~100x more scheduled functions. This is what the single-item `complete`/`fail` mutations still do.
- **Separate onComplete table.** Write onComplete items to a table, have a dedicated consumer process them. Survives executor crashes but adds latency and another table.
- **Event-driven onComplete.** Use Convex's reactive queries to trigger onComplete processing. Elegant but high overhead for high-throughput scenarios.

---

## Choice 5: Watchdog for Self-Healing

### What it does
A periodic `_watchdog` internal mutation self-schedules every 30 seconds while work exists. It handles three failure modes: stale claims (dead executors), stuck executors (marked active but not processing), and missing executors (slots without an executor).

### Pros
- **Survives redeploys.** Scheduled functions persist across deploys, so even if all executor actions are killed, the watchdog fires and restarts them.
- **Handles executor crashes.** Detects stale claims (older than claimTimeoutMs) and returns them to pending.
- **Detects stuck executors.** If a slot has old pending tasks (>30s), it assumes the executor is dead and restarts it.
- **Self-terminating.** Stops scheduling itself when no work remains.

### Cons
- **30-second detection latency.** Minimum 30 seconds to recover from a crash/redeploy. Could be reduced but increases mutation overhead.
- **Singleton contention.** Watchdog reads/writes `batchConfig`, which is also written by `executorDone` and `_maybeStartExecutors`. Potential OCC if they run concurrently.
- **False positives.** The "stuck executor" heuristic (pending tasks older than 30s) could fire during legitimate slow processing or high memory pressure pauses.
- **Dedup window is coarse.** Uses a 20s dedup window (`watchdogScheduledAt`) to avoid scheduling multiple watchdogs. If many enqueues happen rapidly, multiple `_maybeStartExecutors` could each try to schedule a watchdog.

### Alternatives
- **Heartbeat-based liveness.** Each executor writes a heartbeat timestamp to its slot in the config doc. Watchdog checks heartbeats. More accurate but increases writes.
- **Convex cron job.** Schedule a cron that runs every N seconds to check for stale work. Simpler than self-scheduling but always runs even when no work exists.
- **No watchdog — rely on enqueue to restart.** Each enqueue checks for missing executors. Simpler but doesn't handle the case where all tasks are enqueued and executors die with no new enqueues coming in.

---

## Choice 6: Singleton batchConfig Document

### What it does
A single `batchConfig` document stores executor handle, maxWorkers, activeSlots array, claimTimeoutMs, and watchdogScheduledAt. It's the coordination point for all executor lifecycle management.

### Pros
- **Simple coordination.** One place to check what's running, what's configured, and when the watchdog last fired.
- **Atomic updates.** Updating activeSlots is a single patch operation.

### Cons
- **OCC hotspot.** Every `executorDone`, `_maybeStartExecutors`, and `_watchdog` call reads and writes this document. With 20 executors exiting near-simultaneously (e.g., at the 10-min mark), there will be OCC conflicts on executorDone.
  - Mitigated: executorDone retries up to 5 times with backoff.
- **No per-slot state.** Can't track per-executor metrics (tasks processed, errors, last heartbeat) without expanding the singleton.
- **activeSlots array manipulation.** Filtering and appending to an array isn't atomic — it's read-modify-write. Two concurrent executorDone calls could clobber each other's slot removal.

### Alternatives
- **Per-slot config documents.** One document per executor slot. Each executor only writes its own document. Watchdog reads all. Eliminates OCC on the config.
- **No config document — derive state from task table.** Count claimed tasks per slot to infer active executors. Eliminates the singleton entirely but makes queries more expensive.
- **Distributed counter.** Use separate counter documents for different aspects (activeCount, watchdogTs, etc.) to reduce contention.

---

## Choice 7: Lazy Config Initialization via Enqueue

### What it does
The first `enqueue()` call passes `batchConfig` to the component. The component schedules `_maybeStartExecutors` as a separate transaction (via `ctx.scheduler.runAfter(0, ...)`) to avoid OCC between concurrent enqueues.

### Pros
- **Zero setup step.** No need to call `configure()` before enqueuing. The system bootstraps itself.
- **Separate TX for config.** By scheduling `_maybeStartExecutors` instead of doing it inline, concurrent enqueues don't conflict on the batchConfig singleton.
- **Dedup via `configSentThisTx`.** After the first enqueue in a mutation, subsequent enqueues skip sending batchConfig.

### Cons
- **Extra scheduled function.** Every first-enqueue triggers a scheduled mutation. If tasks arrive in bursts, many `_maybeStartExecutors` are scheduled (though they're mostly no-ops after the first).
- **Race between enqueue and config.** Tasks can exist in the table before executors are started. There's a brief window where tasks are pending but no executor is running.
- **configSentThisTx is per-instance state.** If the user creates multiple BatchWorkpool instances (shouldn't, but could), the flag doesn't transfer.

### Alternatives
- **Explicit configure step.** Require the user to call `batch.start(ctx)` once. Simpler runtime logic but worse DX.
- **Config in schema defaults.** Pre-populate batchConfig via schema defaults or migration. No runtime initialization needed.
- **Inline config upsert (accept OCC).** Do the config upsert in the enqueue mutation itself. Simpler code but concurrent enqueues will OCC on the config doc.

---

## Choice 8: Handler Registration via `batch.action()`

### What it does
Handlers are registered as named entries in a Map on the BatchWorkpool instance. `batch.action("name", { args, handler })` stores the handler function and also returns a real `internalAction` for type-safety and potential direct invocation.

### Pros
- **Clean API.** Looks like defining a normal Convex action. The handler can use `ctx.runQuery`, `ctx.runMutation`, and `fetch`.
- **Dual registration.** The returned action can be called directly (bypassing the batch pool) for testing or one-off use.
- **String-based dispatch.** Handler names are stored in the task row. The executor looks up the handler by name. Simple and serializable.

### Cons
- **Name collision risk.** If two modules register the same name, the second silently overwrites the first.
- **Global registration.** All handlers must be registered on the same BatchWorkpool instance. In a large app with many modules, this creates coupling.
- **No type safety on enqueue.** `batch.enqueue(ctx, "name", args)` takes a string name and `any` args. Typos in the name or wrong args won't be caught at compile time.
- **Import dependency.** The executor file must import all handler modules (directly or transitively) so they register before the executor starts. The example uses `import "./batchActions"` for this.

### Alternatives
- **Function reference dispatch.** Use Convex function handles as keys instead of strings. Full type safety but requires more boilerplate.
- **Decorator pattern.** `@batchHandler("name")` on an exported function. Nicer syntax but not idiomatic in Convex/TypeScript.
- **Plugin architecture.** Handlers register themselves via a shared registry module. Decouples handler definition from the workpool instance.

---

## Choice 9: Memory Back-Pressure

### What it does
Before claiming new tasks, the executor checks `process.memoryUsage().heapUsed`. If it exceeds `maxHeapMB` (default 448 MB out of 512 MB limit), it stops claiming but lets in-flight tasks finish.

### Pros
- **Prevents OOM crashes.** Convex actions have a 512 MB memory limit (Node) or 64 MB (V8 isolate). Without back-pressure, 1000 concurrent API calls could each hold response buffers.
- **Graceful degradation.** Instead of crashing, the executor naturally slows down.
- **Configurable.** Users can tune `maxHeapMB` or disable it entirely (`maxHeapMB: 0`).

### Cons
- **Node-only.** `process.memoryUsage()` doesn't exist in Convex's V8 runtime. Memory pressure is only checked in Node (isNode flag).
- **Coarse-grained.** Heap usage is checked per claim cycle, not per task. A task that allocates 400 MB after claiming won't be caught until the next cycle.
- **GC timing.** `heapUsed` can be misleading — it includes garbage not yet collected. A GC pause could temporarily spike heap usage.

### Alternatives
- **Task-level memory limits.** Track estimated memory per handler and pre-filter based on available memory. More accurate but requires user-provided estimates.
- **Concurrency-based throttling only.** Simpler: just limit concurrent tasks and trust that each task stays within memory bounds.
- **WebAssembly memory tracking.** In the Convex V8 runtime, use ArrayBuffer tracking. Platform-specific but more accurate.

---

## Choice 10: Completion Buffering & Concurrent Batch Flushing

### What it does
Completions and failures are buffered in arrays and flushed in batches of 200 (`FLUSH_BATCH_SIZE`) with up to 3 concurrent flush mutations (`FLUSH_CONCURRENCY=3`). The flush is fire-and-forget from the main loop (`void flushBuffers()`) — claiming and execution continue while completions flush in the background. `Promise.allSettled` is used so one batch failure doesn't block others. Transient errors cause the failed batch to be re-queued for retry.

### Pros
- **Dramatically fewer mutations.** 10,000 completions → ~50 mutations instead of 10,000.
- **Concurrent flushing.** 3 parallel completeBatch calls reduce total flush time from O(N/batchSize) to O(N/batchSize/concurrency). Measured impact: 5x overall throughput improvement.
- **Fully non-blocking.** The main loop never `await`s flushBuffers. Claiming continues at full speed while completions drain in the background. A `flushRunning` guard prevents overlapping flushes.
- **Transient error resilience.** OCC failures and "too many concurrent commits" are retried per-batch (failed batches re-queued via `unshift`), while successful batches in the same round proceed normally.

### Cons
- **Data loss window.** Buffered completions exist only in the executor's memory. If the executor crashes before flushing, those results are lost. The tasks will eventually be swept back to pending by the watchdog and re-executed (if idempotent, no problem; if not, could duplicate side effects).
- **Ordering not guaranteed.** Tasks may complete in a different order than they started. With concurrent flushes, batch B may commit before batch A even if A's tasks finished first. If an onComplete for task A depends on B's state, behavior is unpredictable.
- **Complexity of flush lifecycle.** The `flushRunning` guard, transient error retry, `Promise.allSettled` result handling, and buffer splice/unshift logic is subtle. The `finally` block must wait for background flushes to complete before doing a final synchronous flush.
- **Mutation pressure from concurrent flushes.** 3 concurrent flushes × 20 executors = 60 simultaneous completeBatch mutations. Combined with 5 concurrent onComplete dispatches × 20 executors = 100 more. Total: ~160 concurrent mutations, approaching Convex limits.

### Alternatives
- **Synchronous flush after each task.** Simplest, but throughput drops to ~1 mutation per task completion. Defeats the purpose.
- **Flush on a timer.** Flush every N ms regardless of buffer size. Predictable latency but may flush small batches.
- **Sequential flush (the previous approach).** Single `completeBatch` at a time per executor. Simpler, fewer concurrent mutations, but was the primary bottleneck — executors spent more time flushing than working.
- **Write-ahead log.** Write completions to a separate table immediately, then batch-process them. Survives crashes but adds write amplification.

---

## Choice 11: Delete-on-Complete (vs. Status Update)

### What it does
When a task completes successfully, the row is deleted from `batchTasks` (`ctx.db.delete(taskId)`). Failed tasks that exhaust retries are also deleted. Only pending and claimed tasks exist in the table.

### Pros
- **Keeps the table small.** For high-throughput workloads (20K+ tasks), the table stays bounded to only active work. No unbounded growth.
- **Simpler index queries.** `by_slot_status_readyAt` only contains actionable tasks. No need to filter out completed/failed.
- **Matches Convex patterns.** Deleting completed work is common in Convex apps to avoid unbounded table growth.

### Cons
- **No completion history.** Once a task completes, there's no record of it in the batch tables. If onComplete fails, there's no way to inspect what happened.
- **Status query ambiguity.** `status(taskId)` returns `{ state: "finished" }` for both completed tasks and tasks that never existed. Can't distinguish "completed successfully" from "was deleted" from "never enqueued."
- **Debugging difficulty.** Can't query the table to see what completed, when, or with what result.

### Alternatives
- **Soft delete / status update.** Mark tasks as "completed" or "failed" instead of deleting. Keep the full history. Use a TTL or cleanup job to eventually delete old rows.
- **Archive table.** Move completed tasks to a separate `batchTasksArchive` table. Keeps the hot table small while preserving history.
- **Write result to work table, delete after onComplete.** Two-phase: mark complete, dispatch onComplete, then delete. Survives onComplete failures.

---

## Choice 12: `executorDone(startMore=true)` Restarts ALL Missing Executors

### What it does
When an executor finishes and there's remaining work, `executorDone(startMore=true)` doesn't just restart the exiting executor — it checks for ALL missing slots (0..maxWorkers-1) and starts executors for all of them.

### Pros
- **Catches slots that exited without anyone noticing.** If slot 3's executor crashed and slot 5 exits normally, slot 5's executorDone will notice slot 3 is missing and restart it.
- **Self-healing without the watchdog.** Reduces dependency on the 30-second watchdog interval.

### Cons
- **Thundering herd.** If many executors exit simultaneously (e.g., all hit the 10-min timeout), they all try to restart all missing slots. N executorDone calls, each scheduling up to N new executors. Many of these are redundant.
- **OCC on batchConfig.** All these concurrent executorDone mutations read/write the same batchConfig doc. The retry loop (5 attempts) mitigates this but adds latency.
- **Over-provisioning.** If only 2 slots have pending work, all 20 slots still get started. The idle executors will poll for 5 minutes before exiting.

### Alternatives
- **Only restart the exiting slot.** Simpler, fewer OCC conflicts. Rely on watchdog for other missing slots.
- **Optimistic restart with dedup.** Use a per-slot "lastStartedAt" field. Only schedule a new executor if it hasn't been started recently.
- **Demand-based scaling.** Count pending tasks per slot and only start executors for slots that have work.

---

## Choice 13: Claim All Concurrency Slots at Once (maxClaimBatch = maxConcurrency)

### What it does
Previously `maxClaimBatch` was hardcoded to 200, meaning an executor with 1000 concurrency slots needed 5 claim cycles to fill up. Now `maxClaimBatch = maxConcurrency` (default 1000), so the executor attempts to fill all available slots in a single listPending + claimByIds round trip.

### Pros
- **Faster ramp-up.** An idle executor fills to full concurrency in one claim cycle (~100ms) instead of five (~500ms+). For pipelines where each stage re-enqueues to the next, this dramatically reduces stage transition latency.
- **Fewer claim round trips.** 1 query + 1 mutation instead of 5 queries + 5 mutations per fill cycle. Reduces total mutation count.
- **Safe because of two-step claiming.** listPending is a read-only query (no OCC). claimByIds uses point reads only. Even with 1000 IDs, there's no index range conflict.

### Cons
- **Large claimByIds mutation.** 1000 point reads + 1000 patches in a single mutation. Approaches Convex's document limit per mutation (8192). If tasks have large `args` or `onComplete` payloads, could hit size limits.
- **All-or-nothing claiming.** If the claimByIds mutation fails (transient error), the executor loses all 1000 claims and must retry the entire cycle. Smaller batches would lose fewer claims on failure.
- **Stale reads amplified.** With 1000 IDs from listPending, more of them may have been claimed/canceled by the time claimByIds runs, leading to smaller effective batches.

### Alternatives
- **Fixed moderate batch (the old 200 limit).** Safer per-mutation size, but leaves concurrency slots unfilled.
- **Adaptive batch size.** Start small, increase when claim success rate is high, decrease when many IDs are stale. Self-tuning but complex.
- **Chunked claiming.** Split 1000 IDs into 5 parallel claimByIds calls of 200 each. Limits per-mutation size while still filling quickly.

---

## Choice 14: 50ms Loop Timer (vs. 200ms)

### What it does
The executor's main loop waits for either a task to complete or a timer to fire, whichever comes first. The timer was reduced from 200ms to 50ms, making the loop 4x more responsive.

### Pros
- **Faster reaction to completions.** When a task completes and frees a concurrency slot, the executor notices and claims new work within 50ms instead of 200ms.
- **Pipeline stage transitions.** In pipelines (task A's onComplete enqueues task B), the 150ms savings per stage compounds across stages. For a 3-stage pipeline with 20K tasks, this shaves significant time.
- **Negligible cost.** The timer doesn't trigger any I/O — it's just a setTimeout in the action's event loop.

### Cons
- **More loop iterations.** 4x more iterations of the while loop per second, meaning 4x more checks of `Date.now()`, memory pressure, `canClaim`, etc. Negligible CPU cost but slightly more noisy in profiling.
- **More frequent (potentially empty) claim attempts.** If the loop wakes up due to the timer rather than a task completion, it may find no available concurrency slots and do a no-op iteration.

### Alternatives
- **Pure event-driven (no timer).** Only wake when a task completes. Lowest overhead but can't react to externally-enqueued tasks or buffer flush completion.
- **Adaptive timer.** Short when busy, longer when idle. E.g., 50ms when inFlight > 50%, 500ms when inFlight == 0. Reduces no-op wake-ups during idle polling.

---

## Choice 15: Node.js Runtime for Executor Actions ("use node")

### What it does
The example executor file uses `"use node"` to run in the Node.js runtime (512 MB memory) instead of Convex's default V8 isolate (64 MB memory).

### Pros
- **8x memory headroom.** 512 MB vs 64 MB. With 1000 concurrent `fetch()` calls, each holding response buffers, 64 MB is easily exceeded.
- **Memory back-pressure works.** `process.memoryUsage()` is only available in Node. The back-pressure system (Choice 9) is effectively disabled in the V8 runtime.
- **Access to Node APIs.** Can use `fs`, `crypto`, `Buffer`, etc. if handlers need them.

### Cons
- **Slower cold starts.** Node.js actions have higher cold start latency than V8 isolates (~5-20s vs ~1-2s on dev deployments).
- **Higher resource consumption on Convex.** Node actions consume more platform resources.
- **Not portable.** Code that depends on Node APIs won't work if switched back to the default runtime.

### Alternatives
- **Default V8 runtime with lower concurrency.** Set `maxConcurrencyPerWorker: 50` to stay within 64 MB. Requires more executors (workers) to achieve the same throughput.
- **Hybrid approach.** Use V8 for lightweight handlers (no large HTTP responses), Node for heavy handlers. Requires two executor pools.

---

## Benchmark Results

Real-world benchmark with 20,000 Anthropic Sonnet 4 API pipelines (3 stages: translate to Spanish → translate back to English → count letters = 40K real API calls):

| Metric | Standard Mode (100 actions) | Batch Mode (20 executors × 1000) |
|---|---|---|
| 3K tasks wall-clock | ~4 min | 47s |
| 20K tasks wall-clock | ~27 min (estimated) | **75s** |
| Avg job duration | ~103s | **41s** |
| OCC errors | Dozens | 1 |
| "Too many concurrent commits" | Frequent | 0 |
| Failures | — | 0 |
| API rate-limit retries (429s) | — | 6,135 (all successful) |
| Remaining bottleneck | Scheduler + cold starts | Anthropic API rate limits |

The batch mode saturates the external API rate limit — Convex is no longer the bottleneck.

---

## Cross-Cutting Concerns

### Idempotency
The batch system assumes handlers are **not necessarily idempotent** but handles duplicates gracefully:
- A handler might run twice if the executor crashes after execution but before completeBatch.
- The watchdog will sweep the task back to pending and it'll be re-executed.
- For truly non-idempotent work (charging a credit card), this is a real concern.

### Observability
- No built-in metrics or logging beyond `console.log` for memory pressure and `console.error` for failures.
- Standard mode has a `logLevel` option with REPORT/INFO/DEBUG. Batch mode has none.
- No way to see how many tasks are in-flight, what the queue depth is, or how long tasks are taking, except by querying the table directly.

### API Surface
- The `BatchWorkpool` class and `Workpool` class have different APIs: `enqueue` takes a string name (batch) vs. a function reference (standard). Users can't easily switch between modes.
- The `onComplete` mechanism is shared, which is good for interop. Pipeline examples show both modes using the same onComplete handlers.

### Testing
- Comprehensive unit tests for both executor loop and component mutations (~2000 lines).
- Tests use dependency injection (`_ExecutorDeps`) for the executor loop and `convex-test` for component mutations.
- No integration tests that run the full loop against a real Convex backend.

---

## Summary: When to Use Each Mode

| Criterion | Standard Workpool | Batch Workpool |
|---|---|---|
| Tasks < 100 | Preferred | Overkill |
| Tasks > 1000 | Bottlenecked | Preferred |
| I/O-bound (API calls) | OK | Much better |
| CPU-bound | Same | Same (single-threaded) |
| Per-task timeout needed | Yes (10 min each) | No (shared deadline) |
| Dashboard visibility | Full | Limited |
| Crash recovery | Automatic (scheduler) | Watchdog (30s delay) |
| Mutation overhead | High (O(N)) | Low (O(N/200), concurrent) |
| Cold start overhead | Per task | Per executor |
| Code complexity | Low | High |
