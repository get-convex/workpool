# Workpool Load Testing Plan

## TODO

- [ ] Factor the test to separately configure args/context/return value sizes?
- [ ] Variants on big args / context/ returns:
  - [ ] Canceling a big batch
  - [ ] Retrying a lot of tasks
  - [ ] Big tasks that fail
- [ ] Add harness to run tests, capture data, visualize stats

## Overview

This load testing framework is designed to stress-test the workpool component
across various dimensions including argument size, return size, execution
duration, and database pressure.

## Architecture

## Test Scenarios

Run in a separate terminal to monitor status:

```sh
npx convex run test/run:status --watch
```

If something fails, you can run these to clear pending values and cancel the run
before trying again:

```sh
npx convex run --component testWorkpool danger:clearPending '{olderThan: 0}'
npx convex run test/run:cancel
```

### 1. Big Arguments (`scenarios/bigArgs.ts`)

**Purpose**: Test the system's ability to handle large payloads in function
arguments

**Test Parameters**:

This is the full parameterization. Default values shown here (can be omitted).

```sh
npx convex run test/scenarios/bigArgs '{taskCount:50, argSizeBytes:800000, taskType:"mutation", batchEnqueue:false, maxParallelism:50}'
```

### 2. Big Context (`scenarios/bigContext.ts`)

**Purpose**: Test handling of large data in onComplete context variables

**Test Parameters**:

This is the full parameterization. Default values shown here (can be omitted).

```sh
npx convex run test/scenarios/bigContext '{taskCount:50, argSizeBytes:800000, taskType:"mutation", batchEnqueue:false, maxParallelism:50}'
```

### 3. Big Return Types (`scenarios/bigReturnTypes.ts`)

**Purpose**: Test handling of large return values from functions

**Test Parameters**:

This is the full parameterization. Default values shown here (can be omitted).

```sh
npx convex run test/scenarios/bigReturnTypes '{taskCount:50, argSizeBytes:800000, taskType:"mutation", batchEnqueue:false, maxParallelism:50}'
```

### 4. Data Pressure (`scenarios/dataPressure.ts`) (TODO)

**Purpose**: Stress test database read/write operations

**Test Parameters**:

- Read/write sizes: 15MB (configurable)
- Concurrent operations: 50 (default)

### 5. Long Running (`scenarios/longRunning.ts`) (TODO)

**Purpose**: Test action timeout handling and long-duration task management

**Test Parameters**:

- Durations: 5 minutes (configurable)
- Concurrent long tasks: 10 (configurable)

### 6. Self Scheduling (`scenarios/selfScheduling.ts`)

**Purpose**: Test recursive task scheduling via onComplete handlers

**Test Parameters**:

- Chain depth: 2 (configurable)
- Initial tasks: 10 (configurable)
- Parallelism: 1 (configurable)

### 7. Noisy Neighbor (`scenarios/noisyNeighbor.ts`)

**Purpose**: Verify that misbehaving tasks sharing a batched `runWork` action
(up to 32 actions/queries per scheduled function) don't delay or corrupt their
batch-mates. All tasks are enqueued interleaved with a shared future `runAt`
so they land in the same loop iteration and the same runWork chunk, then
per-class latency/outcome is compared.

**Presets** (`preset` arg): `parallelism` (32 equal 5s actions — intra-batch
concurrency), `slowNeighbor`, `failRetry`, `failTerminal`, `queryMix`,
`bigReturn`, `occOnComplete` (all onCompletes contend on one counter doc —
checks exactly-once completion under OCC). Custom mixes via `classes`.

```sh
npx convex run test/scenarios/noisyNeighbor:run '{"preset":"slowNeighbor","pool":"new"}'
```

**Worker-death blast radius** is covered by unit tests in `src/component`
(it needs hooks into component internals to simulate). Findings from live
testing, for the record:

- `scheduler.cancel` on an in-progress action does NOT interrupt it — an
  in-flight runWork batch survives a cancel and completes normally.
- The genuine kill vector is the wrapper *throwing*. A missing stored payload
  used to reject the whole runWork action (31 innocent batch-mates stuck) and
  also wedged the recovery scan's completion batch forever; runWork now
  isolates each item and `complete` tolerates an already-deleted payload.
