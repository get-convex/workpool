# Workpool Load Testing Plan

## Overview

This load testing framework is designed to stress-test the workpool component
across various dimensions including argument size, return size, execution
duration, and database pressure.

## Architecture

### Components

1. **work.ts** - Core configurable work functions
   - `configurableMutation`: Simulates database operations with configurable
     read/write/return sizes
   - `configurableAction`: Simulates long-running operations with configurable
     duration and return size

2. **runTracking.ts** - Test run management
   - Tracks test runs and individual tasks
   - Prevents concurrent test runs
   - Provides real-time status monitoring
   - Manages dynamic workpool configuration

3. **Dynamic Workpool** - A dedicated workpool instance with
   runtime-configurable parallelism

### Data Model

- **runs** table: Tracks test runs with parameters and status
- **tasks** table: Tracks individual tasks with indexed lookup on (runId,
  taskNum)

## Test Scenarios

### 1. Big Arguments (`scenarios/bigArgs.ts`)

**Purpose**: Test the system's ability to handle large payloads in function
arguments

**Test Parameters**:

- Argument sizes: 100KB, 500KB, 800KB (default), 900KB
- Task count: 50 tasks default (configurable)
- Enqueue methods:
  - Batch enqueue (all 50 at once)
  - Individual enqueue (50 separate calls from action)
- Configurable parallelism

### 2. Big Return Types (`scenarios/bigReturnTypes.ts`)

**Purpose**: Test handling of large return values from functions

**Test Parameters**:

- Return sizes: 100KB, 1MB, 5MB, 10MB
- Task types: Both mutations and actions
- Concurrent tasks: Configurable

### 3. Data Pressure (`scenarios/dataPressure.ts`)

**Purpose**: Stress test database read/write operations

**Test Parameters**:

- Read/write sizes: 15MB (configurable)
- Concurrent operations: 50 (default)

### 4. Long Running (`scenarios/longRunning.ts`)

**Purpose**: Test action timeout handling and long-duration task management

**Test Parameters**:

- Durations: 5 minutes (configurable)
- Concurrent long tasks: 10 (configurable)

### 5. Self Scheduling (`scenarios/selfScheduling.ts`)

**Purpose**: Test recursive task scheduling via onComplete handlers

**Test Parameters**:

- Chain depth: 2 (configurable)
- Initial tasks: 10 (configurable)
- Parallelism: 1 (configurable)

## Usage

### Running a Test

1. **Start Test Run**: Start a new test run

```sh
npx convex run test/run '{ scenario: "bigArguments", parameters: { maxParallelism: 20 } }'
```

2. **Monitor Progress**: Check status periodically

```sh
npx convex run test/run:status --watch
```

## Task Completion Tracking

Tasks can complete in two ways:

1. **With onComplete**: The onComplete handler marks the task as done
2. **Without onComplete**: The task marks itself as done before returning

This dual approach ensures accurate tracking regardless of configuration.

## Future Enhancements

1. **Metrics Collection**: Add performance metric recording
2. **Automated Test Suites**: Chain multiple scenarios
3. **Comparison Reports**: Compare runs with different parameters
4. **Resource Monitoring**: Track CPU/memory usage
5. **Error Analysis**: Detailed failure categorization
6. **Query Support**: Add configurable query testing
7. **Leak Detection**: Memory and resource leak monitoring
8. **Visualization**: Dashboard for test results
