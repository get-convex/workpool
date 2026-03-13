# Changelog

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
