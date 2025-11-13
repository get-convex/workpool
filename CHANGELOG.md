# Changelog

## 0.3.1 alpha

- Only warn if the limit is set to >100

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
