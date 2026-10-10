import type { CompleteJob } from "./complete.js";

/**
 * Group commit for the completions of one `runBatch` action. The first job to
 * finish commits right away; jobs that finish while a commit is in flight share
 * the next one. A completion never waits on slower work in the same batch.
 */
export function createCompleter(
  completeJobs: (jobs: CompleteJob[]) => Promise<unknown>,
  onBatchFailure: (jobs: CompleteJob[], error: unknown) => Promise<void>,
) {
  let queued: CompleteJob[] = [];
  let flushing: Promise<void> | null = null;

  async function flush() {
    try {
      while (queued.length > 0) {
        const jobs = queued;
        queued = [];
        try {
          await completeJobs(jobs);
        } catch (e) {
          await onBatchFailure(jobs, e);
        }
      }
    } finally {
      flushing = null;
    }
  }

  return {
    /** Resolves once a commit that includes `job` has finished. */
    add(job: CompleteJob): Promise<void> {
      queued.push(job);
      flushing ??= flush();
      return flushing;
    },
  };
}
