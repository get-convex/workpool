import { Workpool, BatchWorkpool } from "@convex-dev/workpool";
import { components } from "./_generated/api";

// Standard mode: 1 action per task, maxParallelism 10
export const standard = new Workpool(components.standardPool, {
  maxParallelism: 10,
  logLevel: "INFO",
});

// Batch mode: 2 executor actions, 50 concurrent tasks each
export const batch = new BatchWorkpool(components.batchPool, {
  maxWorkers: 2,
  maxConcurrencyPerWorker: 50,
});
