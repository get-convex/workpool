import { Workpool, BatchWorkpool } from "@convex-dev/workpool";
import { components } from "./_generated/api";

// Standard mode: 1 action per task, maxParallelism 100
export const standard = new Workpool(components.standardPool, {
  maxParallelism: 100,
  logLevel: "INFO",
});

// Batch mode: 5 executor actions, 1000 concurrent tasks each
export const batch = new BatchWorkpool(components.batchPool, {
  maxWorkers: 5,
  maxConcurrencyPerWorker: 1000,
});
