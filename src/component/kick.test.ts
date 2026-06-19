import { describe, expect, it } from "vitest";
import { components, internal } from "./_generated/api.js";
import { kickMainLoop } from "./kick.js";
import { WORKER_NAME } from "./shared.js";
import { setupTest } from "./setup.test.js";

describe("kickMainLoop", () => {
  async function seedRunningState(
    t: ReturnType<typeof setupTest>,
    maxParallelism = 1,
  ) {
    await t.run(async (ctx) => {
      await ctx.db.insert("globals", {
        logLevel: "WARN",
        maxParallelism,
      });
      const workId = await ctx.db.insert("work", {
        fnType: "action",
        fnHandle: "test_handle",
        fnName: "test_handle",
        fnArgs: {},
        attempts: 0,
      });
      const scheduledId = await ctx.scheduler.runAfter(
        60_000,
        internal.worker.runActionWrapper,
        {
          workId,
          fnHandle: "test_handle",
          fnArgs: {},
          logLevel: "WARN",
          attempt: 0,
        },
      );
      await ctx.db.insert("internalState", {
        generation: 0n,
        segmentCursors: { incoming: 0n, completion: 0n, cancelation: 0n },
        lastRecovery: 0n,
        report: {
          completed: 0,
          succeeded: 0,
          failed: 0,
          retries: 0,
          canceled: 0,
          lastReportTs: Date.now(),
        },
        running: [{ workId, scheduledId, started: Date.now() }],
      });
    });
  }

  async function workerStatus(t: ReturnType<typeof setupTest>) {
    return await t.query(components.batchWorker.lib.status, {
      name: WORKER_NAME,
    });
  }

  it("pings on enqueue", async () => {
    const t = setupTest();
    await seedRunningState(t, 1);

    await t.run((ctx) => kickMainLoop(ctx, "enqueue"));

    expect((await workerStatus(t))?.kind).toBe("running");
  });

  it("pings on every source even when saturated", async () => {
    for (const source of [
      "enqueue",
      "complete",
      "retry",
      "cancel",
      "kick",
    ] as const) {
      const t = setupTest();
      await seedRunningState(t, 1);

      await t.run((ctx) => kickMainLoop(ctx, source));

      expect((await workerStatus(t))?.kind).toBe("running");
    }
  });
});
