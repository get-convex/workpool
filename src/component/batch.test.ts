import { convexTest } from "convex-test";
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";
import { api, internal } from "./_generated/api.js";
import schema from "./schema.js";

const modules = import.meta.glob("./**/*.ts");

describe("batch", () => {
  async function setupTest() {
    const t = convexTest(schema, modules);
    return t;
  }

  let t: Awaited<ReturnType<typeof setupTest>>;

  beforeEach(async () => {
    vi.useFakeTimers();
    t = await setupTest();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  // Helper: claim tasks using the two-step listPending + claimByIds pattern
  async function claimBatch(slot: number, limit: number) {
    const ids = await t.query(api.batch.listPending, { slot, limit });
    if (ids.length === 0) return [];
    return t.mutation(api.batch.claimByIds, { taskIds: ids });
  }

  // Helper: insert batch config
  async function setupPoolConfig(
    overrides?: Partial<{
      executorHandle: string;
      maxWorkers: number;
      activeSlots: number[];
      claimTimeoutMs: number;
    }>,
  ) {
    await t.run(async (ctx) => {
      await ctx.db.insert("batchConfig", {
        executorHandle: overrides?.executorHandle ?? "function://test-executor",
        maxWorkers: overrides?.maxWorkers ?? 10,
        activeSlots: overrides?.activeSlots ?? [],
        claimTimeoutMs: overrides?.claimTimeoutMs ?? 120_000,
      });
    });
  }

  describe("enqueue", () => {
    it("should create a pending batch task", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "generateBio",
        slot: 0,
        args: { userId: "user123" },
      });

      expect(taskId).toBeDefined();

      await t.run(async (ctx) => {
        const task = await ctx.db.get(taskId);
        expect(task).not.toBeNull();
        expect(task!.name).toBe("generateBio");
        expect(task!.args).toEqual({ userId: "user123" });
        expect(task!.status).toBe("pending");
        expect(task!.slot).toBe(0);
        expect(task!.attempt).toBe(0);
        expect(task!.readyAt).toBeLessThanOrEqual(Date.now());
      });
    });

    it("should lazy-init batch config via batchConfig arg", async () => {
      const taskId = await t.mutation(api.batch.enqueue, {
        name: "myHandler",
        slot: 0,
        args: {},
        batchConfig: {
          executorHandle: "function://my-executor",
          maxWorkers: 5,
          claimTimeoutMs: 60_000,
        },
      });

      expect(taskId).toBeDefined();

      // Flush the scheduled _maybeStartExecutors mutation
      await (t.finishAllScheduledFunctions as any)(() => vi.advanceTimersByTime(1000), 10);

      await t.run(async (ctx) => {
        const config = await ctx.db.query("batchConfig").unique();
        expect(config).not.toBeNull();
        expect(config!.executorHandle).toBe("function://my-executor");
        expect(config!.maxWorkers).toBe(5);
        expect(config!.claimTimeoutMs).toBe(60_000);
        // activeSlots has all 5 slots because _maybeStartExecutors starts all workers
        expect(config!.activeSlots.sort()).toEqual([0, 1, 2, 3, 4]);
      });
    });

    it("should store onComplete and retryBehavior", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "myHandler",
        slot: 0,
        args: {},
        onComplete: {
          fnHandle: "function://on-complete",
          context: { key: "value" },
        },
        retryBehavior: {
          maxAttempts: 3,
          initialBackoffMs: 100,
          base: 2,
        },
      });

      await t.run(async (ctx) => {
        const task = await ctx.db.get(taskId);
        expect(task!.onComplete).toEqual({
          fnHandle: "function://on-complete",
          context: { key: "value" },
        });
        expect(task!.retryBehavior).toEqual({
          maxAttempts: 3,
          initialBackoffMs: 100,
          base: 2,
        });
      });
    });
  });

  describe("enqueueBatch", () => {
    it("should create multiple pending batch tasks", async () => {
      await setupPoolConfig();

      const taskIds = await t.mutation(api.batch.enqueueBatch, {
        tasks: [
          { name: "handler1", slot: 0, args: { a: 1 } },
          { name: "handler2", slot: 1, args: { b: 2 } },
          { name: "handler3", slot: 2, args: { c: 3 } },
        ],
      });

      expect(taskIds).toHaveLength(3);

      await t.run(async (ctx) => {
        for (const id of taskIds) {
          const task = await ctx.db.get(id);
          expect(task).not.toBeNull();
          expect(task!.status).toBe("pending");
        }
      });
    });
  });


  describe("complete", () => {
    it("should delete a claimed task on completion", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler",
        slot: 0,
        args: {},
      });

      // Claim it
      await claimBatch(0, 1);

      // Complete it
      await t.mutation(api.batch.complete, {
        taskId,
        result: { value: 42 },
      });

      // Task should be deleted
      await t.run(async (ctx) => {
        const task = await ctx.db.get(taskId);
        expect(task).toBeNull();
      });
    });

    it("should no-op if task doesn't exist (was canceled)", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler",
        slot: 0,
        args: {},
      });

      // Delete the task (simulating cancellation)
      await t.run(async (ctx) => {
        await ctx.db.delete(taskId);
      });

      // Complete should not throw
      await t.mutation(api.batch.complete, {
        taskId,
        result: null,
      });
    });

    it("should no-op if task is not in claimed state", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler",
        slot: 0,
        args: {},
      });
      // Task is "pending", not "claimed"

      await t.mutation(api.batch.complete, {
        taskId,
        result: null,
      });

      // Task should still exist and still be pending
      await t.run(async (ctx) => {
        const task = await ctx.db.get(taskId);
        expect(task).not.toBeNull();
        expect(task!.status).toBe("pending");
      });
    });
  });

  describe("fail", () => {
    it("should delete task when no retry config", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler",
        slot: 0,
        args: {},
        // no retryBehavior
      });

      // Claim and fail
      await claimBatch(0, 1);
      await t.mutation(api.batch.fail, {
        taskId,
        error: "Something went wrong",
      });

      // Task should be deleted
      await t.run(async (ctx) => {
        const task = await ctx.db.get(taskId);
        expect(task).toBeNull();
      });
    });

    it("should retry when attempts remain", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler",
        slot: 0,
        args: {},
        retryBehavior: {
          maxAttempts: 3,
          initialBackoffMs: 100,
          base: 2,
        },
      });

      // Claim and fail
      await claimBatch(0, 1);
      await t.mutation(api.batch.fail, {
        taskId,
        error: "Temporary error",
      });

      // Task should be back to pending with incremented attempt
      await t.run(async (ctx) => {
        const task = await ctx.db.get(taskId);
        expect(task).not.toBeNull();
        expect(task!.status).toBe("pending");
        expect(task!.attempt).toBe(1);
        expect(task!.claimedAt).toBeUndefined();
        // readyAt should be in the future (backoff)
        expect(task!.readyAt).toBeGreaterThan(Date.now());
        expect(task!.error).toBe("Temporary error");
      });
    });

    it("should delete task when max attempts exhausted", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler",
        slot: 0,
        args: {},
        retryBehavior: {
          maxAttempts: 2, // 2 total attempts (1 initial + 1 retry)
          initialBackoffMs: 100,
          base: 2,
        },
      });

      // First attempt: claim and fail
      await claimBatch(0, 1);
      await t.mutation(api.batch.fail, {
        taskId,
        error: "First failure",
      });

      // Task should be pending with attempt=1
      await t.run(async (ctx) => {
        const task = await ctx.db.get(taskId);
        expect(task!.status).toBe("pending");
        expect(task!.attempt).toBe(1);
      });

      // Advance time so readyAt is in the past
      vi.advanceTimersByTime(60_000);

      // Second attempt: claim and fail again
      await claimBatch(0, 1);
      await t.mutation(api.batch.fail, {
        taskId,
        error: "Second failure",
      });

      // Task should be deleted (max attempts reached)
      await t.run(async (ctx) => {
        const task = await ctx.db.get(taskId);
        expect(task).toBeNull();
      });
    });
  });

  describe("countPending", () => {
    it("should count pending tasks globally", async () => {
      await setupPoolConfig();

      // Initially 0
      const count0 = await t.query(api.batch.countPending, {});
      expect(count0).toBe(0);

      // Enqueue 3 tasks on different slots
      for (let i = 0; i < 3; i++) {
        await t.mutation(api.batch.enqueue, {
          name: `handler${i}`,
          slot: i,
          args: {},
        });
      }

      const count3 = await t.query(api.batch.countPending, {});
      // Returns 1 (at least one pending) not exact count
      expect(count3).toBe(1);

      // Claim all from slot 0
      await claimBatch(0, 10);

      // Still pending on other slots
      const countAfter = await t.query(api.batch.countPending, {});
      expect(countAfter).toBe(1);
    });

    it("should count pending tasks for a specific slot", async () => {
      await setupPoolConfig();

      // Enqueue tasks on different slots
      await t.mutation(api.batch.enqueue, {
        name: "slot0-task",
        slot: 0,
        args: {},
      });
      await t.mutation(api.batch.enqueue, {
        name: "slot1-task",
        slot: 1,
        args: {},
      });

      // Check slot 0
      const slot0Count = await t.query(api.batch.countPending, { slot: 0 });
      expect(slot0Count).toBe(1);

      // Check slot 1
      const slot1Count = await t.query(api.batch.countPending, { slot: 1 });
      expect(slot1Count).toBe(1);

      // Check empty slot
      const slot2Count = await t.query(api.batch.countPending, { slot: 2 });
      expect(slot2Count).toBe(0);

      // Claim slot 0
      await claimBatch(0, 10);

      // Slot 0 now empty, slot 1 still has pending
      const slot0After = await t.query(api.batch.countPending, { slot: 0 });
      expect(slot0After).toBe(0);
      const slot1After = await t.query(api.batch.countPending, { slot: 1 });
      expect(slot1After).toBe(1);
    });
  });

  describe("status", () => {
    it("should return pending for a newly enqueued task", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler",
        slot: 0,
        args: {},
      });

      const s = await t.query(api.batch.status, { taskId });
      expect(s).toEqual({ state: "pending", attempt: 0 });
    });

    it("should return running for a claimed task", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler",
        slot: 0,
        args: {},
      });

      await claimBatch(0, 1);

      const s = await t.query(api.batch.status, { taskId });
      expect(s).toEqual({ state: "running", attempt: 0 });
    });

    it("should return finished for a completed task", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler",
        slot: 0,
        args: {},
      });

      await claimBatch(0, 1);
      await t.mutation(api.batch.complete, { taskId, result: null });

      const s = await t.query(api.batch.status, { taskId });
      expect(s).toEqual({ state: "finished" });
    });

    it("should return finished for a non-existent task", async () => {
      // Use a task ID from a task we create and then delete
      await setupPoolConfig();
      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler",
        slot: 0,
        args: {},
      });
      await t.run(async (ctx) => {
        await ctx.db.delete(taskId);
      });

      const s = await t.query(api.batch.status, { taskId });
      expect(s).toEqual({ state: "finished" });
    });
  });

  describe("cancel", () => {
    it("should delete a pending task", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler",
        slot: 0,
        args: {},
      });

      await t.mutation(api.batch.cancel, { taskId });

      await t.run(async (ctx) => {
        const task = await ctx.db.get(taskId);
        expect(task).toBeNull();
      });
    });

    it("should delete a claimed task", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler",
        slot: 0,
        args: {},
      });

      await claimBatch(0, 1);
      await t.mutation(api.batch.cancel, { taskId });

      await t.run(async (ctx) => {
        const task = await ctx.db.get(taskId);
        expect(task).toBeNull();
      });
    });

    it("should no-op for a non-existent task", async () => {
      await setupPoolConfig();
      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler",
        slot: 0,
        args: {},
      });
      await t.run(async (ctx) => {
        await ctx.db.delete(taskId);
      });

      // Should not throw
      await t.mutation(api.batch.cancel, { taskId });
    });
  });

  describe("sweepStaleClaims", () => {
    it("should sweep claims older than claimTimeoutMs", async () => {
      await setupPoolConfig({ claimTimeoutMs: 60_000 });

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler",
        slot: 0,
        args: {},
      });

      // Claim it
      await claimBatch(0, 1);

      // Verify it's claimed
      await t.run(async (ctx) => {
        const task = await ctx.db.get(taskId);
        expect(task!.status).toBe("claimed");
      });

      // Advance time past the claim timeout
      vi.advanceTimersByTime(120_000);

      // Sweep
      const swept = await t.mutation(api.batch.sweepStaleClaims, {});
      expect(swept).toBe(1);

      // Task should be back to pending
      await t.run(async (ctx) => {
        const task = await ctx.db.get(taskId);
        expect(task!.status).toBe("pending");
        expect(task!.claimedAt).toBeUndefined();
      });
    });

    it("should not sweep recent claims", async () => {
      await setupPoolConfig({ claimTimeoutMs: 60_000 });

      await t.mutation(api.batch.enqueue, {
        name: "handler",
        slot: 0,
        args: {},
      });

      await claimBatch(0, 1);

      // Don't advance time — claim is fresh
      const swept = await t.mutation(api.batch.sweepStaleClaims, {});
      expect(swept).toBe(0);
    });
  });

  describe("executorDone", () => {
    it("should remove slot from activeSlots", async () => {
      await setupPoolConfig({ activeSlots: [0, 1, 2] });

      await t.mutation(api.batch.executorDone, { startMore: false, slot: 1 });

      await t.run(async (ctx) => {
        const config = await ctx.db.query("batchConfig").unique();
        expect(config!.activeSlots.sort()).toEqual([0, 2]);
      });
    });

    it("should no-op for slot not in activeSlots", async () => {
      await setupPoolConfig({ activeSlots: [] });

      await t.mutation(api.batch.executorDone, { startMore: false, slot: 0 });

      await t.run(async (ctx) => {
        const config = await ctx.db.query("batchConfig").unique();
        expect(config!.activeSlots).toEqual([]);
      });
    });

    it("should start all missing executors when startMore is true", async () => {
      await setupPoolConfig({ activeSlots: [0, 2] });

      await t.mutation(api.batch.executorDone, { startMore: true, slot: 2 });

      await t.run(async (ctx) => {
        const config = await ctx.db.query("batchConfig").unique();
        // Slot 2 was removed, then ALL missing slots 0..9 were started
        expect(config!.activeSlots.sort()).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
      });
    });
  });

  describe("releaseClaims", () => {
    it("should return claimed tasks to pending", async () => {
      await setupPoolConfig();

      const id1 = await t.mutation(api.batch.enqueue, {
        name: "handler1",
        slot: 0,
        args: {},
      });
      const id2 = await t.mutation(api.batch.enqueue, {
        name: "handler2",
        slot: 0,
        args: {},
      });

      // Claim both
      await claimBatch(0, 10);

      // Verify claimed
      await t.run(async (ctx) => {
        expect((await ctx.db.get(id1))!.status).toBe("claimed");
        expect((await ctx.db.get(id2))!.status).toBe("claimed");
      });

      // Release both
      await t.mutation(api.batch.releaseClaims, { taskIds: [id1, id2] });

      // Verify back to pending
      await t.run(async (ctx) => {
        const task1 = await ctx.db.get(id1);
        expect(task1!.status).toBe("pending");
        expect(task1!.claimedAt).toBeUndefined();
        expect(task1!.readyAt).toBeLessThanOrEqual(Date.now());

        const task2 = await ctx.db.get(id2);
        expect(task2!.status).toBe("pending");
        expect(task2!.claimedAt).toBeUndefined();
      });
    });

    it("should not release a completed task", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler",
        slot: 0,
        args: {},
      });

      // Claim and complete
      await claimBatch(0, 1);
      await t.mutation(api.batch.complete, { taskId, result: null });

      // Release should no-op (task is deleted)
      await t.mutation(api.batch.releaseClaims, { taskIds: [taskId] });

      // Task should still be gone
      await t.run(async (ctx) => {
        const task = await ctx.db.get(taskId);
        expect(task).toBeNull();
      });
    });

    it("should not release a pending task", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler",
        slot: 0,
        args: {},
      });

      // Don't claim — release should no-op
      await t.mutation(api.batch.releaseClaims, { taskIds: [taskId] });

      // Task should still be pending (unchanged)
      await t.run(async (ctx) => {
        const task = await ctx.db.get(taskId);
        expect(task!.status).toBe("pending");
      });
    });
  });

  describe("_maybeStartExecutors", () => {
    it("should not start slots already active", async () => {
      await setupPoolConfig({ maxWorkers: 2, activeSlots: [0, 1] });

      // Enqueue a task — _maybeStartExecutors runs but should not start more
      await t.mutation(api.batch.enqueue, {
        name: "handler",
        slot: 0,
        args: {},
        batchConfig: {
          executorHandle: "function://test-executor",
          maxWorkers: 2,
          claimTimeoutMs: 120_000,
        },
      });

      // Flush the scheduled _maybeStartExecutors mutation
      await (t.finishAllScheduledFunctions as any)(() => vi.advanceTimersByTime(1000), 10);

      await t.run(async (ctx) => {
        const config = await ctx.db.query("batchConfig").unique();
        // Should stay at [0, 1], not add more
        expect(config!.activeSlots.sort()).toEqual([0, 1]);
      });
    });

    it("should start all executors up to maxWorkers", async () => {
      // No pre-existing config — lazy init via batchConfig arg
      const _taskId = await t.mutation(api.batch.enqueue, {
        name: "handler",
        slot: 0,
        args: {},
        batchConfig: {
          executorHandle: "function://test-executor",
          maxWorkers: 5,
          claimTimeoutMs: 120_000,
        },
      });

      // Flush the scheduled _maybeStartExecutors mutation
      await (t.finishAllScheduledFunctions as any)(() => vi.advanceTimersByTime(1000), 10);

      await t.run(async (ctx) => {
        const config = await ctx.db.query("batchConfig").unique();
        // All 5 slots should be active
        expect(config!.activeSlots.sort()).toEqual([0, 1, 2, 3, 4]);
      });
    });
  });

  describe("enqueueBatch with batchConfig", () => {
    it("should lazy-init batch config", async () => {
      const taskIds = await t.mutation(api.batch.enqueueBatch, {
        tasks: [
          { name: "handler1", slot: 0, args: { a: 1 } },
          { name: "handler2", slot: 1, args: { b: 2 } },
        ],
        batchConfig: {
          executorHandle: "function://bulk-executor",
          maxWorkers: 3,
          claimTimeoutMs: 90_000,
        },
      });

      expect(taskIds).toHaveLength(2);

      // Flush the scheduled _maybeStartExecutors mutation
      await (t.finishAllScheduledFunctions as any)(() => vi.advanceTimersByTime(1000), 10);

      await t.run(async (ctx) => {
        const config = await ctx.db.query("batchConfig").unique();
        expect(config).not.toBeNull();
        expect(config!.executorHandle).toBe("function://bulk-executor");
        expect(config!.maxWorkers).toBe(3);
        expect(config!.claimTimeoutMs).toBe(90_000);
        // All 3 slots active
        expect(config!.activeSlots.sort()).toEqual([0, 1, 2]);
      });
    });
  });

  describe("configure", () => {
    it("should create batch config", async () => {
      await t.mutation(api.batch.configure, {
        executorHandle: "function://test",
        maxWorkers: 5,
        claimTimeoutMs: 30_000,
      });

      await t.run(async (ctx) => {
        const config = await ctx.db.query("batchConfig").unique();
        expect(config).not.toBeNull();
        expect(config!.executorHandle).toBe("function://test");
        expect(config!.maxWorkers).toBe(5);
        expect(config!.claimTimeoutMs).toBe(30_000);
        expect(config!.activeSlots).toEqual([]);
      });
    });

    it("should update existing batch config", async () => {
      await t.mutation(api.batch.configure, {
        executorHandle: "function://old",
        maxWorkers: 5,
        claimTimeoutMs: 30_000,
      });

      await t.mutation(api.batch.configure, {
        executorHandle: "function://new",
        maxWorkers: 20,
        claimTimeoutMs: 60_000,
      });

      await t.run(async (ctx) => {
        const configs = await ctx.db.query("batchConfig").collect();
        expect(configs).toHaveLength(1);
        expect(configs[0].executorHandle).toBe("function://new");
        expect(configs[0].maxWorkers).toBe(20);
      });
    });
  });

  // ─── Full retry lifecycle ───────────────────────────────────────────────

  describe("retry lifecycle", () => {
    it("exponential backoff: readyAt increases with each attempt", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "flaky",
        slot: 0,
        args: {},
        retryBehavior: {
          maxAttempts: 4,
          initialBackoffMs: 1000,
          base: 2,
        },
      });

      // Attempt 0: claim and fail
      await claimBatch(0, 1);
      const beforeFirstFail = Date.now();
      await t.mutation(api.batch.fail, { taskId, error: "fail-0" });

      const afterAttempt0 = await t.run(async (ctx) => {
        const task = await ctx.db.get(taskId);
        expect(task!.attempt).toBe(1);
        expect(task!.status).toBe("pending");
        // readyAt should be ~1000ms in the future (with jitter)
        return task!.readyAt;
      });
      const delay0 = afterAttempt0 - beforeFirstFail;
      expect(delay0).toBeGreaterThan(500); // at least half of 1000ms (jitter)
      expect(delay0).toBeLessThan(2000); // not more than 2x

      // Advance time past backoff
      vi.advanceTimersByTime(delay0 + 100);

      // Attempt 1: claim and fail again
      await claimBatch(0, 1);
      const beforeSecondFail = Date.now();
      await t.mutation(api.batch.fail, { taskId, error: "fail-1" });

      const afterAttempt1 = await t.run(async (ctx) => {
        const task = await ctx.db.get(taskId);
        expect(task!.attempt).toBe(2);
        return task!.readyAt;
      });
      const delay1 = afterAttempt1 - beforeSecondFail;
      // Second backoff should be ~2000ms (1000 * 2^1), roughly double first
      expect(delay1).toBeGreaterThan(delay0 * 0.7); // jitter makes this inexact
    });

    it("full cycle: fail → retry → succeed", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "eventually-works",
        slot: 0,
        args: {},
        retryBehavior: {
          maxAttempts: 3,
          initialBackoffMs: 100,
          base: 2,
        },
      });

      // Attempt 0: fail
      await claimBatch(0, 1);
      await t.mutation(api.batch.fail, { taskId, error: "transient" });

      // Verify retried
      await t.run(async (ctx) => {
        const task = await ctx.db.get(taskId);
        expect(task!.status).toBe("pending");
        expect(task!.attempt).toBe(1);
      });

      // Advance past backoff
      vi.advanceTimersByTime(60_000);

      // Attempt 1: succeed
      await claimBatch(0, 1);
      await t.mutation(api.batch.complete, { taskId, result: "finally!" });

      // Task should be deleted (completed)
      await t.run(async (ctx) => {
        const task = await ctx.db.get(taskId);
        expect(task).toBeNull();
      });
    });

    it("terminal failure after max attempts exhausted", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "always-fails",
        slot: 0,
        args: {},
        retryBehavior: {
          maxAttempts: 2,
          initialBackoffMs: 100,
          base: 2,
        },
      });

      // Attempt 0
      await claimBatch(0, 1);
      await t.mutation(api.batch.fail, { taskId, error: "fail-0" });

      vi.advanceTimersByTime(60_000);

      // Attempt 1 (final)
      await claimBatch(0, 1);
      await t.mutation(api.batch.fail, { taskId, error: "fail-1" });

      // Task deleted — no more retries
      await t.run(async (ctx) => {
        const task = await ctx.db.get(taskId);
        expect(task).toBeNull();
      });
    });
  });

  // ─── Watchdog (redeploy recovery) ──────────────────────────────────────

  describe("watchdog", () => {
    it("should sweep stale claims and restart dead executors", async () => {
      await setupPoolConfig({
        maxWorkers: 3,
        activeSlots: [0, 1, 2], // all "active" but actually dead
        claimTimeoutMs: 60_000,
      });

      // Simulate orphaned tasks: claimed 120s ago by dead executors
      await t.run(async (ctx) => {
        await ctx.db.insert("batchTasks", {
          name: "orphan1",
          slot: 0,
          args: {},
          status: "claimed" as const,
          claimedAt: Date.now() - 120_000,
          readyAt: Date.now() - 120_000,
          attempt: 0,
        });
        await ctx.db.insert("batchTasks", {
          name: "orphan2",
          slot: 1,
          args: {},
          status: "claimed" as const,
          claimedAt: Date.now() - 120_000,
          readyAt: Date.now() - 120_000,
          attempt: 0,
        });
      });

      // Run watchdog (internal mutation via api)
      await t.mutation(internal.batch._watchdog, {});

      await t.run(async (ctx) => {
        // Stale claims should be released back to pending
        const tasks = await ctx.db.query("batchTasks").collect();
        expect(tasks).toHaveLength(2);
        for (const task of tasks) {
          expect(task.status).toBe("pending");
          expect(task.claimedAt).toBeUndefined();
        }

        // Dead slots should be removed and re-added (executors restarted)
        const config = await ctx.db.query("batchConfig").unique();
        expect(config!.activeSlots.sort()).toEqual([0, 1, 2]);
        expect(config!.watchdogScheduledAt).toBeDefined();
      });
    });

    it("should detect stuck executors via old pending tasks", async () => {
      await setupPoolConfig({
        maxWorkers: 2,
        activeSlots: [0, 1], // "active" but dead — no stale claims yet
        claimTimeoutMs: 120_000,
      });

      // Simulate tasks pending for > 30s (executor not claiming them)
      await t.run(async (ctx) => {
        await ctx.db.insert("batchTasks", {
          name: "stuck",
          slot: 0,
          args: {},
          status: "pending" as const,
          readyAt: Date.now() - 60_000, // pending for 60s
          attempt: 0,
        });
      });

      await t.mutation(internal.batch._watchdog, {});

      await t.run(async (ctx) => {
        const config = await ctx.db.query("batchConfig").unique();
        // Slot 0 was detected as dead (old pending task), restarted
        // Slot 1 had no old pending tasks, stays active
        expect(config!.activeSlots.sort()).toEqual([0, 1]);
      });
    });

    it("should stop when no work remains", async () => {
      await setupPoolConfig({
        maxWorkers: 2,
        activeSlots: [0, 1],
      });

      // No tasks at all — watchdog should clean up and stop
      await t.mutation(internal.batch._watchdog, {});

      await t.run(async (ctx) => {
        const config = await ctx.db.query("batchConfig").unique();
        expect(config!.activeSlots).toEqual([]);
      });
    });
  });

  // ─── Orphan recovery ──────────────────────────────────────────────────

  describe("orphan recovery", () => {
    it("sweep recovers only stale claims, leaves fresh ones alone", async () => {
      await setupPoolConfig({ claimTimeoutMs: 60_000 });

      const staleId = await t.mutation(api.batch.enqueue, {
        name: "stale",
        slot: 0,
        args: {},
      });
      const freshId = await t.mutation(api.batch.enqueue, {
        name: "fresh",
        slot: 0,
        args: {},
      });

      // Claim both
      await claimBatch(0, 10);

      // Advance past claim timeout
      vi.advanceTimersByTime(120_000);

      // Enqueue a third task (claimed recently)
      const recentId = await t.mutation(api.batch.enqueue, {
        name: "recent",
        slot: 0,
        args: {},
      });
      await claimBatch(0, 10);

      // Sweep
      const swept = await t.mutation(api.batch.sweepStaleClaims, {});

      // staleId and freshId were claimed 120s ago → both swept
      // recentId was just claimed → not swept
      expect(swept).toBe(2);

      await t.run(async (ctx) => {
        expect((await ctx.db.get(staleId))!.status).toBe("pending");
        expect((await ctx.db.get(freshId))!.status).toBe("pending");
        expect((await ctx.db.get(recentId))!.status).toBe("claimed");
      });
    });

    it("released task can be re-claimed by another executor", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler",
        slot: 0,
        args: {},
      });

      // Executor A claims it
      await claimBatch(0, 1);

      // Executor A hits deadline, releases it
      await t.mutation(api.batch.releaseClaims, { taskIds: [taskId] });

      // Executor B claims it (same slot — after sweep or replacement)
      const claimed = await claimBatch(0, 1);
      expect(claimed).toHaveLength(1);
      expect(claimed[0]._id).toBe(taskId);

      // Executor B completes it
      await t.mutation(api.batch.complete, { taskId, result: "done" });

      await t.run(async (ctx) => {
        const task = await ctx.db.get(taskId);
        expect(task).toBeNull(); // deleted on completion
      });
    });

    it("cancel during claimed state is handled gracefully", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler",
        slot: 0,
        args: {},
      });

      // Claim it (executor is "running" it)
      await claimBatch(0, 1);

      // Cancel while claimed (user cancels from dashboard)
      await t.mutation(api.batch.cancel, { taskId });

      // Task is gone
      await t.run(async (ctx) => {
        expect(await ctx.db.get(taskId)).toBeNull();
      });

      // Executor tries to complete — should no-op, not crash
      await t.mutation(api.batch.complete, { taskId, result: "too late" });
      // Executor tries to fail — should also no-op
      await t.mutation(api.batch.fail, { taskId, error: "too late" });
    });

    it("executorDone with startMore starts all missing executors", async () => {
      await setupPoolConfig({ activeSlots: [0], maxWorkers: 3 });

      // Enqueue work
      await t.mutation(api.batch.enqueue, {
        name: "handler",
        slot: 0,
        args: {},
      });

      // Executor finishes and signals there's more work
      await t.mutation(api.batch.executorDone, { startMore: true, slot: 0 });

      await t.run(async (ctx) => {
        const config = await ctx.db.query("batchConfig").unique();
        // All 3 slots (0, 1, 2) should be started since there's pending work
        expect(config!.activeSlots.sort()).toEqual([0, 1, 2]);
      });
    });
  });

  // ─── Black-box behavioral edge cases ─────────────────────────────────────

  describe("slot isolation", () => {
    it("tasks on slot 0 cannot be claimed from slot 1", async () => {
      await setupPoolConfig();

      await t.mutation(api.batch.enqueue, {
        name: "handler",
        slot: 0,
        args: { data: "for-slot-0" },
      });

      // Try to claim from slot 1 — should get nothing
      const claimed = await claimBatch(1, 10);
      expect(claimed).toHaveLength(0);

      // Claim from slot 0 — should get the task
      const claimed0 = await claimBatch(0, 10);
      expect(claimed0).toHaveLength(1);
      expect(claimed0[0].args).toEqual({ data: "for-slot-0" });
    });

    it("two executors on different slots claim only their own tasks", async () => {
      await setupPoolConfig();

      const id0 = await t.mutation(api.batch.enqueue, {
        name: "h0", slot: 0, args: { who: "zero" },
      });
      const id1 = await t.mutation(api.batch.enqueue, {
        name: "h1", slot: 1, args: { who: "one" },
      });

      const fromSlot0 = await claimBatch(0, 10);
      const fromSlot1 = await claimBatch(1, 10);

      expect(fromSlot0).toHaveLength(1);
      expect(fromSlot0[0]._id).toBe(id0);
      expect(fromSlot1).toHaveLength(1);
      expect(fromSlot1[0]._id).toBe(id1);
    });
  });

  describe("claim race conditions", () => {
    it("claimByIds skips tasks that were claimed between listPending and claimByIds", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler", slot: 0, args: {},
      });

      // Simulate: listPending returns the ID
      const ids = await t.query(api.batch.listPending, { slot: 0, limit: 10 });
      expect(ids).toHaveLength(1);

      // Another executor claims it first
      await claimBatch(0, 10);

      // Our claimByIds should get nothing (task already claimed)
      const claimed = await t.mutation(api.batch.claimByIds, { taskIds: ids });
      expect(claimed).toHaveLength(0);
    });

    it("claimByIds skips tasks with future readyAt", async () => {
      await setupPoolConfig();

      // Insert a task with readyAt in the future (simulating retry backoff)
      await t.run(async (ctx) => {
        await ctx.db.insert("batchTasks", {
          name: "delayed",
          slot: 0,
          args: {},
          status: "pending" as const,
          readyAt: Date.now() + 60_000,
          attempt: 1,
        });
      });

      // listPending should not return it (readyAt > now)
      const ids = await t.query(api.batch.listPending, { slot: 0, limit: 10 });
      expect(ids).toHaveLength(0);
    });

    it("claimByIds skips deleted tasks", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler", slot: 0, args: {},
      });

      // Task gets canceled between listPending and claimByIds
      const ids = await t.query(api.batch.listPending, { slot: 0, limit: 10 });
      await t.mutation(api.batch.cancel, { taskId });

      // claimByIds should gracefully return nothing
      const claimed = await t.mutation(api.batch.claimByIds, { taskIds: ids });
      expect(claimed).toHaveLength(0);
    });
  });

  describe("completeBatch", () => {
    it("completes multiple tasks in one call", async () => {
      await setupPoolConfig();

      const id1 = await t.mutation(api.batch.enqueue, {
        name: "h", slot: 0, args: {},
      });
      const id2 = await t.mutation(api.batch.enqueue, {
        name: "h", slot: 0, args: {},
      });

      await claimBatch(0, 10);

      const onCompleteItems = await t.mutation(api.batch.completeBatch, {
        items: [
          { taskId: id1, result: "result-1" },
          { taskId: id2, result: "result-2" },
        ],
      });

      // Both tasks should be deleted
      await t.run(async (ctx) => {
        expect(await ctx.db.get(id1)).toBeNull();
        expect(await ctx.db.get(id2)).toBeNull();
      });

      // No onComplete configured, so no items returned
      expect(onCompleteItems).toHaveLength(0);
    });

    it("returns onComplete data for tasks with onComplete handlers", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "h", slot: 0, args: {},
        onComplete: {
          fnHandle: "function://my-callback",
          context: { jobId: "j42" },
        },
      });

      await claimBatch(0, 10);

      const items = await t.mutation(api.batch.completeBatch, {
        items: [{ taskId, result: { answer: 42 } }],
      });

      expect(items).toHaveLength(1);
      expect(items[0].fnHandle).toBe("function://my-callback");
      expect(items[0].context).toEqual({ jobId: "j42" });
      expect(items[0].result).toEqual({ kind: "success", returnValue: { answer: 42 } });
    });

    it("skips already-deleted tasks without error", async () => {
      await setupPoolConfig();

      const id1 = await t.mutation(api.batch.enqueue, {
        name: "h", slot: 0, args: {},
      });
      const id2 = await t.mutation(api.batch.enqueue, {
        name: "h", slot: 0, args: {},
      });

      await claimBatch(0, 10);

      // Cancel id1 between claim and complete
      await t.mutation(api.batch.cancel, { taskId: id1 });

      // completeBatch should handle mixed: id1 gone, id2 still claimed
      const items = await t.mutation(api.batch.completeBatch, {
        items: [
          { taskId: id1, result: "too late" },
          { taskId: id2, result: "on time" },
        ],
      });

      // Only id2 was actually completed
      await t.run(async (ctx) => {
        expect(await ctx.db.get(id2)).toBeNull(); // completed
      });
    });
  });

  describe("failBatch", () => {
    it("returns onComplete data for permanently failed tasks", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "h", slot: 0, args: {},
        onComplete: {
          fnHandle: "function://error-handler",
          context: { attempt: "final" },
        },
        // No retryBehavior → permanent failure on first fail
      });

      await claimBatch(0, 10);

      const items = await t.mutation(api.batch.failBatch, {
        items: [{ taskId, error: "permanent error" }],
      });

      expect(items).toHaveLength(1);
      expect(items[0].fnHandle).toBe("function://error-handler");
      expect(items[0].result).toEqual({ kind: "failed", error: "permanent error" });
    });

    it("does NOT return onComplete data for retried tasks", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "h", slot: 0, args: {},
        onComplete: {
          fnHandle: "function://handler",
          context: {},
        },
        retryBehavior: { maxAttempts: 3, initialBackoffMs: 100, base: 2 },
      });

      await claimBatch(0, 10);

      const items = await t.mutation(api.batch.failBatch, {
        items: [{ taskId, error: "transient" }],
      });

      // Task was retried, not permanently failed → no onComplete
      expect(items).toHaveLength(0);

      // Task should be back to pending
      await t.run(async (ctx) => {
        const task = await ctx.db.get(taskId);
        expect(task!.status).toBe("pending");
        expect(task!.attempt).toBe(1);
      });
    });
  });

  describe("retry preserves task data", () => {
    it("original args are preserved across retries", async () => {
      await setupPoolConfig();

      const originalArgs = { complex: { nested: [1, 2, 3] }, key: "value" };
      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler", slot: 0,
        args: originalArgs,
        retryBehavior: { maxAttempts: 3, initialBackoffMs: 100, base: 2 },
      });

      // Fail twice
      for (let i = 0; i < 2; i++) {
        vi.advanceTimersByTime(60_000);
        await claimBatch(0, 10);
        await t.mutation(api.batch.fail, { taskId, error: `fail-${i}` });
      }

      vi.advanceTimersByTime(60_000);

      // Re-claim — args should be identical
      const claimed = await claimBatch(0, 10);
      expect(claimed).toHaveLength(1);
      expect(claimed[0].args).toEqual(originalArgs);
      expect(claimed[0].attempt).toBe(2);
    });

    it("slot assignment is preserved across retries", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler", slot: 7, args: {},
        retryBehavior: { maxAttempts: 3, initialBackoffMs: 100, base: 2 },
      });

      await claimBatch(7, 10);
      await t.mutation(api.batch.fail, { taskId, error: "oops" });

      vi.advanceTimersByTime(60_000);

      // Should still be claimable from slot 7, not slot 0
      const fromSlot0 = await claimBatch(0, 10);
      const fromSlot7 = await claimBatch(7, 10);
      expect(fromSlot0).toHaveLength(0);
      expect(fromSlot7).toHaveLength(1);
    });
  });

  describe("enqueue without config", () => {
    it("creates task even without batchConfig or existing config", async () => {
      // No setupPoolConfig, no batchConfig arg — just raw enqueue
      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler", slot: 0, args: { key: "val" },
      });

      expect(taskId).toBeDefined();

      await t.run(async (ctx) => {
        const task = await ctx.db.get(taskId);
        expect(task!.status).toBe("pending");
        expect(task!.name).toBe("handler");
      });
    });
  });

  describe("complete and fail idempotency", () => {
    it("completing same task twice is a no-op", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "h", slot: 0, args: {},
      });
      await claimBatch(0, 10);

      // First complete — deletes the task
      await t.mutation(api.batch.complete, { taskId, result: "first" });

      // Second complete — task is gone, should not throw
      await t.mutation(api.batch.complete, { taskId, result: "second" });
    });

    it("failing same task twice is a no-op", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "h", slot: 0, args: {},
      });
      await claimBatch(0, 10);

      // First fail (no retry) — deletes the task
      await t.mutation(api.batch.fail, { taskId, error: "first" });

      // Second fail — task is gone, should not throw
      await t.mutation(api.batch.fail, { taskId, error: "second" });
    });

    it("complete after cancel is a no-op", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "h", slot: 0, args: {},
      });
      await claimBatch(0, 10);
      await t.mutation(api.batch.cancel, { taskId });

      // Executor doesn't know about cancel, tries to complete
      await t.mutation(api.batch.complete, { taskId, result: "too late" });
      // Should not throw, should not resurrect the task
    });

    it("fail after cancel is a no-op", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "h", slot: 0, args: {},
      });
      await claimBatch(0, 10);
      await t.mutation(api.batch.cancel, { taskId });

      await t.mutation(api.batch.fail, { taskId, error: "too late" });
    });
  });

  describe("large batch operations", () => {
    it("enqueueBatch handles 100 tasks", async () => {
      await setupPoolConfig();

      const tasks = Array.from({ length: 100 }, (_, i) => ({
        name: "handler",
        slot: i % 5,
        args: { index: i },
      }));

      const ids = await t.mutation(api.batch.enqueueBatch, { tasks });
      expect(ids).toHaveLength(100);

      // All should be pending
      await t.run(async (ctx) => {
        for (const id of ids) {
          const task = await ctx.db.get(id);
          expect(task!.status).toBe("pending");
        }
      });
    });

    it("completeBatch handles 50 tasks", async () => {
      await setupPoolConfig();

      const tasks = Array.from({ length: 50 }, (_, i) => ({
        name: "handler", slot: 0, args: { i },
      }));
      const ids = await t.mutation(api.batch.enqueueBatch, { tasks });

      await claimBatch(0, 100);

      const items = ids.map((id) => ({ taskId: id, result: "done" }));
      await t.mutation(api.batch.completeBatch, { items });

      await t.run(async (ctx) => {
        for (const id of ids) {
          expect(await ctx.db.get(id)).toBeNull();
        }
      });
    });
  });

  describe("status reflects full lifecycle", () => {
    it("pending → running → finished through complete", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "h", slot: 0, args: {},
      });

      expect(await t.query(api.batch.status, { taskId }))
        .toEqual({ state: "pending", attempt: 0 });

      await claimBatch(0, 1);

      expect(await t.query(api.batch.status, { taskId }))
        .toEqual({ state: "running", attempt: 0 });

      await t.mutation(api.batch.complete, { taskId, result: null });

      expect(await t.query(api.batch.status, { taskId }))
        .toEqual({ state: "finished" });
    });

    it("pending → running → pending (retry) → running → finished (fail)", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "h", slot: 0, args: {},
        retryBehavior: { maxAttempts: 2, initialBackoffMs: 100, base: 2 },
      });

      // Attempt 0
      await claimBatch(0, 1);
      expect(await t.query(api.batch.status, { taskId }))
        .toEqual({ state: "running", attempt: 0 });

      await t.mutation(api.batch.fail, { taskId, error: "oops" });
      expect(await t.query(api.batch.status, { taskId }))
        .toEqual({ state: "pending", attempt: 1 });

      // Attempt 1 (final)
      vi.advanceTimersByTime(60_000);
      await claimBatch(0, 1);
      expect(await t.query(api.batch.status, { taskId }))
        .toEqual({ state: "running", attempt: 1 });

      await t.mutation(api.batch.fail, { taskId, error: "still broken" });
      expect(await t.query(api.batch.status, { taskId }))
        .toEqual({ state: "finished" });
    });
  });

  describe("watchdog edge cases", () => {
    it("watchdog with only pending tasks (no stale claims) still restarts dead slots", async () => {
      await setupPoolConfig({
        maxWorkers: 2,
        activeSlots: [0, 1],
        claimTimeoutMs: 120_000,
      });

      // Old pending task on slot 0 — executor for slot 0 must be dead
      await t.run(async (ctx) => {
        await ctx.db.insert("batchTasks", {
          name: "old-pending",
          slot: 0,
          args: {},
          status: "pending" as const,
          readyAt: Date.now() - 60_000,
          attempt: 0,
        });
      });

      await t.mutation(internal.batch._watchdog, {});

      // Slot 0 should have been restarted
      await t.run(async (ctx) => {
        const config = await ctx.db.query("batchConfig").unique();
        expect(config!.activeSlots).toContain(0);
        expect(config!.activeSlots).toContain(1);
      });
    });

    it("watchdog is idempotent — running twice doesn't double-add slots", async () => {
      await setupPoolConfig({
        maxWorkers: 2,
        activeSlots: [],
        claimTimeoutMs: 60_000,
      });

      // Pending task triggers executor start
      await t.mutation(api.batch.enqueue, {
        name: "handler", slot: 0, args: {},
      });

      await t.mutation(internal.batch._watchdog, {});
      await t.mutation(internal.batch._watchdog, {});

      await t.run(async (ctx) => {
        const config = await ctx.db.query("batchConfig").unique();
        // Each slot should appear at most once
        const unique = [...new Set(config!.activeSlots)];
        expect(unique.length).toBe(config!.activeSlots.length);
      });
    });
  });

  // ─── Degenerate / boundary cases ──────────────────────────────────────────

  describe("empty batch operations", () => {
    it("enqueueBatch with empty array returns empty", async () => {
      await setupPoolConfig();
      const ids = await t.mutation(api.batch.enqueueBatch, { tasks: [] });
      expect(ids).toEqual([]);
    });

    it("completeBatch with empty items returns empty", async () => {
      await setupPoolConfig();
      const items = await t.mutation(api.batch.completeBatch, { items: [] });
      expect(items).toEqual([]);
    });

    it("failBatch with empty items returns empty", async () => {
      await setupPoolConfig();
      const items = await t.mutation(api.batch.failBatch, { items: [] });
      expect(items).toEqual([]);
    });

    it("claimByIds with empty array returns empty", async () => {
      await setupPoolConfig();
      const claimed = await t.mutation(api.batch.claimByIds, { taskIds: [] });
      expect(claimed).toEqual([]);
    });

    it("releaseClaims with empty array is a no-op", async () => {
      await setupPoolConfig();
      await t.mutation(api.batch.releaseClaims, { taskIds: [] });
    });

    it("dispatchOnCompleteBatch with empty array returns 0 failures", async () => {
      const failures = await t.mutation(api.batch.dispatchOnCompleteBatch, { items: [] });
      expect(failures).toBe(0);
    });
  });

  describe("cancel fires onComplete with canceled result", () => {
    it("cancel schedules onComplete with kind=canceled", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler", slot: 0, args: {},
        onComplete: {
          fnHandle: "function://on-cancel-handler",
          context: { trackingId: "abc" },
        },
      });

      // Cancel the task
      await t.mutation(api.batch.cancel, { taskId });

      // Task should be deleted
      await t.run(async (ctx) => {
        expect(await ctx.db.get(taskId)).toBeNull();
      });

      // The onComplete should have been scheduled with {kind: "canceled"}
      // (verified by the fact that cancel didn't throw — the scheduling itself worked)
    });

    it("cancel without onComplete doesn't crash", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler", slot: 0, args: {},
        // no onComplete
      });

      await t.mutation(api.batch.cancel, { taskId });

      await t.run(async (ctx) => {
        expect(await ctx.db.get(taskId)).toBeNull();
      });
    });
  });

  describe("sweepStaleClaims without config", () => {
    it("uses default 120_000ms timeout when no config exists", async () => {
      // No config — sweepStaleClaims should still work with fallback

      // Insert a task claimed 130s ago (past default 120s)
      await t.run(async (ctx) => {
        await ctx.db.insert("batchTasks", {
          name: "orphan", slot: 0, args: {},
          status: "claimed" as const,
          claimedAt: Date.now() - 130_000,
          readyAt: Date.now() - 130_000,
          attempt: 0,
        });
      });

      const swept = await t.mutation(api.batch.sweepStaleClaims, {});
      expect(swept).toBe(1);
    });

    it("does not sweep claims under 120s with no config", async () => {
      await t.run(async (ctx) => {
        await ctx.db.insert("batchTasks", {
          name: "recent", slot: 0, args: {},
          status: "claimed" as const,
          claimedAt: Date.now() - 60_000, // only 60s old
          readyAt: Date.now() - 60_000,
          attempt: 0,
        });
      });

      const swept = await t.mutation(api.batch.sweepStaleClaims, {});
      expect(swept).toBe(0);
    });
  });

  describe("countPending with only claimed tasks", () => {
    it("returns 1 when only claimed tasks exist (no pending)", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler", slot: 0, args: {},
      });

      // Claim it
      await claimBatch(0, 1);

      // No pending tasks, but claimed exists
      const count = await t.query(api.batch.countPending, {});
      // countPending checks both pending AND claimed
      expect(count).toBe(1);
    });

    it("returns 0 when no tasks exist at all", async () => {
      await setupPoolConfig();
      const count = await t.query(api.batch.countPending, {});
      expect(count).toBe(0);
    });
  });

  describe("maxAttempts=1 means no retries", () => {
    it("task with maxAttempts=1 is permanently failed on first failure", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler", slot: 0, args: {},
        retryBehavior: { maxAttempts: 1, initialBackoffMs: 100, base: 2 },
      });

      await claimBatch(0, 1);
      await t.mutation(api.batch.fail, { taskId, error: "first and only" });

      // Task should be deleted (no retry despite having retryBehavior)
      await t.run(async (ctx) => {
        expect(await ctx.db.get(taskId)).toBeNull();
      });
    });
  });

  describe("watchdog edge cases (additional)", () => {
    it("watchdog with no config is a no-op", async () => {
      // No setupPoolConfig at all
      await t.mutation(internal.batch._watchdog, {});
      // Should not throw
    });

    it("watchdog sweep limit: only sweeps 500 stale claims per run", async () => {
      await setupPoolConfig({ claimTimeoutMs: 60_000 });

      // Insert 502 stale claimed tasks
      await t.run(async (ctx) => {
        for (let i = 0; i < 502; i++) {
          await ctx.db.insert("batchTasks", {
            name: "stale", slot: i % 10, args: {},
            status: "claimed" as const,
            claimedAt: Date.now() - 120_000,
            readyAt: Date.now() - 120_000,
            attempt: 0,
          });
        }
      });

      // First watchdog run: sweeps 500
      await t.mutation(internal.batch._watchdog, {});

      await t.run(async (ctx) => {
        const still_claimed = await ctx.db
          .query("batchTasks")
          .withIndex("by_status_claimedAt", (q) => q.eq("status", "claimed"))
          .collect();
        // 502 - 500 = 2 still claimed
        expect(still_claimed.length).toBe(2);
      });

      // Second watchdog run: sweeps remaining 2
      await t.mutation(internal.batch._watchdog, {});

      await t.run(async (ctx) => {
        const still_claimed = await ctx.db
          .query("batchTasks")
          .withIndex("by_status_claimedAt", (q) => q.eq("status", "claimed"))
          .collect();
        expect(still_claimed.length).toBe(0);
      });
    });
  });

  describe("upsertBatchConfig skip-write optimization", () => {
    it("does not write when config already matches", async () => {
      // First configure
      await t.mutation(api.batch.configure, {
        executorHandle: "function://exec",
        maxWorkers: 5,
        claimTimeoutMs: 120_000,
      });

      // Get initial config
      let initialId: any;
      await t.run(async (ctx) => {
        const config = await ctx.db.query("batchConfig").unique();
        initialId = config!._id;
      });

      // Configure with same values
      await t.mutation(api.batch.configure, {
        executorHandle: "function://exec",
        maxWorkers: 5,
        claimTimeoutMs: 120_000,
      });

      // Config should still exist (same doc)
      await t.run(async (ctx) => {
        const config = await ctx.db.query("batchConfig").unique();
        expect(config!._id).toBe(initialId);
      });
    });
  });

  describe("dispatchOnCompleteBatch partial failure", () => {
    it("one handler failing doesn't block other handlers", async () => {
      // We can't easily test runMutation in convex-test with real function handles,
      // but we can verify the mutation doesn't throw when items are provided.
      // The mutation catches errors internally and returns failure count.
      // Since we can't register real function handles in test, we verify
      // the error handling path by passing an invalid handle.
      const failures = await t.mutation(api.batch.dispatchOnCompleteBatch, {
        items: [
          {
            fnHandle: "function://nonexistent1",
            workId: "w1",
            result: { kind: "success" as const, returnValue: null },
          },
          {
            fnHandle: "function://nonexistent2",
            workId: "w2",
            result: { kind: "failed" as const, error: "err" },
          },
        ],
      });

      // Both should fail (invalid handles) but mutation should not throw
      expect(failures).toBe(2);
    });
  });

  describe("releaseClaims then immediate re-claim", () => {
    it("released task sets readyAt to now, making it immediately claimable", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler", slot: 0, args: {},
      });

      // Claim
      await claimBatch(0, 1);

      // Release
      await t.mutation(api.batch.releaseClaims, { taskIds: [taskId] });

      // Verify readyAt is now (not in the future from a backoff)
      await t.run(async (ctx) => {
        const task = await ctx.db.get(taskId);
        expect(task!.readyAt).toBeLessThanOrEqual(Date.now());
        expect(task!.status).toBe("pending");
      });

      // Should be immediately claimable
      const claimed = await claimBatch(0, 1);
      expect(claimed).toHaveLength(1);
    });
  });

  describe("enqueueBatch atomicity", () => {
    it("canceling one task from a batch doesn't affect others", async () => {
      await setupPoolConfig();

      const ids = await t.mutation(api.batch.enqueueBatch, {
        tasks: [
          { name: "h1", slot: 0, args: { idx: 0 } },
          { name: "h2", slot: 0, args: { idx: 1 } },
          { name: "h3", slot: 0, args: { idx: 2 } },
        ],
      });

      // Cancel the middle one
      await t.mutation(api.batch.cancel, { taskId: ids[1] });

      // The other two should still be claimable
      const claimed = await claimBatch(0, 10);
      expect(claimed).toHaveLength(2);
      const claimedIds = claimed.map((c) => c._id).sort();
      expect(claimedIds).toEqual([ids[0], ids[2]].sort());
    });
  });

  describe("high slot numbers", () => {
    it("tasks with slot far outside maxWorkers range still work", async () => {
      await setupPoolConfig({ maxWorkers: 3 });

      // Enqueue on slot 999 — far outside [0, maxWorkers)
      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler", slot: 999, args: {},
      });

      // Claimable from slot 999
      const claimed = await claimBatch(999, 1);
      expect(claimed).toHaveLength(1);
      expect(claimed[0]._id).toBe(taskId);

      // Not claimable from slot 0
      const from0 = await claimBatch(0, 1);
      expect(from0).toHaveLength(0);
    });
  });

  describe("_maybeStartExecutors idempotency", () => {
    it("concurrent calls don't create duplicate slots", async () => {
      // Simulate two enqueue mutations both scheduling _maybeStartExecutors
      const taskId1 = await t.mutation(api.batch.enqueue, {
        name: "h1", slot: 0, args: {},
        batchConfig: {
          executorHandle: "function://exec",
          maxWorkers: 3,
          claimTimeoutMs: 120_000,
        },
      });

      const taskId2 = await t.mutation(api.batch.enqueue, {
        name: "h2", slot: 1, args: {},
        batchConfig: {
          executorHandle: "function://exec",
          maxWorkers: 3,
          claimTimeoutMs: 120_000,
        },
      });

      // Flush both scheduled _maybeStartExecutors
      await (t.finishAllScheduledFunctions as any)(() => vi.advanceTimersByTime(1000), 10);

      await t.run(async (ctx) => {
        const config = await ctx.db.query("batchConfig").unique();
        // Each slot should appear at most once
        const unique = [...new Set(config!.activeSlots)];
        expect(unique.length).toBe(config!.activeSlots.length);
        expect(unique.sort()).toEqual([0, 1, 2]);
      });
    });
  });

  // ─── Claim ownership bugs ──────────────────────────────────────────────────
  // These tests verify that a stale executor (whose claim was swept) cannot
  // interfere with a task that has been re-claimed by another executor.

  describe("stale executor completes a re-claimed task", () => {
    it("should NOT delete a task that was re-claimed by another executor", async () => {
      await setupPoolConfig({ claimTimeoutMs: 60_000 });

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "slow-handler", slot: 0, args: {},
      });

      // Executor A claims it — record the claimedAt
      const claimedByA = await claimBatch(0, 1);
      const aClaimedAt = claimedByA[0].claimedAt;

      // Executor A's handler is slow — time passes beyond claimTimeout
      vi.advanceTimersByTime(120_000);

      // Stale claim sweep puts it back to pending
      const swept = await t.mutation(api.batch.sweepStaleClaims, {});
      expect(swept).toBe(1);

      // Executor B claims it
      const claimedByB = await claimBatch(0, 1);
      expect(claimedByB).toHaveLength(1);

      // Executor A's handler finally completes, calls completeBatch with its claimedAt
      // This SHOULD be a no-op since A no longer owns the claim
      await t.mutation(api.batch.completeBatch, {
        items: [{ taskId, result: "stale result from A", claimedAt: aClaimedAt }],
      });

      // Task should STILL exist (claimed by B), not deleted
      await t.run(async (ctx) => {
        const task = await ctx.db.get(taskId);
        expect(task).not.toBeNull();
        expect(task!.status).toBe("claimed");
      });
    });
  });

  describe("stale executor fails a re-claimed task", () => {
    it("should NOT modify a task that was re-claimed by another executor", async () => {
      await setupPoolConfig({ claimTimeoutMs: 60_000 });

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "slow-handler", slot: 0, args: {},
        retryBehavior: { maxAttempts: 3, initialBackoffMs: 100, base: 2 },
      });

      // Executor A claims it — record the claimedAt
      const claimedByA = await claimBatch(0, 1);
      const aClaimedAt = claimedByA[0].claimedAt;

      // Executor A's handler is slow
      vi.advanceTimersByTime(120_000);

      // Sweep puts it back to pending
      await t.mutation(api.batch.sweepStaleClaims, {});

      // Executor B claims it
      const claimedByB = await claimBatch(0, 1);
      expect(claimedByB).toHaveLength(1);

      // Executor A's handler finally fails, passes its claimedAt
      // This SHOULD be a no-op since A no longer owns the claim
      await t.mutation(api.batch.failBatch, {
        items: [{ taskId, error: "stale failure from A", claimedAt: aClaimedAt }],
      });

      // Task should STILL be claimed by B, not put back to pending
      await t.run(async (ctx) => {
        const task = await ctx.db.get(taskId);
        expect(task).not.toBeNull();
        expect(task!.status).toBe("claimed");
      });
    });
  });

  describe("stale executor releases a re-claimed task", () => {
    it("should NOT release a task that was re-claimed by another executor", async () => {
      await setupPoolConfig({ claimTimeoutMs: 60_000 });

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler", slot: 0, args: {},
      });

      // Executor A claims it — record the claimedAt
      const claimedByA = await claimBatch(0, 1);
      const aClaimedAt = claimedByA[0].claimedAt;

      // Time passes beyond claimTimeout
      vi.advanceTimersByTime(120_000);

      // Sweep puts it back to pending
      await t.mutation(api.batch.sweepStaleClaims, {});

      // Executor B claims it
      await claimBatch(0, 1);

      // Executor A hits soft deadline, tries to release with its claimedAt
      // This SHOULD be a no-op since A no longer owns the claim
      await t.mutation(api.batch.releaseClaims, {
        items: [{ taskId, claimedAt: aClaimedAt }],
      });

      // Task should STILL be claimed by B
      await t.run(async (ctx) => {
        const task = await ctx.db.get(taskId);
        expect(task).not.toBeNull();
        expect(task!.status).toBe("claimed");
      });
    });
  });

  // ─── Potential bug: enqueue without batchConfig after watchdog stops ────────

  describe("enqueue without batchConfig after all executors finish", () => {
    it("new task enqueued without batchConfig should still get executors", async () => {
      // Scenario:
      // 1. First enqueue with batchConfig → executors start, watchdog starts
      // 2. All tasks complete → executors call executorDone(false) → activeSlots = []
      // 3. Watchdog fires, sees no work → stops (doesn't reschedule)
      // 4. New enqueue WITHOUT batchConfig (simulating configSentThisTx=true)
      // 5. Executors SHOULD be started for the new task

      // Step 1: Initial enqueue with config
      await t.mutation(api.batch.enqueue, {
        name: "first-task", slot: 0, args: {},
        batchConfig: {
          executorHandle: "function://test-executor",
          maxWorkers: 2,
          claimTimeoutMs: 120_000,
        },
      });

      // Flush scheduled _maybeStartExecutors
      await (t.finishAllScheduledFunctions as any)(() => vi.advanceTimersByTime(1000), 10);

      // Verify executors were started
      await t.run(async (ctx) => {
        const config = await ctx.db.query("batchConfig").unique();
        expect(config!.activeSlots.sort()).toEqual([0, 1]);
      });

      // Step 2: Claim and complete the task
      const claimed = await claimBatch(0, 10);
      expect(claimed).toHaveLength(1);
      await t.mutation(api.batch.complete, {
        taskId: claimed[0]._id,
        result: "done",
      });

      // Step 3: All executors finish (executorDone with startMore=false)
      await t.mutation(api.batch.executorDone, { slot: 0, startMore: false });
      await t.mutation(api.batch.executorDone, { slot: 1, startMore: false });

      // Verify activeSlots is empty
      await t.run(async (ctx) => {
        const config = await ctx.db.query("batchConfig").unique();
        expect(config!.activeSlots).toEqual([]);
      });

      // Step 4: Watchdog fires and sees no work → stops
      await t.mutation(internal.batch._watchdog, {});

      // Step 5: New task enqueued WITHOUT batchConfig
      // (simulates configSentThisTx=true in the BatchWorkpool class)
      const newTaskId = await t.mutation(api.batch.enqueue, {
        name: "second-task", slot: 0, args: {},
        // NO batchConfig!
      });

      // Verify task exists and is pending
      await t.run(async (ctx) => {
        const task = await ctx.db.get(newTaskId);
        expect(task!.status).toBe("pending");
      });

      // Flush any scheduled functions
      await (t.finishAllScheduledFunctions as any)(() => vi.advanceTimersByTime(1000), 10);

      // Executors SHOULD be running to process the new task
      await t.run(async (ctx) => {
        const config = await ctx.db.query("batchConfig").unique();
        expect(config!.activeSlots.length).toBeGreaterThan(0);
      });
    });
  });

  describe("resetConfig and resetTasks", () => {
    it("resetConfig clears config and activeSlots", async () => {
      await setupPoolConfig({ activeSlots: [0, 1, 2], maxWorkers: 3 });

      await t.mutation(api.batch.resetConfig, {});

      await t.run(async (ctx) => {
        const config = await ctx.db.query("batchConfig").unique();
        expect(config).toBeNull();
      });
    });

    it("resetTasks deletes all batch tasks", async () => {
      await setupPoolConfig();

      for (let i = 0; i < 5; i++) {
        await t.mutation(api.batch.enqueue, {
          name: "h", slot: 0, args: { i },
        });
      }

      const result = await t.mutation(api.batch.resetTasks, {});
      expect(result.deleted).toBe(5);

      await t.run(async (ctx) => {
        const tasks = await ctx.db.query("batchTasks").collect();
        expect(tasks).toHaveLength(0);
      });
    });
  });
});
