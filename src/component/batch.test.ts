import { convexTest } from "convex-test";
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";
import { api } from "./_generated/api.js";
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
      await t.finishAllScheduledFunctions(vi.runAllTimers);

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

  describe("claimBatch", () => {
    it("should claim pending tasks and set them to claimed", async () => {
      await setupPoolConfig();

      // Enqueue some tasks on slot 0
      const id1 = await t.mutation(api.batch.enqueue, {
        name: "handler1",
        slot: 0,
        args: { a: 1 },
      });
      const id2 = await t.mutation(api.batch.enqueue, {
        name: "handler2",
        slot: 0,
        args: { b: 2 },
      });

      // Claim them
      const claimed = await t.mutation(api.batch.claimBatch, { slot: 0, limit: 10 });
      expect(claimed).toHaveLength(2);
      expect(claimed.map((c: { name: string }) => c.name).sort()).toEqual([
        "handler1",
        "handler2",
      ]);

      // Verify they are now claimed in the DB
      await t.run(async (ctx) => {
        const task1 = await ctx.db.get(id1);
        expect(task1!.status).toBe("claimed");
        expect(task1!.claimedAt).toBeDefined();

        const task2 = await ctx.db.get(id2);
        expect(task2!.status).toBe("claimed");
      });
    });

    it("should respect the limit", async () => {
      await setupPoolConfig();

      // Enqueue 5 tasks on slot 0
      for (let i = 0; i < 5; i++) {
        await t.mutation(api.batch.enqueue, {
          name: `handler${i}`,
          slot: 0,
          args: {},
        });
      }

      // Claim only 2
      const claimed = await t.mutation(api.batch.claimBatch, { slot: 0, limit: 2 });
      expect(claimed).toHaveLength(2);

      // 3 should still be pending
      await t.run(async (ctx) => {
        const pending = await ctx.db
          .query("batchTasks")
          .withIndex("by_slot_status_readyAt", (q) =>
            q.eq("slot", 0).eq("status", "pending"),
          )
          .collect();
        expect(pending).toHaveLength(3);
      });
    });

    it("should not claim tasks with future readyAt", async () => {
      await setupPoolConfig();

      // Enqueue a task with readyAt in the future (simulate retry backoff)
      await t.run(async (ctx) => {
        await ctx.db.insert("batchTasks", {
          name: "futureTask",
          slot: 0,
          args: {},
          status: "pending",
          readyAt: Date.now() + 60_000, // 1 minute in the future
          attempt: 1,
        });
      });

      // Claim — should get nothing
      const claimed = await t.mutation(api.batch.claimBatch, { slot: 0, limit: 10 });
      expect(claimed).toHaveLength(0);
    });

    it("should not claim already-claimed tasks", async () => {
      await setupPoolConfig();

      await t.mutation(api.batch.enqueue, {
        name: "handler",
        slot: 0,
        args: {},
      });

      // First claim
      const batch1 = await t.mutation(api.batch.claimBatch, { slot: 0, limit: 10 });
      expect(batch1).toHaveLength(1);

      // Second claim — should get nothing
      const batch2 = await t.mutation(api.batch.claimBatch, { slot: 0, limit: 10 });
      expect(batch2).toHaveLength(0);
    });

    it("should only claim tasks matching the given slot", async () => {
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
      await t.mutation(api.batch.enqueue, {
        name: "slot2-task",
        slot: 2,
        args: {},
      });

      // Claim from slot 1 — should only get the slot-1 task
      const claimed = await t.mutation(api.batch.claimBatch, { slot: 1, limit: 10 });
      expect(claimed).toHaveLength(1);
      expect(claimed[0].name).toBe("slot1-task");

      // Slot 0 and slot 2 tasks should still be pending
      await t.run(async (ctx) => {
        const pending = await ctx.db
          .query("batchTasks")
          .filter((q) => q.eq(q.field("status"), "pending"))
          .collect();
        expect(pending).toHaveLength(2);
        expect(pending.map((t) => t.name).sort()).toEqual([
          "slot0-task",
          "slot2-task",
        ]);
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
      await t.mutation(api.batch.claimBatch, { slot: 0, limit: 1 });

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
      await t.mutation(api.batch.claimBatch, { slot: 0, limit: 1 });
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
      await t.mutation(api.batch.claimBatch, { slot: 0, limit: 1 });
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
      await t.mutation(api.batch.claimBatch, { slot: 0, limit: 1 });
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
      await t.mutation(api.batch.claimBatch, { slot: 0, limit: 1 });
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
      await t.mutation(api.batch.claimBatch, { slot: 0, limit: 10 });

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
      await t.mutation(api.batch.claimBatch, { slot: 0, limit: 10 });

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

      await t.mutation(api.batch.claimBatch, { slot: 0, limit: 1 });

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

      await t.mutation(api.batch.claimBatch, { slot: 0, limit: 1 });
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

      await t.mutation(api.batch.claimBatch, { slot: 0, limit: 1 });
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
      await t.mutation(api.batch.claimBatch, { slot: 0, limit: 1 });

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

      await t.mutation(api.batch.claimBatch, { slot: 0, limit: 1 });

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

    it("should schedule replacement for same slot when startMore is true", async () => {
      await setupPoolConfig({ activeSlots: [0, 2] });

      await t.mutation(api.batch.executorDone, { startMore: true, slot: 2 });

      await t.run(async (ctx) => {
        const config = await ctx.db.query("batchConfig").unique();
        // Slot 2 was removed then re-added (replacement scheduled)
        expect(config!.activeSlots.sort()).toEqual([0, 2]);
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
      await t.mutation(api.batch.claimBatch, { slot: 0, limit: 10 });

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
      await t.mutation(api.batch.claimBatch, { slot: 0, limit: 1 });
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
      await t.finishAllScheduledFunctions(vi.runAllTimers);

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
      await t.finishAllScheduledFunctions(vi.runAllTimers);

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
      await t.finishAllScheduledFunctions(vi.runAllTimers);

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
      await t.mutation(api.batch.claimBatch, { slot: 0, limit: 1 });
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
      await t.mutation(api.batch.claimBatch, { slot: 0, limit: 1 });
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
      await t.mutation(api.batch.claimBatch, { slot: 0, limit: 1 });
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
      await t.mutation(api.batch.claimBatch, { slot: 0, limit: 1 });
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
      await t.mutation(api.batch.claimBatch, { slot: 0, limit: 1 });
      await t.mutation(api.batch.fail, { taskId, error: "fail-0" });

      vi.advanceTimersByTime(60_000);

      // Attempt 1 (final)
      await t.mutation(api.batch.claimBatch, { slot: 0, limit: 1 });
      await t.mutation(api.batch.fail, { taskId, error: "fail-1" });

      // Task deleted — no more retries
      await t.run(async (ctx) => {
        const task = await ctx.db.get(taskId);
        expect(task).toBeNull();
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
      await t.mutation(api.batch.claimBatch, { slot: 0, limit: 10 });

      // Advance past claim timeout
      vi.advanceTimersByTime(120_000);

      // Enqueue a third task (claimed recently)
      const recentId = await t.mutation(api.batch.enqueue, {
        name: "recent",
        slot: 0,
        args: {},
      });
      await t.mutation(api.batch.claimBatch, { slot: 0, limit: 10 });

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
      await t.mutation(api.batch.claimBatch, { slot: 0, limit: 1 });

      // Executor A hits deadline, releases it
      await t.mutation(api.batch.releaseClaims, { taskIds: [taskId] });

      // Executor B claims it (same slot — after sweep or replacement)
      const claimed = await t.mutation(api.batch.claimBatch, { slot: 0, limit: 1 });
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
      await t.mutation(api.batch.claimBatch, { slot: 0, limit: 1 });

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

    it("executorDone with startMore starts replacement when work remains", async () => {
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
        // Slot 0 was removed then re-added (replacement scheduled)
        expect(config!.activeSlots).toEqual([0]);
      });
    });
  });
});
