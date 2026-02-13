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
      activeExecutors: number;
      claimTimeoutMs: number;
    }>,
  ) {
    await t.run(async (ctx) => {
      await ctx.db.insert("batchConfig", {
        executorHandle: overrides?.executorHandle ?? "function://test-executor",
        maxWorkers: overrides?.maxWorkers ?? 10,
        activeExecutors: overrides?.activeExecutors ?? 0,
        claimTimeoutMs: overrides?.claimTimeoutMs ?? 120_000,
      });
    });
  }

  describe("enqueue", () => {
    it("should create a pending batch task", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "generateBio",
        args: { userId: "user123" },
      });

      expect(taskId).toBeDefined();

      await t.run(async (ctx) => {
        const task = await ctx.db.get(taskId);
        expect(task).not.toBeNull();
        expect(task!.name).toBe("generateBio");
        expect(task!.args).toEqual({ userId: "user123" });
        expect(task!.status).toBe("pending");
        expect(task!.attempt).toBe(0);
        expect(task!.readyAt).toBeLessThanOrEqual(Date.now());
      });
    });

    it("should lazy-init batch config via batchConfig arg", async () => {
      const taskId = await t.mutation(api.batch.enqueue, {
        name: "myHandler",
        args: {},
        batchConfig: {
          executorHandle: "function://my-executor",
          maxWorkers: 5,
          claimTimeoutMs: 60_000,
        },
      });

      expect(taskId).toBeDefined();

      await t.run(async (ctx) => {
        const config = await ctx.db.query("batchConfig").unique();
        expect(config).not.toBeNull();
        expect(config!.executorHandle).toBe("function://my-executor");
        expect(config!.maxWorkers).toBe(5);
        expect(config!.claimTimeoutMs).toBe(60_000);
        // activeExecutors is 1 because enqueue() calls ensureExecutors()
        // which starts an executor when there's pending work
        expect(config!.activeExecutors).toBe(1);
      });
    });

    it("should store onComplete and retryBehavior", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "myHandler",
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
          { name: "handler1", args: { a: 1 } },
          { name: "handler2", args: { b: 2 } },
          { name: "handler3", args: { c: 3 } },
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

      // Enqueue some tasks
      const id1 = await t.mutation(api.batch.enqueue, {
        name: "handler1",
        args: { a: 1 },
      });
      const id2 = await t.mutation(api.batch.enqueue, {
        name: "handler2",
        args: { b: 2 },
      });

      // Claim them
      const claimed = await t.mutation(api.batch.claimBatch, { limit: 10 });
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

      // Enqueue 5 tasks
      for (let i = 0; i < 5; i++) {
        await t.mutation(api.batch.enqueue, {
          name: `handler${i}`,
          args: {},
        });
      }

      // Claim only 2
      const claimed = await t.mutation(api.batch.claimBatch, { limit: 2 });
      expect(claimed).toHaveLength(2);

      // 3 should still be pending
      await t.run(async (ctx) => {
        const pending = await ctx.db
          .query("batchTasks")
          .withIndex("by_status_readyAt", (q) => q.eq("status", "pending"))
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
          args: {},
          status: "pending",
          readyAt: Date.now() + 60_000, // 1 minute in the future
          attempt: 1,
        });
      });

      // Claim — should get nothing
      const claimed = await t.mutation(api.batch.claimBatch, { limit: 10 });
      expect(claimed).toHaveLength(0);
    });

    it("should not claim already-claimed tasks", async () => {
      await setupPoolConfig();

      await t.mutation(api.batch.enqueue, {
        name: "handler",
        args: {},
      });

      // First claim
      const batch1 = await t.mutation(api.batch.claimBatch, { limit: 10 });
      expect(batch1).toHaveLength(1);

      // Second claim — should get nothing
      const batch2 = await t.mutation(api.batch.claimBatch, { limit: 10 });
      expect(batch2).toHaveLength(0);
    });
  });

  describe("complete", () => {
    it("should delete a claimed task on completion", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler",
        args: {},
      });

      // Claim it
      await t.mutation(api.batch.claimBatch, { limit: 1 });

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
        args: {},
        // no retryBehavior
      });

      // Claim and fail
      await t.mutation(api.batch.claimBatch, { limit: 1 });
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
        args: {},
        retryBehavior: {
          maxAttempts: 3,
          initialBackoffMs: 100,
          base: 2,
        },
      });

      // Claim and fail
      await t.mutation(api.batch.claimBatch, { limit: 1 });
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
        args: {},
        retryBehavior: {
          maxAttempts: 2, // 2 total attempts (1 initial + 1 retry)
          initialBackoffMs: 100,
          base: 2,
        },
      });

      // First attempt: claim and fail
      await t.mutation(api.batch.claimBatch, { limit: 1 });
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
      await t.mutation(api.batch.claimBatch, { limit: 1 });
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
    it("should count pending tasks", async () => {
      await setupPoolConfig();

      // Initially 0
      const count0 = await t.query(api.batch.countPending, {});
      expect(count0).toBe(0);

      // Enqueue 3 tasks
      for (let i = 0; i < 3; i++) {
        await t.mutation(api.batch.enqueue, {
          name: `handler${i}`,
          args: {},
        });
      }

      const count3 = await t.query(api.batch.countPending, {});
      expect(count3).toBe(3);

      // Claim 1
      await t.mutation(api.batch.claimBatch, { limit: 1 });

      const count2 = await t.query(api.batch.countPending, {});
      expect(count2).toBe(2);
    });
  });

  describe("status", () => {
    it("should return pending for a newly enqueued task", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler",
        args: {},
      });

      const s = await t.query(api.batch.status, { taskId });
      expect(s).toEqual({ state: "pending", attempt: 0 });
    });

    it("should return running for a claimed task", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler",
        args: {},
      });

      await t.mutation(api.batch.claimBatch, { limit: 1 });

      const s = await t.query(api.batch.status, { taskId });
      expect(s).toEqual({ state: "running", attempt: 0 });
    });

    it("should return finished for a completed task", async () => {
      await setupPoolConfig();

      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler",
        args: {},
      });

      await t.mutation(api.batch.claimBatch, { limit: 1 });
      await t.mutation(api.batch.complete, { taskId, result: null });

      const s = await t.query(api.batch.status, { taskId });
      expect(s).toEqual({ state: "finished" });
    });

    it("should return finished for a non-existent task", async () => {
      // Use a task ID from a task we create and then delete
      await setupPoolConfig();
      const taskId = await t.mutation(api.batch.enqueue, {
        name: "handler",
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
        args: {},
      });

      await t.mutation(api.batch.claimBatch, { limit: 1 });
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
        args: {},
      });

      // Claim it
      await t.mutation(api.batch.claimBatch, { limit: 1 });

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
        args: {},
      });

      await t.mutation(api.batch.claimBatch, { limit: 1 });

      // Don't advance time — claim is fresh
      const swept = await t.mutation(api.batch.sweepStaleClaims, {});
      expect(swept).toBe(0);
    });
  });

  describe("executorDone", () => {
    it("should decrement activeExecutors", async () => {
      await setupPoolConfig({ activeExecutors: 3 });

      await t.mutation(api.batch.executorDone, { startMore: false });

      await t.run(async (ctx) => {
        const config = await ctx.db.query("batchConfig").unique();
        expect(config!.activeExecutors).toBe(2);
      });
    });

    it("should not go below 0", async () => {
      await setupPoolConfig({ activeExecutors: 0 });

      await t.mutation(api.batch.executorDone, { startMore: false });

      await t.run(async (ctx) => {
        const config = await ctx.db.query("batchConfig").unique();
        expect(config!.activeExecutors).toBe(0);
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
        expect(config!.activeExecutors).toBe(0);
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
});
