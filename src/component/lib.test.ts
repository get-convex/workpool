import { convexTest } from "convex-test";
import {
  afterEach,
  assert,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";
import { api } from "./_generated/api.js";
import type { Id } from "./_generated/dataModel.js";
import schema from "./schema.js";

const modules = import.meta.glob("./**/*.ts");

// Mock Id type
type WorkId = Id<"work">;

describe("lib", () => {
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

  describe("enqueue", () => {
    it("should successfully enqueue a work item", async () => {
      const id = await t.mutation(api.lib.enqueue, {
        fnHandle: "testHandle",
        fnName: "testFunction",
        fnArgs: { test: true },
        fnType: "mutation",
        runAt: Date.now(),
        config: {
          maxParallelism: 10,
          logLevel: "WARN",
        },
      });

      expect(id).toBeDefined();
      const status = await t.query(api.lib.status, { id });
      expect(status).toEqual({ state: "pending", previousAttempts: 0 });
    });

    it("should throw error if maxParallelism is too high", async () => {
      await expect(
        t.mutation(api.lib.enqueue, {
          fnHandle: "testHandle",
          fnName: "testFunction",
          fnArgs: { test: true },
          fnType: "mutation",
          runAt: Date.now(),
          config: {
            maxParallelism: 101, // More than MAX_POSSIBLE_PARALLELISM
            logLevel: "WARN",
          },
        })
      ).rejects.toThrow("maxParallelism must be <= 50");
    });

    it("should throw error if maxParallelism is too low", async () => {
      await expect(
        t.mutation(api.lib.enqueue, {
          fnHandle: "testHandle",
          fnName: "testFunction",
          fnArgs: { test: true },
          fnType: "mutation",
          runAt: Date.now(),
          config: {
            maxParallelism: 0, // Less than minimum
            logLevel: "WARN",
          },
        })
      ).rejects.toThrow("maxParallelism must be >= 1");
    });
  });

  describe("cancel", () => {
    it("should successfully queue a work item for cancelation", async () => {
      const id = await t.mutation(api.lib.enqueue, {
        fnHandle: "testHandle",
        fnName: "testFunction",
        fnArgs: { test: true },
        fnType: "mutation",
        runAt: Date.now(),
        config: {
          maxParallelism: 10,
          logLevel: "WARN",
        },
      });

      await t.mutation(api.lib.cancel, {
        id,
        logLevel: "WARN",
      });

      // Verify a pending cancelation was created
      await t.run(async (ctx) => {
        const pendingCancelations = await ctx.db
          .query("pendingCancelation")
          .collect();
        expect(pendingCancelations).toHaveLength(1);
        expect(pendingCancelations[0].workId).toBe(id);
      });
    });

    it("should not create duplicate cancelation requests", async () => {
      const id = await t.mutation(api.lib.enqueue, {
        fnHandle: "testHandle",
        fnName: "testFunction",
        fnArgs: { test: true },
        fnType: "mutation",
        runAt: Date.now(),
        config: {
          maxParallelism: 10,
          logLevel: "WARN",
        },
      });

      // Cancel the first time
      await t.mutation(api.lib.cancel, {
        id,
        logLevel: "WARN",
      });

      // Cancel the second time
      await t.mutation(api.lib.cancel, {
        id,
        logLevel: "WARN",
      });

      // Verify only one pending cancelation was created
      await t.run(async (ctx) => {
        const pendingCancelations = await ctx.db
          .query("pendingCancelation")
          .collect();
        expect(pendingCancelations).toHaveLength(1);
        expect(pendingCancelations[0].workId).toBe(id);
      });
    });

    it("should not create cancelation for non-existent work", async () => {
      const id = await t.mutation(api.lib.enqueue, {
        fnHandle: "testHandle",
        fnName: "testFunction",
        fnArgs: { test: true },
        fnType: "mutation",
        runAt: Date.now(),
        config: {
          maxParallelism: 10,
          logLevel: "WARN",
        },
      });

      // Delete the work item
      await t.run(async (ctx) => {
        await ctx.db.delete(id);
      });

      // Try to cancel the deleted work
      await t.mutation(api.lib.cancel, {
        id,
        logLevel: "WARN",
      });

      // Verify no pending cancelation was created
      await t.run(async (ctx) => {
        const pendingCancelations = await ctx.db
          .query("pendingCancelation")
          .collect();
        expect(pendingCancelations).toHaveLength(0);
      });
    });
  });

  describe("cancelAll", () => {
    it("should queue multiple work items for cancelation", async () => {
      const ids: WorkId[] = [];
      for (let i = 0; i < 3; i++) {
        const id = await t.mutation(api.lib.enqueue, {
          fnHandle: "testHandle",
          fnName: "testFunction",
          fnArgs: { test: i },
          fnType: "mutation",
          runAt: Date.now() + 5 * 60 * 1000,
          config: {
            maxParallelism: 10,
            logLevel: "WARN",
          },
        });
        ids.push(id);
      }

      await t.mutation(api.lib.cancelAll, {
        logLevel: "WARN",
        before: Date.now() + 1000,
      });

      // Verify pending cancelations were created
      await t.run(async (ctx) => {
        const pendingCancelations = await ctx.db
          .query("pendingCancelation")
          .collect();
        expect(pendingCancelations).toHaveLength(3);
        const canceledIds = pendingCancelations.map((pc) => pc.workId);
        expect(canceledIds).toEqual(expect.arrayContaining(ids));
      });
    });

    it("should process work items in batches for cancelAll", async () => {
      const PAGE_SIZE = 64; // Same as in lib.ts

      // Create PAGE_SIZE + 1 work items to trigger pagination
      for (let i = 0; i < PAGE_SIZE + 1; i++) {
        await t.mutation(api.lib.enqueue, {
          fnHandle: "testHandle",
          fnName: "testFunction",
          fnArgs: { test: i },
          fnType: "mutation",
          runAt: Date.now(),
          config: {
            maxParallelism: 10,
            logLevel: "WARN",
          },
        });
      }

      await t.mutation(api.lib.cancelAll, {
        logLevel: "WARN",
        before: Date.now() + 1000,
      });

      // assert that cancelAll was scheduled
      await t.run(async (ctx) => {
        const scheduledFunctions = await ctx.db.system
          .query("_scheduled_functions")
          .collect();
        expect(scheduledFunctions.length).toBeGreaterThan(0);
        // check that one of the scheduled functions is cancelAll
        const cancelAllScheduledFunction = scheduledFunctions.find(
          (sf) => sf.name === "lib:cancelAll"
        );
        expect(cancelAllScheduledFunction).toBeDefined();
        assert(cancelAllScheduledFunction);
      });

      // Verify the first page of cancelations was created
      await t.run(async (ctx) => {
        const pendingCancelations = await ctx.db
          .query("pendingCancelation")
          .collect();

        // We should have at least PAGE_SIZE cancelations
        expect(pendingCancelations.length).toEqual(PAGE_SIZE);
      });
    });
  });

  describe("status", () => {
    it("should return finished state for non-existent work", async () => {
      const id = await t.mutation(api.lib.enqueue, {
        fnHandle: "testHandle",
        fnName: "testFunction",
        fnArgs: { test: true },
        fnType: "mutation",
        runAt: Date.now(),
        config: {
          maxParallelism: 10,
          logLevel: "WARN",
        },
      });
      await t.run(async (ctx) => {
        await ctx.db.delete(id);
      });

      const status = await t.query(api.lib.status, { id });
      expect(status).toEqual({ state: "finished" });
    });

    it("should return pending state for newly enqueued work", async () => {
      const id = await t.mutation(api.lib.enqueue, {
        fnHandle: "testHandle",
        fnName: "testFunction",
        fnArgs: { test: true },
        fnType: "mutation",
        runAt: Date.now(),
        config: {
          maxParallelism: 10,
          logLevel: "WARN",
        },
      });

      // Verify work item and pending start were created
      await t.run(async (ctx) => {
        const work = await ctx.db.get(id);
        expect(work).toBeDefined();
        const pendingStarts = await ctx.db.query("pendingStart").collect();
        expect(pendingStarts).toHaveLength(1);
        expect(pendingStarts[0].workId).toBe(id);
      });

      const status = await t.query(api.lib.status, { id });
      expect(status).toEqual({ state: "pending", previousAttempts: 0 });
    });

    it("should return running state when work is in progress", async () => {
      const id = await t.mutation(api.lib.enqueue, {
        fnHandle: "testHandle",
        fnName: "testFunction",
        fnArgs: { test: true },
        fnType: "mutation",
        runAt: Date.now(),
        config: {
          maxParallelism: 10,
          logLevel: "WARN",
        },
      });

      // Delete the pendingStart to simulate work in progress
      await t.run(async (ctx) => {
        const pendingStart = await ctx.db.query("pendingStart").first();
        expect(pendingStart).toBeDefined();
        assert(pendingStart);
        await ctx.db.delete(pendingStart._id);
      });

      const status = await t.query(api.lib.status, { id });
      expect(status).toEqual({ state: "running", previousAttempts: 0 });
    });

    it("should return pending state for work pending retry", async () => {
      const id = await t.mutation(api.lib.enqueue, {
        fnHandle: "testHandle",
        fnName: "testFunction",
        fnArgs: { test: true },
        fnType: "mutation",
        runAt: Date.now(),
        retryBehavior: {
          maxAttempts: 3,
          initialBackoffMs: 100,
          base: 2,
        },
        config: {
          maxParallelism: 10,
          logLevel: "WARN",
        },
      });

      // Delete the pendingStart to simulate work in progress
      await t.run(async (ctx) => {
        const pendingStart = await ctx.db.query("pendingStart").first();
        expect(pendingStart).toBeDefined();
        assert(pendingStart);
        await ctx.db.delete(pendingStart._id);

        // Create a pendingCompletion with retry=true to simulate a failed job that will be retried
        await ctx.db.insert("pendingCompletion", {
          workId: id,
          segment: 1n, // Using a simple segment value for testing
          runResult: { kind: "failed", error: "Test error" },
          retry: true,
        });
      });

      const status = await t.query(api.lib.status, { id });
      expect(status).toEqual({ state: "pending", previousAttempts: 0 });
    });

    it("should return running state for work with pendingCancelation", async () => {
      const id = await t.mutation(api.lib.enqueue, {
        fnHandle: "testHandle",
        fnName: "testFunction",
        fnArgs: { test: true },
        fnType: "mutation",
        runAt: Date.now(),
        config: {
          maxParallelism: 10,
          logLevel: "WARN",
        },
      });

      // Delete the pendingStart and add pendingCancelation to simulate cancellation in progress
      await t.run(async (ctx) => {
        const pendingStart = await ctx.db.query("pendingStart").first();
        expect(pendingStart).toBeDefined();
        assert(pendingStart);
        await ctx.db.delete(pendingStart._id);

        // Create a pendingCancelation
        await ctx.db.insert("pendingCancelation", {
          workId: id,
          segment: 1n, // Using a simple segment value for testing
        });
      });

      // According to the implementation, a job with pendingCancelation but no pendingStart
      // or pendingCompletion with retry=true is considered "running"
      const status = await t.query(api.lib.status, { id });
      expect(status).toEqual({ state: "running", previousAttempts: 0 });
    });

    it("should return running state for work with pendingCompletion but retry=false", async () => {
      const id = await t.mutation(api.lib.enqueue, {
        fnHandle: "testHandle",
        fnName: "testFunction",
        fnArgs: { test: true },
        fnType: "mutation",
        runAt: Date.now(),
        config: {
          maxParallelism: 10,
          logLevel: "WARN",
        },
      });

      // Delete the pendingStart and add pendingCompletion with retry=false
      await t.run(async (ctx) => {
        const pendingStart = await ctx.db.query("pendingStart").first();
        expect(pendingStart).toBeDefined();
        assert(pendingStart);
        await ctx.db.delete(pendingStart._id);

        // Create a pendingCompletion with retry=false
        await ctx.db.insert("pendingCompletion", {
          workId: id,
          segment: 1n, // Using a simple segment value for testing
          runResult: { kind: "failed", error: "Test error" },
          retry: false,
        });
      });

      // According to the implementation, a job with pendingCompletion but retry=false
      // is considered "running"
      const status = await t.query(api.lib.status, { id });
      expect(status).toEqual({ state: "running", previousAttempts: 0 });
    });
  });
});
