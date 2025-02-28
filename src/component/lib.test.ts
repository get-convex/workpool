import { convexTest } from "convex-test";
import { describe, expect, it, beforeEach, afterEach, vi } from "vitest";
import { Id } from "./_generated/dataModel";
import schema from "./schema";
import { api } from "./_generated/api";

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
          logLevel: "INFO",
        },
      });

      expect(id).toBeDefined();
      const status = await t.query(api.lib.status, { id });
      expect(status).toEqual({ state: "pending", attempt: 0 });
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
            logLevel: "INFO",
          },
        })
      ).rejects.toThrow("maxParallelism must be <= 100");
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
          logLevel: "INFO",
        },
      });

      await t.mutation(api.lib.cancel, {
        id,
        logLevel: "INFO",
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
          runAt: Date.now(),
          config: {
            maxParallelism: 10,
            logLevel: "INFO",
          },
        });
        ids.push(id);
      }

      await t.mutation(api.lib.cancelAll, {
        logLevel: "INFO",
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
          logLevel: "INFO",
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
          logLevel: "INFO",
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
      expect(status).toEqual({ state: "pending", attempt: 0 });
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
          logLevel: "INFO",
        },
      });

      // Delete the pendingStart to simulate work in progress
      await t.run(async (ctx) => {
        const pendingStart = await ctx.db.query("pendingStart").first();
        expect(pendingStart).toBeDefined();
        if (pendingStart) {
          await ctx.db.delete(pendingStart._id);
        }
      });

      const status = await t.query(api.lib.status, { id });
      expect(status).toEqual({ state: "running", attempt: 0 });
    });
  });
});
