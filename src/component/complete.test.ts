import { convexTest } from "convex-test";
import { describe, expect, it, beforeEach, afterEach, vi } from "vitest";
import schema from "./schema";
import { api } from "./_generated/api";
import { completeHandler } from "./complete";

const modules = import.meta.glob("./**/*.ts");

describe("complete", () => {
  async function setupTest() {
    const t = convexTest(schema, modules);
    return t;
  }

  let t: Awaited<ReturnType<typeof setupTest>>;

  beforeEach(async () => {
    vi.useFakeTimers();
    t = await setupTest();

    // Set up globals for logging
    await t.run(async (ctx) => {
      await ctx.db.insert("globals", {
        maxParallelism: 10,
        logLevel: "INFO",
      });
    });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe("completeHandler", () => {
    it("should process a successful job and delete the work", async () => {
      // Enqueue a work item
      const workId = await t.mutation(api.lib.enqueue, {
        fnHandle: "testHandle",
        fnName: "testFunction",
        fnArgs: { test: "data" },
        fnType: "mutation",
        runAt: Date.now(),
        config: {
          maxParallelism: 10,
          logLevel: "INFO",
        },
      });

      // Simulate a successful job completion
      await t.run(async (ctx) => {
        await completeHandler(ctx, {
          jobs: [
            {
              workId,
              runResult: { kind: "success", returnValue: "test result" },
              attempt: 0,
            },
          ],
        });
      });

      // Verify work was deleted
      await t.run(async (ctx) => {
        const work = await ctx.db.get(workId);
        expect(work).toBeNull();
      });

      // Verify pendingCompletion was created
      await t.run(async (ctx) => {
        const pendingCompletions = await ctx.db
          .query("pendingCompletion")
          .withIndex("workId", (q) => q.eq("workId", workId))
          .collect();
        expect(pendingCompletions).toHaveLength(1);
        expect(pendingCompletions[0].runResult.kind).toBe("success");
        expect(pendingCompletions[0].retry).toBe(false);
      });
    });

    it("should process a failed job with retry behavior", async () => {
      // Enqueue a work item with retry behavior
      const workId = await t.mutation(api.lib.enqueue, {
        fnHandle: "testHandle",
        fnName: "testFunction",
        fnArgs: { test: "data" },
        fnType: "mutation",
        runAt: Date.now(),
        config: {
          maxParallelism: 10,
          logLevel: "INFO",
        },
        retryBehavior: {
          maxAttempts: 3,
          initialBackoffMs: 100,
          base: 2,
        },
      });

      // Simulate a failed job completion
      await t.run(async (ctx) => {
        await completeHandler(ctx, {
          jobs: [
            {
              workId,
              runResult: { kind: "failed", error: "test error" },
              attempt: 0,
            },
          ],
        });
      });

      // Verify work was not deleted (since it should be retried)
      await t.run(async (ctx) => {
        const work = await ctx.db.get(workId);
        expect(work).not.toBeNull();
        expect(work?.attempts).toBe(1); // Incremented from 0
      });

      // Verify pendingCompletion was created with retry=true
      await t.run(async (ctx) => {
        const pendingCompletions = await ctx.db
          .query("pendingCompletion")
          .withIndex("workId", (q) => q.eq("workId", workId))
          .collect();
        expect(pendingCompletions).toHaveLength(1);
        expect(pendingCompletions[0].runResult.kind).toBe("failed");
        expect(pendingCompletions[0].retry).toBe(true);
      });
    });

    it("should process a failed job that has reached max attempts", async () => {
      // Enqueue a work item with retry behavior
      const workId = await t.mutation(api.lib.enqueue, {
        fnHandle: "testHandle",
        fnName: "testFunction",
        fnArgs: { test: "data" },
        fnType: "mutation",
        runAt: Date.now(),
        config: {
          maxParallelism: 10,
          logLevel: "INFO",
        },
        retryBehavior: {
          maxAttempts: 2, // Only 1 retry allowed
          initialBackoffMs: 100,
          base: 2,
        },
      });

      // Update the work to simulate it's already been attempted once
      await t.run(async (ctx) => {
        const work = await ctx.db.get(workId);
        if (work) {
          await ctx.db.patch(work._id, { attempts: 1 });
        }
      });

      // Simulate a failed job completion on the final attempt
      await t.run(async (ctx) => {
        await completeHandler(ctx, {
          jobs: [
            {
              workId,
              runResult: { kind: "failed", error: "test error" },
              attempt: 1,
            },
          ],
        });
      });

      // Verify work was deleted (since max attempts reached)
      await t.run(async (ctx) => {
        const work = await ctx.db.get(workId);
        expect(work).toBeNull();
      });

      // Verify pendingCompletion was created with retry=false
      await t.run(async (ctx) => {
        const pendingCompletions = await ctx.db
          .query("pendingCompletion")
          .withIndex("workId", (q) => q.eq("workId", workId))
          .collect();
        expect(pendingCompletions).toHaveLength(1);
        expect(pendingCompletions[0].runResult.kind).toBe("failed");
        expect(pendingCompletions[0].retry).toBe(false);
      });
    });

    it("should process a canceled job", async () => {
      // Enqueue a work item
      const workId = await t.mutation(api.lib.enqueue, {
        fnHandle: "testHandle",
        fnName: "testFunction",
        fnArgs: { test: "data" },
        fnType: "mutation",
        runAt: Date.now(),
        config: {
          maxParallelism: 10,
          logLevel: "INFO",
        },
      });

      // Simulate a canceled job completion
      await t.run(async (ctx) => {
        await completeHandler(ctx, {
          jobs: [
            {
              workId,
              runResult: { kind: "canceled" },
              attempt: 0,
            },
          ],
        });
      });

      // Verify work was deleted
      await t.run(async (ctx) => {
        const work = await ctx.db.get(workId);
        expect(work).toBeNull();
      });

      // Verify no pendingCompletion was created for canceled jobs
      await t.run(async (ctx) => {
        const pendingCompletions = await ctx.db
          .query("pendingCompletion")
          .withIndex("workId", (q) => q.eq("workId", workId))
          .collect();
        expect(pendingCompletions).toHaveLength(0);
      });
    });

    it("should call onComplete handler for successful jobs", async () => {
      // Create a spy on runMutation
      const runMutationSpy = vi.fn();

      // Enqueue a work item with onComplete handler
      const workId = await t.mutation(api.lib.enqueue, {
        fnHandle: "testHandle",
        fnName: "testFunction",
        fnArgs: { test: "data" },
        fnType: "mutation",
        runAt: Date.now(),
        config: {
          maxParallelism: 10,
          logLevel: "INFO",
        },
        onComplete: {
          fnHandle: "testOnComplete",
          context: { someContext: "value" },
        },
      });

      // Simulate a successful job completion with a spy on runMutation
      await t.run(async (ctx) => {
        // Create a modified context with a spy on runMutation
        const spyCtx = {
          ...ctx,
          runMutation: runMutationSpy,
        };

        await completeHandler(spyCtx, {
          jobs: [
            {
              workId,
              runResult: { kind: "success", returnValue: "test result" },
              attempt: 0,
            },
          ],
        });

        // Verify onComplete was called with the right arguments
        expect(runMutationSpy).toHaveBeenCalledWith(
          "testOnComplete",
          expect.objectContaining({
            workId,
            context: { someContext: "value" },
            result: { kind: "success", returnValue: "test result" },
          })
        );
      });
    });

    it("should handle multiple jobs in a single call", async () => {
      // Enqueue multiple work items
      const workId1 = await t.mutation(api.lib.enqueue, {
        fnHandle: "testHandle",
        fnName: "testFunction",
        fnArgs: { test: 1 },
        fnType: "mutation",
        runAt: Date.now(),
        config: {
          maxParallelism: 10,
          logLevel: "INFO",
        },
      });

      const workId2 = await t.mutation(api.lib.enqueue, {
        fnHandle: "testHandle",
        fnName: "testFunction",
        fnArgs: { test: 2 },
        fnType: "mutation",
        runAt: Date.now(),
        config: {
          maxParallelism: 10,
          logLevel: "INFO",
        },
        retryBehavior: {
          maxAttempts: 3,
          initialBackoffMs: 100,
          base: 2,
        },
      });

      // Simulate completion of multiple jobs
      await t.run(async (ctx) => {
        await completeHandler(ctx, {
          jobs: [
            {
              workId: workId1,
              runResult: { kind: "success", returnValue: "result 1" },
              attempt: 0,
            },
            {
              workId: workId2,
              runResult: { kind: "failed", error: "error 2" },
              attempt: 0,
            },
          ],
        });
      });

      // Verify both jobs were processed correctly
      await t.run(async (ctx) => {
        // First job should be deleted
        const work1 = await ctx.db.get(workId1);
        expect(work1).toBeNull();

        // Second job should still exist (for retry)
        const work2 = await ctx.db.get(workId2);
        expect(work2).not.toBeNull();
        expect(work2?.attempts).toBe(1);

        // Both should have pendingCompletion entries
        const pendingCompletions = await ctx.db
          .query("pendingCompletion")
          .collect();
        expect(pendingCompletions).toHaveLength(2);
      });
    });

    it("should handle mismatched attempt numbers", async () => {
      // Enqueue a work item
      const workId = await t.mutation(api.lib.enqueue, {
        fnHandle: "testHandle",
        fnName: "testFunction",
        fnArgs: { test: "data" },
        fnType: "mutation",
        runAt: Date.now(),
        config: {
          maxParallelism: 10,
          logLevel: "INFO",
        },
      });

      // Update the work to have a different attempt number
      await t.run(async (ctx) => {
        const work = await ctx.db.get(workId);
        if (work) {
          await ctx.db.patch(work._id, { attempts: 5 });
        }
      });

      // Simulate a job completion with mismatched attempt number
      await t.run(async (ctx) => {
        await completeHandler(ctx, {
          jobs: [
            {
              workId,
              runResult: { kind: "success", returnValue: "test result" },
              attempt: 0, // Mismatched with the work's attempt number (5)
            },
          ],
        });
      });

      // Verify work was not modified
      await t.run(async (ctx) => {
        const work = await ctx.db.get(workId);
        expect(work).not.toBeNull();
        expect(work?.attempts).toBe(5); // Should remain unchanged
      });

      // Verify no pendingCompletion was created
      await t.run(async (ctx) => {
        const pendingCompletions = await ctx.db
          .query("pendingCompletion")
          .withIndex("workId", (q) => q.eq("workId", workId))
          .collect();
        expect(pendingCompletions).toHaveLength(0);
      });
    });
  });
});
