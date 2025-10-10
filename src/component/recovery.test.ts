import { convexTest } from "convex-test";
import type { WithoutSystemFields } from "convex/server";
import {
  afterEach,
  assert,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";
import { internal } from "./_generated/api.js";
import type { Doc, Id } from "./_generated/dataModel.js";
import type { MutationCtx } from "./_generated/server.js";
import { recoveryHandler } from "./recovery.js";
import schema from "./schema.js";

const modules = import.meta.glob("./**/*.ts");

describe("recovery", () => {
  async function setupTest() {
    const t = convexTest(schema, modules);
    return t;
  }

  let t: Awaited<ReturnType<typeof setupTest>>;

  // Helper function to create a work item
  async function makeDummyWork(
    ctx: MutationCtx,
    overrides: Partial<WithoutSystemFields<Doc<"work">>> = {}
  ) {
    return ctx.db.insert("work", {
      fnType: "action",
      fnHandle: "test_handle",
      fnName: "test_handle",
      fnArgs: {},
      attempts: 0,
      ...overrides,
    });
  }

  // Helper function to create a scheduled function
  async function makeDummyScheduledFunction(
    ctx: MutationCtx,
    workId: Id<"work">
  ) {
    return ctx.scheduler.runAfter(0, internal.worker.runActionWrapper, {
      workId,
      fnHandle: "test_handle",
      fnArgs: {},
      logLevel: "WARN",
      attempt: 0,
    });
  }

  beforeEach(async () => {
    vi.useFakeTimers();
    t = await setupTest();

    // Set up globals for logging
    await t.run(async (ctx) => {
      await ctx.db.insert("globals", {
        maxParallelism: 10,
        logLevel: "WARN",
      });
    });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe("recover", () => {
    it("should skip jobs that already have a pendingCompletion", async () => {
      // Create work and scheduled function

      const [workId, scheduledId] = await t.run(async (ctx) => {
        const workId = await makeDummyWork(ctx);
        const scheduledId = await makeDummyScheduledFunction(ctx, workId);

        // Create a pendingCompletion for this work
        await ctx.db.insert("pendingCompletion", {
          segment: BigInt(1),
          workId,
          runResult: { kind: "failed", error: "test error" },
          retry: true,
        });

        return [workId, scheduledId];
      });

      // Run recovery
      await t.mutation(internal.recovery.recover, {
        jobs: [
          {
            scheduledId,
            workId,
            attempt: 0,
            started: Date.now(),
          },
        ],
      });

      // Verify no additional pendingCompletion was created
      await t.run(async (ctx) => {
        const pendingCompletions = await ctx.db
          .query("pendingCompletion")
          .withIndex("workId", (q) => q.eq("workId", workId))
          .collect();
        expect(pendingCompletions).toHaveLength(1);
      });
    });

    it("should skip jobs where work is not found", async () => {
      // Create a non-existent work ID and a valid scheduled function ID
      const [workId, scheduledId] = await t.run(async (ctx) => {
        // Create a temporary work ID that we'll delete
        const workId = await makeDummyWork(ctx);
        const scheduledId = await makeDummyScheduledFunction(ctx, workId);

        // Delete the work to simulate it not being found
        await ctx.db.delete(workId);

        return [workId, scheduledId];
      });

      // Run recovery
      await t.mutation(internal.recovery.recover, {
        jobs: [
          {
            scheduledId,
            workId,
            attempt: 0,
            started: Date.now(),
          },
        ],
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

    it("should skip jobs where work attempts mismatch", async () => {
      // Create work and scheduled function
      const [workId, scheduledId] = await t.run(async (ctx) => {
        const workId = await makeDummyWork(ctx);
        const scheduledId = await makeDummyScheduledFunction(ctx, workId);

        // Update the work to have a different attempt number
        const work = await ctx.db.get(workId);
        if (work) {
          await ctx.db.patch(work._id, { attempts: 5 });
        }

        return [workId, scheduledId];
      });

      // Run recovery
      await t.mutation(internal.recovery.recover, {
        jobs: [
          {
            scheduledId,
            workId,
            attempt: 0, // Mismatched with the work's attempt number (5)
            started: Date.now(),
          },
        ],
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

    it("should handle scheduled job not found", async () => {
      // Create work but use a non-existent scheduled ID
      const [workId, scheduledId] = await t.run(async (ctx) => {
        const workId = await makeDummyWork(ctx);
        const scheduledId = await makeDummyScheduledFunction(ctx, workId);

        return [workId, scheduledId];
      });

      // Run recovery with mocked system.get
      await t.run(async (ctx) => {
        // Mock the system.get to return null for our scheduledId
        const originalGet = ctx.db.system.get;
        ctx.db.system.get = async (id) => {
          if (id === scheduledId) {
            return null;
          }
          return await originalGet(id);
        };

        await recoveryHandler(ctx, {
          jobs: [
            {
              scheduledId,
              workId,
              attempt: 0,
              started: Date.now(),
            },
          ],
        });
      });

      // Verify pendingCompletion was created with failure
      await t.run(async (ctx) => {
        const pendingCompletions = await ctx.db
          .query("pendingCompletion")
          .withIndex("workId", (q) => q.eq("workId", workId))
          .collect();
        expect(pendingCompletions).toHaveLength(1);
        expect(pendingCompletions[0].runResult.kind).toBe("failed");
        assert(pendingCompletions[0].runResult.kind === "failed");
        expect(pendingCompletions[0].runResult.error).toContain(
          "Scheduled job not found"
        );
      });
    });

    it("should handle failed scheduled jobs", async () => {
      // Create work and scheduled function

      const [workId, scheduledId] = await t.run(async (ctx) => {
        const workId = await makeDummyWork(ctx);
        const scheduledId = await makeDummyScheduledFunction(ctx, workId);

        return [workId, scheduledId];
      });

      // Run recovery with mocked failed state
      await t.run(async (ctx) => {
        // Mock the system.get to return a failed state
        const originalGet = ctx.db.system.get;
        ctx.db.system.get = async (id) => {
          if (id === scheduledId) {
            return {
              _id: scheduledId,
              _creationTime: Date.now(),
              name: "internal/worker.runActionWrapper",
              args: [
                {
                  workId,
                  fnHandle: "test_handle",
                  fnArgs: {},
                  logLevel: "WARN",
                  attempt: 0,
                },
              ],
              scheduledTime: Date.now(),
              state: {
                kind: "failed",
                error: "Function execution failed",
              },
            };
          }
          return await originalGet(id);
        };

        await recoveryHandler(ctx, {
          jobs: [
            {
              scheduledId,
              workId,
              attempt: 0,
              started: Date.now(),
            },
          ],
        });
      });

      // Verify pendingCompletion was created with the same failure
      await t.run(async (ctx) => {
        const pendingCompletions = await ctx.db
          .query("pendingCompletion")
          .withIndex("workId", (q) => q.eq("workId", workId))
          .collect();
        expect(pendingCompletions).toHaveLength(1);
        expect(pendingCompletions[0].runResult.kind).toBe("failed");
        assert(pendingCompletions[0].runResult.kind === "failed");
        expect(pendingCompletions[0].runResult.error).toBe(
          "Function execution failed"
        );
      });
    });

    it("should handle canceled scheduled jobs", async () => {
      // Create work and scheduled function
      let workId: Id<"work">;
      let scheduledId: Id<"_scheduled_functions">;

      await t.run(async (ctx) => {
        workId = await makeDummyWork(ctx);
        scheduledId = await makeDummyScheduledFunction(ctx, workId);
      });

      // Run recovery with mocked system.get
      await t.run(async (ctx) => {
        // Mock the system.get to return a canceled state
        const originalGet = ctx.db.system.get;
        ctx.db.system.get = async (id) => {
          if (id === scheduledId) {
            return {
              _id: scheduledId,
              _creationTime: Date.now(),
              name: "internal/worker.runActionWrapper",
              args: [
                {
                  workId,
                  fnHandle: "test_handle",
                  fnArgs: {},
                  logLevel: "WARN",
                  attempt: 0,
                },
              ],
              scheduledTime: Date.now(),
              state: {
                kind: "canceled",
              },
            };
          }
          return await originalGet(id);
        };

        await recoveryHandler(ctx, {
          jobs: [
            {
              scheduledId,
              workId,
              attempt: 0,
              started: Date.now(),
            },
          ],
        });
      });

      // Verify pendingCompletion was created with failure due to cancelation
      await t.run(async (ctx) => {
        const pendingCompletions = await ctx.db
          .query("pendingCompletion")
          .withIndex("workId", (q) => q.eq("workId", workId))
          .collect();
        expect(pendingCompletions).toHaveLength(1);
        expect(pendingCompletions[0].runResult.kind).toBe("failed");
        assert(pendingCompletions[0].runResult.kind === "failed");
        expect(pendingCompletions[0].runResult.error).toBe(
          "Canceled via scheduler"
        );
      });
    });

    it("should handle multiple jobs in a single call", async () => {
      // Create multiple work items and scheduled functions
      let workId1: Id<"work">;
      let workId2: Id<"work">;
      let scheduledId1: Id<"_scheduled_functions">;
      let scheduledId2: Id<"_scheduled_functions">;

      await t.run(async (ctx) => {
        workId1 = await makeDummyWork(ctx, { fnArgs: { test: 1 } });
        workId2 = await makeDummyWork(ctx, { fnArgs: { test: 2 } });
        scheduledId1 = await makeDummyScheduledFunction(ctx, workId1);
        scheduledId2 = await makeDummyScheduledFunction(ctx, workId2);
      });

      // Run recovery with mocked system.get
      await t.run(async (ctx) => {
        // Mock the system.get to return different states for each scheduled function
        const originalGet = ctx.db.system.get;
        ctx.db.system.get = async (id) => {
          if (id === scheduledId1) {
            return {
              _id: scheduledId1,
              _creationTime: Date.now(),
              name: "internal/worker.runActionWrapper",
              args: [
                {
                  workId: workId1,
                  fnHandle: "test_handle",
                  fnArgs: { test: 1 },
                  logLevel: "WARN",
                  attempt: 0,
                },
              ],
              scheduledTime: Date.now(),
              state: {
                kind: "failed",
                error: "Function 1 failed",
              },
            };
          } else if (id === scheduledId2) {
            return {
              _id: scheduledId2,
              _creationTime: Date.now(),
              name: "internal/worker.runActionWrapper",
              args: [
                {
                  workId: workId2,
                  fnHandle: "test_handle",
                  fnArgs: { test: 2 },
                  logLevel: "WARN",
                  attempt: 0,
                },
              ],
              scheduledTime: Date.now(),
              state: {
                kind: "canceled",
              },
            };
          }
          return await originalGet(id);
        };

        await recoveryHandler(ctx, {
          jobs: [
            {
              scheduledId: scheduledId1,
              workId: workId1,
              attempt: 0,
              started: Date.now(),
            },
            {
              scheduledId: scheduledId2,
              workId: workId2,
              attempt: 0,
              started: Date.now(),
            },
          ],
        });
      });

      // Verify both jobs were processed correctly
      await t.run(async (ctx) => {
        const pendingCompletions = await ctx.db
          .query("pendingCompletion")
          .collect();
        expect(pendingCompletions).toHaveLength(2);

        // Find completions for each work ID
        const completion1 = pendingCompletions.find(
          (pc) => pc.workId === workId1
        );
        const completion2 = pendingCompletions.find(
          (pc) => pc.workId === workId2
        );

        expect(completion1).toBeDefined();
        expect(completion2).toBeDefined();

        if (completion1) {
          expect(completion1.runResult.kind).toBe("failed");
          assert(completion1.runResult.kind === "failed");
          expect(completion1.runResult.error).toBe("Function 1 failed");
        }

        if (completion2) {
          expect(completion2.runResult.kind).toBe("failed");
          assert(completion2.runResult.kind === "failed");
          expect(completion2.runResult.error).toBe("Canceled via scheduler");
        }
      });
    });

    it("should not process jobs with other scheduled states", async () => {
      // Create work and scheduled function
      const [workId, scheduledId] = await t.run(async (ctx) => {
        const workId = await makeDummyWork(ctx);
        const scheduledId = await makeDummyScheduledFunction(ctx, workId);

        return [workId, scheduledId];
      });

      // Run recovery with mocked system.get
      await t.run(async (ctx) => {
        // Mock the system.get to return a pending state
        const originalGet = ctx.db.system.get;
        ctx.db.system.get = async (id) => {
          if (id === scheduledId) {
            return {
              _id: scheduledId,
              _creationTime: Date.now(),
              name: "internal/worker.runActionWrapper",
              args: [
                {
                  workId,
                  fnHandle: "test_handle",
                  fnArgs: {},
                  logLevel: "WARN",
                  attempt: 0,
                },
              ],
              scheduledTime: Date.now(),
              state: {
                kind: "pending",
              },
            };
          }
          return await originalGet(id);
        };

        await recoveryHandler(ctx, {
          jobs: [
            {
              scheduledId,
              workId,
              attempt: 0,
              started: Date.now(),
            },
          ],
        });
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
