import { convexTest } from "convex-test";
import { WithoutSystemFields } from "convex/server";
import {
  afterEach,
  assert,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";
import { api, internal } from "./_generated/api";
import { Doc, Id } from "./_generated/dataModel";
import { MutationCtx } from "./_generated/server";
import schema from "./schema";
import {
  DEFAULT_MAX_PARALLELISM,
  getCurrentSegment,
  getNextSegment,
  toSegment,
} from "./shared";
import { DEFAULT_LOG_LEVEL } from "./logging";

const modules = import.meta.glob("./**/*.ts");

describe("loop", () => {
  async function setupTest() {
    const t = convexTest(schema, modules);
    return t;
  }

  let t: Awaited<ReturnType<typeof setupTest>>;

  async function setMaxParallelism(maxParallelism: number) {
    await t.run(async (ctx) => {
      const globals = await ctx.db.query("globals").unique();
      if (!globals) {
        await ctx.db.insert("globals", {
          logLevel: DEFAULT_LOG_LEVEL,
          maxParallelism,
        });
      } else {
        await ctx.db.patch(globals._id, {
          maxParallelism,
        });
      }
    });
  }

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

  async function insertInternalState(
    ctx: MutationCtx,
    overrides: Partial<WithoutSystemFields<Doc<"internalState">>> = {}
  ) {
    await ctx.db.insert("internalState", {
      generation: 1n,
      segmentCursors: { incoming: 0n, completion: 0n, cancelation: 0n },
      lastRecovery: getCurrentSegment(),
      report: {
        completed: 0,
        succeeded: 0,
        failed: 0,
        retries: 0,
        canceled: 0,
        lastReportTs: Date.now(),
      },
      running: [],
      ...overrides,
    });
  }

  beforeEach(async () => {
    vi.useFakeTimers();
    t = await setupTest();
    await t.run(async (ctx) => {
      await ctx.db.insert("globals", {
        logLevel: "WARN",
        maxParallelism: DEFAULT_MAX_PARALLELISM,
      });
    });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe("data state machine", () => {
    it("should follow the pendingStart -> workerRunning -> complete flow", async () => {
      // Setup initial state
      const workId = await t.run<Id<"work">>(async (ctx) => {
        // Create internal state
        await insertInternalState(ctx);

        // Create running runStatus
        await ctx.db.insert("runStatus", {
          state: { kind: "running" },
        });

        // Create work
        const workId = await makeDummyWork(ctx, { attempts: 0 });

        // Create pendingStart
        await ctx.db.insert("pendingStart", {
          workId,
          segment: 1n,
        });

        return workId;
      });

      // Run main loop to process pendingStart -> workerRunning
      await t.mutation(internal.loop.main, { generation: 1n, segment: 1n });

      // Verify work is now in running state
      await t.run(async (ctx) => {
        // Check that pendingStart was deleted
        const pendingStarts = await ctx.db.query("pendingStart").collect();
        expect(pendingStarts).toHaveLength(0);

        // Check that work is in running list
        const state = await ctx.db.query("internalState").unique();
        expect(state).toBeDefined();
        assert(state);
        expect(state.running).toHaveLength(1);
        expect(state.running[0].workId).toBe(workId);
      });

      // Complete the work (workerRunning -> complete)
      await t.mutation(internal.complete.complete, {
        jobs: [
          {
            workId,
            runResult: { kind: "success", returnValue: null },
            attempt: 0,
          },
        ],
      });

      // Verify pendingCompletion was created
      await t.run(async (ctx) => {
        const pendingCompletions = await ctx.db
          .query("pendingCompletion")
          .collect();
        expect(pendingCompletions).toHaveLength(1);
        expect(pendingCompletions[0].workId).toBe(workId);
        expect(pendingCompletions[0].runResult.kind).toBe("success");
        expect(pendingCompletions[0].retry).toBe(false);
      });
    });

    it("should follow the pendingStart + pendingCancelation -> complete flow", async () => {
      // Setup initial state
      const workId = await t.run<Id<"work">>(async (ctx) => {
        // Create internal state
        await insertInternalState(ctx);

        // Create running runStatus
        await ctx.db.insert("runStatus", {
          state: { kind: "running" },
        });

        // Create work
        const workId = await makeDummyWork(ctx, { attempts: 0 });

        // Create pendingStart
        await ctx.db.insert("pendingStart", {
          workId,
          segment: 1n,
        });

        // Create pendingCancelation
        await ctx.db.insert("pendingCancelation", {
          workId,
          segment: 1n,
        });

        return workId;
      });

      // Run main loop to process pendingStart and pendingCancelation
      await t.mutation(internal.loop.main, { generation: 1n, segment: 1n });

      // Verify work was canceled
      await t.run(async (ctx) => {
        // Check that pendingStart was deleted
        const pendingStarts = await ctx.db.query("pendingStart").collect();
        expect(pendingStarts).toHaveLength(0);

        // Check that pendingCancelation was deleted
        const pendingCancelations = await ctx.db
          .query("pendingCancelation")
          .collect();
        expect(pendingCancelations).toHaveLength(0);

        // Check that work is not in running list
        const state = await ctx.db.query("internalState").unique();
        expect(state).toBeDefined();
        assert(state);
        expect(state.running).toHaveLength(0);
        expect(state.report.canceled).toBe(1);

        const work = await ctx.db.get(workId);
        expect(work).not.toBeNull();
        expect(work!.canceled).toBe(true);
      });
    });

    it("should follow the complete -> pendingCompletion -> pendingStart flow for retries", async () => {
      // Setup initial state with a running job that will need retry
      const workId = await t.run<Id<"work">>(async (ctx) => {
        // Create internal state
        await insertInternalState(ctx);

        // Create running runStatus
        await ctx.db.insert("runStatus", {
          state: { kind: "running" },
        });

        // Create work with retry behavior
        const workId = await makeDummyWork(ctx, {
          attempts: 0,
          retryBehavior: {
            maxAttempts: 3,
            initialBackoffMs: 1000,
            base: 2,
          },
        });

        // Schedule a function and get its ID
        const scheduledId = await makeDummyScheduledFunction(ctx, workId);

        // Add to running list
        const state = await ctx.db.query("internalState").unique();
        assert(state);
        await ctx.db.patch(state._id, {
          running: [{ workId, scheduledId, started: Date.now() }],
        });

        return workId;
      });

      // Complete the work with failure (workerRunning -> complete)
      await t.mutation(internal.complete.complete, {
        jobs: [
          {
            workId,
            runResult: { kind: "failed", error: "Test error" },
            attempt: 0,
          },
        ],
      });

      // Verify pendingCompletion was created with retry=true
      await t.run(async (ctx) => {
        const pendingCompletions = await ctx.db
          .query("pendingCompletion")
          .collect();
        expect(pendingCompletions).toHaveLength(1);
        expect(pendingCompletions[0].workId).toBe(workId);
        expect(pendingCompletions[0].runResult.kind).toBe("failed");
        expect(pendingCompletions[0].retry).toBe(true);
      });

      // Run main loop to process pendingCompletion -> pendingStart
      await t.mutation(internal.loop.main, {
        generation: 1n,
        segment: getNextSegment(),
      });

      // Verify work is now in pendingStart for retry
      await t.run(async (ctx) => {
        // Check that pendingCompletion was deleted
        const pendingCompletions = await ctx.db
          .query("pendingCompletion")
          .collect();
        expect(pendingCompletions).toHaveLength(0);

        // Check that pendingStart was created for retry
        const pendingStarts = await ctx.db.query("pendingStart").collect();
        expect(pendingStarts).toHaveLength(1);
        expect(pendingStarts[0].workId).toBe(workId);

        // Check that work still exists
        const work = await ctx.db.get(workId);
        expect(work).not.toBeNull();
        expect(work!.attempts).toBe(1);
      });
    });
  });

  describe("status transitions", () => {
    it("should transition from idle to running when work is enqueued", async () => {
      // Setup initial idle state
      await t.run(async (ctx) => {
        // Create internal state
        await insertInternalState(ctx);

        // Create idle runStatus
        await ctx.db.insert("runStatus", {
          state: { kind: "idle", generation: 1n },
        });
      });

      // Enqueue work
      await t.mutation(api.lib.enqueue, {
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

      // Verify state transition to running
      await t.run(async (ctx) => {
        const runStatus = await ctx.db.query("runStatus").unique();
        expect(runStatus).toBeDefined();
        assert(runStatus);
        expect(runStatus.state.kind).toBe("running");
      });
    });

    it("should transition from running to scheduled when all work is started and there's leftover capacity", async () => {
      // Setup initial running state with work
      await t.run(async (ctx) => {
        // Create internal state
        await insertInternalState(ctx);

        // Create running runStatus
        await ctx.db.insert("runStatus", {
          state: { kind: "running" },
        });

        // Create work
        const workId = await makeDummyWork(ctx);

        // Create pendingStart
        await ctx.db.insert("pendingStart", {
          workId,
          segment: 1n,
        });
      });

      // Run main loop to process the work
      await t.mutation(internal.loop.main, {
        generation: 1n,
        segment: getNextSegment(),
      });

      // Run updateRunStatus to transition to scheduled
      await t.mutation(internal.loop.updateRunStatus, {
        generation: 2n,
        segment: getNextSegment(),
      });

      // Verify state transition to scheduled
      await t.run(async (ctx) => {
        const runStatus = await ctx.db.query("runStatus").unique();
        expect(runStatus).toBeDefined();
        assert(runStatus);
        expect(runStatus.state.kind).toBe("scheduled");
        assert(runStatus.state.kind === "scheduled");
        expect(runStatus.state.saturated).toBe(false);
      });
    });

    it("should transition from running to saturated when maxed out", async () => {
      // Setup initial running state with max capacity
      await setMaxParallelism(1);
      const segment = getCurrentSegment();
      await t.run(async (ctx) => {
        // Create work item
        const workId = await makeDummyWork(ctx);

        // Schedule a function and get its ID
        const scheduledId = await makeDummyScheduledFunction(ctx, workId);

        // Create internal state with running job
        await insertInternalState(ctx, {
          running: [{ workId, scheduledId, started: Date.now() }],
        });

        // Create running runStatus
        await ctx.db.insert("runStatus", {
          state: { kind: "running" },
        });

        // Create another pendingStart to exceed capacity
        const anotherWorkId = await makeDummyWork(ctx);

        await ctx.db.insert("pendingStart", {
          workId: anotherWorkId,
          segment,
        });
      });

      // Run updateRunStatus to transition to scheduled with saturated=true
      await t.mutation(internal.loop.updateRunStatus, {
        generation: 1n,
        segment,
      });

      // Verify state transition to scheduled with saturated=true
      await t.run(async (ctx) => {
        const runStatus = await ctx.db.query("runStatus").unique();
        expect(runStatus).toBeDefined();
        assert(runStatus);
        expect(runStatus.state.kind).toBe("scheduled");
        assert(runStatus.state.kind === "scheduled");
        expect(runStatus.state.saturated).toBe(true);
      });
    });

    it("should transition from scheduled to running when new work is enqueued", async () => {
      // Setup initial scheduled state
      await t.run<Id<"_scheduled_functions">>(async (ctx) => {
        // Create internal state
        await insertInternalState(ctx);

        // Schedule main loop
        const scheduledId = await ctx.scheduler.runAfter(
          1000,
          internal.loop.main,
          { generation: 1n, segment: getNextSegment() + 10n }
        );

        // Create scheduled runStatus
        await ctx.db.insert("runStatus", {
          state: {
            kind: "scheduled",
            segment: getNextSegment() + 10n,
            scheduledId,
            saturated: false,
            generation: 1n,
          },
        });

        return scheduledId;
      });

      // Enqueue work to trigger transition to running
      await t.mutation(api.lib.enqueue, {
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

      // Verify state transition to running
      await t.run(async (ctx) => {
        const runStatus = await ctx.db.query("runStatus").unique();
        expect(runStatus).toBeDefined();
        assert(runStatus);
        expect(runStatus.state.kind).toBe("running");
      });
    });

    it("should transition from running to idle when all work is done", async () => {
      const segment = getNextSegment();
      // Setup initial running state with work
      const workId = await t.run<Id<"work">>(async (ctx) => {
        // Create internal state
        await insertInternalState(ctx);

        // Create running runStatus
        await ctx.db.insert("runStatus", {
          state: { kind: "running" },
        });

        // Create work
        const workId = await makeDummyWork(ctx, { attempts: 0 });

        // Create pendingStart
        await ctx.db.insert("pendingStart", {
          workId,
          segment,
        });

        return workId;
      });

      // Run main loop to process the work
      await t.mutation(internal.loop.main, { generation: 1n, segment });

      // Complete the work
      await t.mutation(internal.complete.complete, {
        jobs: [
          {
            workId,
            runResult: { kind: "success", returnValue: null },
            attempt: 0,
          },
        ],
      });

      // Run main loop again to process the completion
      await t.mutation(internal.loop.main, { generation: 2n, segment });

      // Run updateRunStatus to transition to idle
      await t.mutation(internal.loop.updateRunStatus, {
        generation: 3n,
        segment,
      });

      // Verify state transition to idle
      await t.run(async (ctx) => {
        const runStatus = await ctx.db.query("runStatus").unique();
        expect(runStatus).toBeDefined();
        assert(runStatus);
        expect(runStatus.state.kind).toBe("idle");
        assert(runStatus.state.kind === "idle");
      });
    });
    it("should transition from scheduled to running when main loop runs", async () => {
      const segment = getNextSegment();
      await t.run(async (ctx) => {
        await insertInternalState(ctx);

        const scheduledId = await ctx.scheduler.runAfter(
          1000,
          internal.loop.main,
          { generation: 1n, segment }
        );

        await ctx.db.insert("runStatus", {
          state: {
            kind: "scheduled",
            scheduledId,
            generation: 1n,
            segment,
            saturated: false,
          },
        });
      });
      // Run main loop
      await t.mutation(internal.loop.main, { generation: 1n, segment });

      // Verify state transition to running
      await t.run(async (ctx) => {
        const runStatus = await ctx.db.query("runStatus").unique();
        expect(runStatus).toBeDefined();
        assert(runStatus);
        expect(runStatus.state.kind).toBe("running");
      });
    });
  });

  describe("main function", () => {
    it("should handle generation mismatch", async () => {
      // Setup state with different generation
      await t.run(async (ctx) => {
        await insertInternalState(ctx, { generation: 2n });
      });

      // Call main with mismatched generation
      await expect(
        t.mutation(internal.loop.main, { generation: 1n, segment: 1n })
      ).rejects.toThrow("generation mismatch");
    });

    it("should process pending completions", async () => {
      // Setup state with a running job
      await t.run(async (ctx) => {
        // Create a work item for the running list
        const workId = await makeDummyWork(ctx);

        // Schedule a function and get its ID
        const scheduledId = await makeDummyScheduledFunction(ctx, workId);

        // Create internal state
        await insertInternalState(ctx, {
          running: [{ workId, scheduledId, started: 900000 }],
        });

        // Create pending completion
        await ctx.db.insert("pendingCompletion", {
          workId,
          runResult: { kind: "success", returnValue: null },
          segment: 1n,
          retry: false,
        });
      });

      // Call main
      await t.mutation(internal.loop.main, { generation: 1n, segment: 1n });

      // Verify completion was processed
      await t.run(async (ctx) => {
        // Check that pendingCompletion was deleted
        const completions = await ctx.db.query("pendingCompletion").collect();
        expect(completions).toHaveLength(0);

        // Check that work was removed from running list
        const state = await ctx.db.query("internalState").unique();
        expect(state).toBeDefined();
        assert(state);
        expect(state.running).toHaveLength(0);
        expect(state.report.completed).toBe(1);
        expect(state.report.succeeded).toBe(1);
      });
    });

    it("should handle job retries", async () => {
      // Setup state with a job that needs retry
      const workId = await t.run<Id<"work">>(async (ctx) => {
        // Create a work item for the running list
        const workId = await makeDummyWork(ctx, {
          attempts: 1,
          retryBehavior: {
            maxAttempts: 3,
            initialBackoffMs: 1000,
            base: 2,
          },
        });

        // Schedule a function and get its ID
        const scheduledId = await makeDummyScheduledFunction(ctx, workId);

        // Create internal state
        await insertInternalState(ctx, {
          running: [
            {
              workId,
              scheduledId,
              started: 900000,
            },
          ],
        });

        // Create pending completion with failed result
        await ctx.db.insert("pendingCompletion", {
          workId,
          runResult: { kind: "failed", error: "test error" },
          segment: 1n,
          retry: true,
        });

        return workId;
      });

      // Call main
      await t.mutation(internal.loop.main, { generation: 1n, segment: 1n });

      // Verify job was retried
      await t.run(async (ctx) => {
        // Check that pendingCompletion was deleted
        const completions = await ctx.db.query("pendingCompletion").collect();
        expect(completions).toHaveLength(0);

        // Check that work was updated
        const work = await ctx.db.get(workId);
        expect(work).toBeDefined();
        expect(work!.attempts).toBe(1);

        // Check that a new pendingStart was created
        const pendingStarts = await ctx.db.query("pendingStart").collect();
        expect(pendingStarts).toHaveLength(1);
        expect(pendingStarts[0].workId).toBe(workId);

        // Check that report was updated
        const state = await ctx.db.query("internalState").unique();
        expect(state).toBeDefined();
        expect(state!.report.retries).toBe(1);
      });
    });

    it("should process pending cancelations", async () => {
      // Setup state with a pending cancelation
      const workId = await t.run<Id<"work">>(async (ctx) => {
        // Create a work item for the running list
        const runningWorkId = await makeDummyWork(ctx);

        // Schedule a function and get its ID
        const scheduledId = await makeDummyScheduledFunction(
          ctx,
          runningWorkId
        );

        // Create internal state
        await insertInternalState(ctx, {
          running: [{ workId: runningWorkId, scheduledId, started: 900000 }],
        });

        // Create work
        const workId = await makeDummyWork(ctx, {
          retryBehavior: {
            maxAttempts: 3,
            initialBackoffMs: 1000,
            base: 2,
          },
        });

        // Create pending start
        await ctx.db.insert("pendingStart", {
          workId,
          segment: 1n,
        });

        // Create pending cancelation
        await ctx.db.insert("pendingCancelation", {
          workId,
          segment: 1n,
        });

        return workId;
      });

      // Call main
      await t.mutation(internal.loop.main, { generation: 1n, segment: 1n });

      // Verify cancelation was processed
      await t.run(async (ctx) => {
        // Check that pendingCancelation was deleted
        const cancelations = await ctx.db.query("pendingCancelation").collect();
        expect(cancelations).toHaveLength(0);

        // Check that pendingStart was deleted
        const pendingStarts = await ctx.db.query("pendingStart").collect();
        expect(pendingStarts).toHaveLength(0);

        const work = await ctx.db.get(workId);
        expect(work).toBeDefined();
        expect(work!.canceled).toBe(true);

        // Check that report was updated
        const state = await ctx.db.query("internalState").unique();
        expect(state).toBeDefined();
        expect(state!.report.canceled).toBe(1);
      });
    });

    it("should schedule new work", async () => {
      // Setup state with pending start items
      const workId = await t.run<Id<"work">>(async (ctx) => {
        // Create internal state
        await insertInternalState(ctx);

        // Create work
        const workId = await makeDummyWork(ctx);

        // Create pending start
        await ctx.db.insert("pendingStart", {
          workId,
          segment: 1n,
        });

        return workId;
      });

      // Call main
      await t.mutation(internal.loop.main, { generation: 1n, segment: 1n });

      // Verify work was started
      await t.run(async (ctx) => {
        // Check that pendingStart was deleted
        const pendingStarts = await ctx.db.query("pendingStart").collect();
        expect(pendingStarts).toHaveLength(0);

        // Check that work was added to running list
        const state = await ctx.db.query("internalState").unique();
        expect(state).toBeDefined();
        expect(state!.running).toHaveLength(1);
        expect(state!.running[0].workId).toBe(workId);
      });
    });

    it("should schedule recovery for old jobs", async () => {
      // Setup state with old running jobs
      const oldTime = Date.now() - 5 * 60 * 1000 - 1000; // Older than recovery threshold

      await t.run(async (ctx) => {
        // Create work for the running list
        const workId = await makeDummyWork(ctx);

        // Schedule a function and get its ID
        const scheduledId = await makeDummyScheduledFunction(ctx, workId);

        // Create internal state with old job
        await insertInternalState(ctx, {
          lastRecovery: 0n,
          running: [{ workId, scheduledId, started: oldTime }],
        });
      });

      // Call main
      const segment = toSegment(60 * 60 * 1000);
      await t.mutation(internal.loop.main, {
        generation: 1n,
        segment,
      });

      // Verify recovery was scheduled
      await t.run(async (ctx) => {
        // Check that lastRecovery was updated
        const state = await ctx.db.query("internalState").unique();
        expect(state).toBeDefined();
        expect(state!.lastRecovery).toBe(segment);

        // We can't directly check if recovery.recover was scheduled,
        // but we can verify the state was updated correctly
      });
    });
  });

  describe("updateRunStatus function", () => {
    it("should handle generation mismatch", async () => {
      // Setup state with different generation
      await t.run(async (ctx) => {
        await insertInternalState(ctx, { generation: 2n });
      });

      // Call updateRunStatus with mismatched generation
      await expect(
        t.mutation(internal.loop.updateRunStatus, {
          generation: 1n,
          segment: 1n,
        })
      ).rejects.toThrow("generation mismatch");
    });

    it("should schedule main immediately if there are outstanding cancelations", async () => {
      // Setup state with outstanding cancelations
      await t.run(async (ctx) => {
        // Create work for cancelation
        const workId = await makeDummyWork(ctx);

        // Create internal state
        await insertInternalState(ctx, {});

        // Create run status
        await ctx.db.insert("runStatus", {
          state: { kind: "running" },
        });

        // Create pending cancelation
        await ctx.db.insert("pendingCancelation", {
          workId,
          segment: 1n,
        });
      });

      // Call updateRunStatus
      await t.mutation(internal.loop.updateRunStatus, {
        generation: 1n,
        segment: 1n,
      });

      // Verify main was scheduled (indirectly by checking runStatus)
      await t.run(async (ctx) => {
        // We can't directly check if main was scheduled,
        // but we can verify the state was updated correctly
        const runStatus = await ctx.db.query("runStatus").unique();
        expect(runStatus).toBeDefined();
        // The state should no longer be idle
        expect(runStatus!.state.kind).not.toBe("idle");
      });
    });

    it("should transition to idle state when there is no work", async () => {
      // Setup state with no work
      await t.run(async (ctx) => {
        // Create internal state with no running jobs
        await insertInternalState(ctx, {});

        // Create run status in running state
        await ctx.db.insert("runStatus", {
          state: { kind: "running" },
        });
      });

      // Call updateRunStatus
      await t.mutation(internal.loop.updateRunStatus, {
        generation: 1n,
        segment: 1n,
      });

      // Verify idle state was set
      await t.run(async (ctx) => {
        const runStatus = await ctx.db.query("runStatus").unique();
        expect(runStatus).toBeDefined();
        expect(runStatus!.state.kind).toBe("idle");
        assert(runStatus!.state.kind === "idle");
        expect(runStatus!.state.generation).toBe(1n);
      });
    });

    it("should set saturated flag when at max capacity", async () => {
      // Setup state with running jobs at max capacity
      const now = getCurrentSegment();
      const later = now + 10n;
      await setMaxParallelism(10);
      await t.run(async (ctx) => {
        // Create 10 work items and scheduled functions
        const runningJobs = await Promise.all(
          Array(10)
            .fill(0)
            .map(async () => {
              const workId = await makeDummyWork(ctx);

              // Schedule a function and get its ID
              const scheduledId = await makeDummyScheduledFunction(ctx, workId);

              return { workId, scheduledId, started: Date.now() };
            })
        );

        // Create internal state with max running jobs
        await insertInternalState(ctx, {
          running: runningJobs,
        });

        // Create run status
        await ctx.db.insert("runStatus", {
          state: { kind: "running" },
        });

        // Create future completion to trigger scheduling
        await ctx.db.insert("pendingCompletion", {
          workId: runningJobs[0].workId,
          runResult: { kind: "success", returnValue: null },
          segment: later,
          retry: false,
        });
      });

      // Call updateRunStatus
      await t.mutation(internal.loop.updateRunStatus, {
        generation: 1n,
        segment: 1n,
      });

      // Verify scheduled state was set with saturated flag
      await t.run(async (ctx) => {
        const runStatus = await ctx.db.query("runStatus").unique();
        expect(runStatus).toBeDefined();
        expect(runStatus!.state.kind).toBe("scheduled");
        assert(runStatus!.state.kind === "scheduled");
        expect(runStatus!.state.saturated).toBe(true);
      });
    });

    it("should reset cursors correctly when there's old work detected", async () => {
      // Setup state with old work
      const now = getCurrentSegment();
      await t.run(async (ctx) => {
        // Create internal state with old work
        await insertInternalState(ctx, {
          segmentCursors: {
            incoming: now - 1n,
            completion: now - 1n,
            cancelation: now - 1n,
          },
        });
      });

      // Insert very old work
      await t.run(async (ctx) => {
        const workId = await makeDummyWork(ctx);
        await ctx.db.insert("pendingStart", {
          workId,
          segment: 0n,
        });
      });

      // Call updateRunStatus
      await t.mutation(internal.loop.updateRunStatus, {
        generation: 1n,
        segment: now,
      });

      // Verify cursors were reset
      await t.run(async (ctx) => {
        const state = await ctx.db.query("internalState").unique();
        expect(state).toBeDefined();
        expect(state!.segmentCursors.incoming).toBe(0n);
      });

      // Set maxParallelism to 0 so it doesn't schedule anything / make progress
      await setMaxParallelism(0);

      // Run main
      await t.mutation(internal.loop.main, {
        generation: 1n,
        segment: now,
      });

      // Verify start cursor weren't updated
      await t.run(async (ctx) => {
        const state = await ctx.db.query("internalState").unique();
        expect(state).toBeDefined();
        expect(state!.segmentCursors.incoming).toBe(0n);
      });
    });
  });

  describe("complete function", () => {
    it("should run onComplete handlers and delete work", async () => {
      // Setup mock work with onComplete handler
      const workId = await t.run<Id<"work">>(async (ctx) => {
        const workId = await makeDummyWork(ctx, {
          attempts: 0,
          onComplete: {
            // TODO: make this a real handle
            fnHandle: "onComplete_handle",
            context: { data: "test" },
          },
        });
        return workId;
      });

      // Call complete
      await t.mutation(internal.complete.complete, {
        jobs: [
          {
            workId,
            runResult: { kind: "success", returnValue: null },
            attempt: 0,
          },
        ],
      });

      // Verify work was deleted
      await t.run(async (ctx) => {
        const work = await ctx.db.get(workId);
        expect(work).toBeNull();
      });
    });

    it("should handle missing work gracefully", async () => {
      // Call complete with non-existent work ID
      const workId = await t.run(async (ctx) => {
        const id = await makeDummyWork(ctx, { attempts: 0 });
        await ctx.db.delete(id);
        return id;
      });
      await t.mutation(internal.complete.complete, {
        jobs: [
          {
            workId,
            runResult: { kind: "success", returnValue: null },
            attempt: 0,
          },
        ],
      });

      // No error should be thrown
    });
  });
});
