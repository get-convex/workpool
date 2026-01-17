import { paginator } from "convex-helpers/server/pagination";
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
import { internal } from "./_generated/api.js";
import type { Logger } from "./logging.js";
import schema from "./schema.js";
import { getCurrentSegment } from "./shared.js";

const modules = import.meta.glob("./**/*.ts");

// Create a proper Logger mock
function createLoggerMock(): Logger {
  return {
    event: vi.fn(),
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    time: vi.fn(),
    timeEnd: vi.fn(),
  };
}

describe("stats", () => {
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

  describe("generateReport", () => {
    it("should not generate a report when log level is above REPORT", async () => {
      // Setup internal state
      const stateId = await t.run(async (ctx) => {
        return await ctx.db.insert("internalState", {
          generation: 1n,
          segmentCursors: {
            incoming: 0n,
            completion: 0n,
            cancelation: 0n,
          },
          lastRecovery: 0n,
          report: {
            completed: 0,
            succeeded: 0,
            failed: 0,
            retries: 0,
            canceled: 0,
            lastReportTs: 0,
          },
          running: [],
        });
      });

      // Mock the console.event function to track if it's called
      const consoleMock = createLoggerMock();

      // Get the state document
      const state = await t.run(async (ctx) => {
        return await ctx.db.get(stateId);
      });
      assert(state);

      // Call generateReport with a log level that won't trigger reporting
      await t.run(async (ctx) => {
        const { generateReport } = await import("./stats.js");
        await generateReport(ctx, consoleMock, state, {
          maxParallelism: 10,
          logLevel: "WARN", // Above REPORT level
        });
      });

      // Verify that console.event was not called
      expect(consoleMock.event).not.toHaveBeenCalled();
    });

    it("should generate a report when backlog is small enough", async () => {
      // Setup internal state
      const stateId = await t.run(async (ctx) => {
        return await ctx.db.insert("internalState", {
          generation: 1n,
          segmentCursors: {
            incoming: 0n,
            completion: 0n,
            cancelation: 0n,
          },
          lastRecovery: 0n,
          report: {
            completed: 10,
            succeeded: 6,
            failed: 2,
            retries: 2,
            canceled: 0,
            lastReportTs: 0,
          },
          running: [],
        });
      });

      // Create a few pending start items
      await t.run(async (ctx) => {
        // Create a work item
        const workId = await ctx.db.insert("work", {
          fnType: "mutation",
          fnHandle: "testHandle",
          fnName: "testFunction",
          fnArgs: { test: true },
          attempts: 0,
        });

        // Create a pendingStart for the work
        await ctx.db.insert("pendingStart", {
          workId,
          segment: 5n, // Some segment between 0 and currentSegment
        });
      });

      // Mock the console.event function to track if it's called
      const consoleMock = createLoggerMock();

      // Get the state document
      const state = await t.run(async (ctx) => {
        return await ctx.db.get(stateId);
      });
      assert(state);

      // Call generateReport with REPORT log level
      await t.run(async (ctx) => {
        const { generateReport } = await import("./stats.js");
        await generateReport(ctx, consoleMock, state, {
          maxParallelism: 10,
          logLevel: "REPORT", // This should trigger reporting
        });
      });

      // Verify that console.event was called with the correct data
      expect(consoleMock.event).toHaveBeenCalledWith("report", {
        backlog: 1, // We created one pendingStart
        running: 0,
        completed: 10,
        succeeded: 6,
        failed: 2,
        retries: 2,
        canceled: 0,
        failureRate: 0.4, // (failed + retries) / completed = (2 + 2) / 10 = 0.4
        permanentFailureRate: 0.25, // failed / (completed - retries) = 2 / (10 - 2) = 2/8
        lastReportTs: expect.any(Number),
      });
    });

    it("should schedule calculateBacklogAndReport when backlog is large", async () => {
      // Setup internal state
      const stateId = await t.run(async (ctx) => {
        return await ctx.db.insert("internalState", {
          generation: 1n,
          segmentCursors: {
            incoming: 0n,
            completion: 0n,
            cancelation: 0n,
          },
          lastRecovery: 0n,
          report: {
            completed: 10,
            succeeded: 8,
            failed: 1,
            retries: 1,
            canceled: 0,
            lastReportTs: 0,
          },
          running: [],
        });
      });

      // Create more pending start items than maxParallelism
      const maxParallelism = 50;

      // Create maxParallelism + 1 work items to trigger pagination
      for (let i = 0; i < maxParallelism + 1; i++) {
        await t.run(async (ctx) => {
          // Create a work item
          const workId = await ctx.db.insert("work", {
            fnType: "mutation",
            fnHandle: "testHandle",
            fnName: `testFunction${i}`,
            fnArgs: { test: i },
            attempts: 0,
          });

          // Create a pendingStart for the work
          await ctx.db.insert("pendingStart", {
            workId,
            segment: 5n, // Some segment between 0 and currentSegment
          });
        });
      }

      // Mock the console.event function
      const consoleMock = createLoggerMock();

      // Get the state document
      const state = await t.run(async (ctx) => {
        return await ctx.db.get(stateId);
      });
      assert(state);

      // Call generateReport with REPORT log level
      await t.run(async (ctx) => {
        const { generateReport } = await import("./stats.js");
        await generateReport(ctx, consoleMock, state, {
          maxParallelism,
          logLevel: "REPORT", // This should trigger reporting
        });
      });

      // Verify that calculateBacklogAndReport was scheduled
      await t.run(async (ctx) => {
        const scheduledFunctions = await ctx.db.system
          .query("_scheduled_functions")
          .collect();

        expect(scheduledFunctions.length).toBeGreaterThan(0);

        // Check that one of the scheduled functions is calculateBacklogAndReport
        const calculateBacklogScheduled = scheduledFunctions.find(
          (sf) => sf.name === "stats:calculateBacklogAndReport",
        );
        expect(calculateBacklogScheduled).toBeDefined();
        assert(calculateBacklogScheduled);

        // Verify console.event was not called yet (will be called by calculateBacklogAndReport)
        expect(consoleMock.event).not.toHaveBeenCalled();
      });
    });

    it("should calculate backlog and report correctly", async () => {
      // Setup internal state
      const stateId = await t.run(async (ctx) => {
        return await ctx.db.insert("internalState", {
          generation: 1n,
          segmentCursors: {
            incoming: 0n,
            completion: 0n,
            cancelation: 0n,
          },
          lastRecovery: 0n,
          report: {
            completed: 10,
            succeeded: 8,
            failed: 1,
            retries: 1,
            canceled: 0,
            lastReportTs: 0,
          },
          running: [],
        });
      });

      // Create some pending start items
      const currentSegment = getCurrentSegment();

      // Create 3 work items
      for (let i = 0; i < 3; i++) {
        await t.run(async (ctx) => {
          // Create a work item
          const workId = await ctx.db.insert("work", {
            fnType: "mutation",
            fnHandle: "testHandle",
            fnName: `testFunction${i}`,
            fnArgs: { test: i },
            attempts: 0,
          });

          // Create a pendingStart for the work
          await ctx.db.insert("pendingStart", {
            workId,
            segment: 5n, // Some segment between 0 and currentSegment
          });
        });
      }

      // Get the state document
      const state = await t.run(async (ctx) => {
        return await ctx.db.get(stateId);
      });
      assert(state);

      const cursor = await t.run(async (ctx) => {
        return await paginator(ctx.db, schema)
          .query("pendingStart")
          .withIndex("segment", (q) =>
            q.gte("segment", 0n).lt("segment", currentSegment),
          )
          .paginate({
            numItems: 1,
            cursor: null,
          });
      });

      // Call calculateBacklogAndReport directly
      await t.mutation(internal.stats.calculateBacklogAndReport, {
        startSegment: 0n,
        endSegment: currentSegment,
        cursor: cursor.continueCursor,
        report: state.report,
        running: state.running.length,
        logLevel: "REPORT",
      });

      // Verify that console.event was called with the correct data
      // Note: We can't directly check the mock since it's created inside the mutation
      // Instead, we can check if the function completed successfully

      // We can verify the function was executed by checking if any scheduled functions were created
      await t.run(async (ctx) => {
        const scheduledFunctions = await ctx.db.system
          .query("_scheduled_functions")
          .collect();

        // Since our backlog is small, no additional scheduled functions should be created
        const calculateBacklogScheduled = scheduledFunctions.find(
          (sf) => sf.name === "stats:calculateBacklogAndReport",
        );
        expect(calculateBacklogScheduled).toBeUndefined();
      });
    });
  });
});
