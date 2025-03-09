import { convexTest } from "convex-test";
import {
  afterEach,
  assert,
  beforeEach,
  describe,
  expect,
  test,
  vi,
} from "vitest";
import { internal } from "./_generated/api";
import { Id } from "./_generated/dataModel.js";
import { kickMainLoop } from "./kick.js";
import { DEFAULT_LOG_LEVEL } from "./logging.js";
import schema from "./schema.js";
import { modules } from "./setup.test.js";
import {
  fromSegment,
  getCurrentSegment,
  getNextSegment,
  toSegment,
} from "./shared";
import { DEFAULT_MAX_PARALLELISM } from "./shared.js";

describe("kickMainLoop", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date(1765432101234)); // Set to a known time
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  test("ensures it creates globals on first call", async () => {
    const t = convexTest(schema, modules);
    await t.run(async (ctx) => {
      await kickMainLoop(ctx, "enqueue");
      const globals = await ctx.db.query("globals").unique();
      expect(globals).not.toBeNull();
      const runStatus = await ctx.db.query("runStatus").unique();
      expect(runStatus).not.toBeNull();
      assert(runStatus);
      expect(runStatus.state.kind).toBe("running");
      const internalState = await ctx.db.query("internalState").unique();
      expect(internalState).not.toBeNull();
      assert(internalState);
      expect(internalState.generation).toBe(0n);
    });
  });

  test("it updates the globals when they change", async () => {
    const t = convexTest(schema, modules);
    await t.run(async (ctx) => {
      await kickMainLoop(ctx, "enqueue");
      const globals = await ctx.db.query("globals").unique();
      expect(globals).not.toBeNull();
      assert(globals);
      expect(globals.maxParallelism).toBe(DEFAULT_MAX_PARALLELISM);
      expect(globals.logLevel).toBe(DEFAULT_LOG_LEVEL);
      await kickMainLoop(ctx, "enqueue", {
        maxParallelism: DEFAULT_MAX_PARALLELISM + 1,
        logLevel: "ERROR",
      });
      const after = await ctx.db.query("globals").unique();
      expect(after).not.toBeNull();
      assert(after);
      expect(after.maxParallelism).toBe(DEFAULT_MAX_PARALLELISM + 1);
      expect(after.logLevel).toBe("ERROR");
    });
  });

  test("does not kick when already running", async () => {
    const t = convexTest(schema, modules);
    await t.run(async (ctx) => {
      // First kick to set up initial state
      await kickMainLoop(ctx, "enqueue");
      const runStatus = await ctx.db.query("runStatus").unique();
      assert(runStatus);
      expect(runStatus.state.kind).toBe("running");

      // Second kick should not change state
      const segment = await kickMainLoop(ctx, "enqueue");
      const afterStatus = await ctx.db.query("runStatus").unique();
      assert(afterStatus);
      expect(afterStatus.state.kind).toBe("running");
      expect(afterStatus._id).toBe(runStatus._id);
      expect(segment).toBe(getNextSegment());
    });
  });

  test("kicks when scheduled with later segment", async () => {
    const t = convexTest(schema, modules);
    await t.run(async (ctx) => {
      // Set up initial scheduled state
      await kickMainLoop(ctx, "enqueue");
      const runStatus = await ctx.db.query("runStatus").unique();
      assert(runStatus);

      // Get current segment and schedule for future
      const now = Date.now();
      const futureTime = now + 10000; // 10 seconds in future
      const futureSegment = toSegment(futureTime);

      // Manually set to scheduled state with future segment
      const scheduledId = await ctx.scheduler.runAfter(
        fromSegment(futureSegment) - now,
        internal.loop.main,
        {
          generation: 0n,
          segment: futureSegment,
        }
      );
      await ctx.db.patch(runStatus._id, {
        state: {
          kind: "scheduled",
          scheduledId,
          saturated: false,
          generation: 0n,
          segment: futureSegment,
        },
      });

      // Kick should reschedule to run sooner
      const segment = await kickMainLoop(ctx, "enqueue");
      expect(segment).toBe(getCurrentSegment());

      const afterStatus = await ctx.db.query("runStatus").unique();
      assert(afterStatus);
      expect(afterStatus.state.kind).toBe("running");
    });
  });

  test("does not kick when scheduled and saturated", async () => {
    const t = convexTest(schema, modules);
    await t.run(async (ctx) => {
      // Set up initial scheduled state
      await kickMainLoop(ctx, "enqueue");
      const runStatus = await ctx.db.query("runStatus").unique();
      assert(runStatus);

      // Get current segment
      const now = Date.now();
      const nearFutureTime = now + 1000; // 1 second in future
      const nearFutureSegment = toSegment(nearFutureTime);

      // Manually set to scheduled saturated state
      const scheduledId = await ctx.scheduler.runAfter(
        fromSegment(nearFutureSegment) - now,
        internal.loop.main,
        {
          generation: 0n,
          segment: nearFutureSegment,
        }
      );
      await ctx.db.patch(runStatus._id, {
        state: {
          kind: "scheduled",
          scheduledId,
          saturated: true,
          generation: 0n,
          segment: nearFutureSegment,
        },
      });

      // Kick should not change state when saturated
      const segment = await kickMainLoop(ctx, "enqueue");
      expect(segment).toBe(getNextSegment());
      const afterStatus = await ctx.db.query("runStatus").unique();
      assert(afterStatus);
      expect(afterStatus.state.kind).toBe("scheduled");
      assert(afterStatus.state.kind === "scheduled");
      expect(afterStatus.state.saturated).toBe(true);
    });
  });

  test("recovers if runStatus is deleted but other state exists", async () => {
    const t = convexTest(schema, modules);
    await t.run(async (ctx) => {
      // First create all state
      await kickMainLoop(ctx, "enqueue");

      // Delete runStatus
      const runStatus = await ctx.db.query("runStatus").unique();
      assert(runStatus);
      await ctx.db.delete(runStatus._id);

      // Kick should recreate runStatus
      await kickMainLoop(ctx, "complete");
      const newRunStatus = await ctx.db.query("runStatus").unique();
      expect(newRunStatus).not.toBeNull();
      assert(newRunStatus);
      expect(newRunStatus.state.kind).toBe("running");
    });
  });

  test("recovers if globals is deleted but other state exists", async () => {
    const t = convexTest(schema, modules);
    await t.run(async (ctx) => {
      // First create all state
      await kickMainLoop(ctx, "enqueue");

      // Delete globals
      const globals = await ctx.db.query("globals").unique();
      assert(globals);
      await ctx.db.delete(globals._id);

      // Kick should recreate globals
      await kickMainLoop(ctx, "complete");
      const newGlobals = await ctx.db.query("globals").unique();
      expect(newGlobals).not.toBeNull();
      assert(newGlobals);
      expect(newGlobals.maxParallelism).toBe(DEFAULT_MAX_PARALLELISM);
      expect(newGlobals.logLevel).toBe(DEFAULT_LOG_LEVEL);
    });
  });

  test("handles race conditions between multiple kicks", async () => {
    const t = convexTest(schema, modules);
    // Run kicks in separate transactions to simulate concurrent access
    const segments = await Promise.all(
      Array.from({ length: 10 }, () =>
        t.run(async (ctx) => {
          const segment = await kickMainLoop(ctx, "enqueue");
          return segment;
        })
      )
    );
    expect(segments.filter((s) => s === getCurrentSegment())).toHaveLength(1);

    // Check final state in a new transaction
    await t.run(async (ctx) => {
      // Should end up with single consistent state
      const runStatus = await ctx.db.query("runStatus").unique();
      const internalState = await ctx.db.query("internalState").unique();
      const globals = await ctx.db.query("globals").unique();

      expect(runStatus).not.toBeNull();
      expect(internalState).not.toBeNull();
      expect(globals).not.toBeNull();
      assert(runStatus);
      assert(internalState);
      assert(globals);

      expect(runStatus.state.kind).toBe("running");
      expect(internalState.generation).toBe(0n);
      expect(globals.maxParallelism).toBe(DEFAULT_MAX_PARALLELISM);
    });
  });

  test("preserves state between kicks with different sources", async () => {
    const t = convexTest(schema, modules);
    await t.run(async (ctx) => {
      // Initial kick with custom config
      await kickMainLoop(ctx, "enqueue", {
        maxParallelism: 5,
        logLevel: "ERROR",
      });

      // Kick from different sources
      await kickMainLoop(ctx, "cancel");
      await kickMainLoop(ctx, "complete");

      // Config should be preserved
      const globals = await ctx.db.query("globals").unique();
      expect(globals).not.toBeNull();
      assert(globals);
      expect(globals.maxParallelism).toBe(5);
      expect(globals.logLevel).toBe("ERROR");
    });
  });

  test("cancels and starts running when scheduled", async () => {
    const t = convexTest(schema, modules);
    await t.run(async (ctx) => {
      await kickMainLoop(ctx, "enqueue");
      const runStatus = await ctx.db.query("runStatus").unique();
      assert(runStatus);
      const segment = getNextSegment() + 10n;
      await ctx.db.patch(runStatus._id, {
        state: {
          generation: 0n,
          saturated: false,
          kind: "scheduled",
          segment,
          scheduledId: "" as Id<"_scheduled_functions">,
        },
      });
      // await all scheduled functions to run
      await kickMainLoop(ctx, "enqueue");
      const afterStatus = await ctx.db.query("runStatus").unique();
      assert(afterStatus);
      expect(afterStatus.state.kind).toBe("running");
      assert(afterStatus.state.kind === "running");
    });
  });
});
