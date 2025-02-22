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
import schema from "./schema.js";
import { modules } from "./setup.test.js";
import { DEFAULT_MAX_PARALLELISM, kickMainLoop } from "./kick.js";
import { DEFAULT_LOG_LEVEL } from "./logging.js";

describe("kickMainLoop", () => {
  beforeEach(() => {
    vi.useFakeTimers();
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
        logLevel: "DEBUG",
      });
      const after = await ctx.db.query("globals").unique();
      expect(after).not.toBeNull();
      assert(after);
      expect(after.maxParallelism).toBe(DEFAULT_MAX_PARALLELISM + 1);
      expect(after.logLevel).toBe("DEBUG");
    });
  });
});
