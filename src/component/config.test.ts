import { convexTest } from "convex-test";
import { assert, expect, test } from "vitest";
import { DEFAULT_LOG_LEVEL } from "./logging.js";
import schema from "./schema.js";
import { modules } from "./setup.test.js";
import { DEFAULT_MAX_PARALLELISM } from "./shared.js";
import { getOrUpdateGlobals } from "./config.js";

test("it updates the globals when they change", async () => {
  const t = convexTest(schema, modules);
  await t.run(async (ctx) => {
    await getOrUpdateGlobals(ctx, {
      maxParallelism: DEFAULT_MAX_PARALLELISM,
      logLevel: DEFAULT_LOG_LEVEL,
    });
    const globals = await ctx.db.query("globals").unique();
    expect(globals).not.toBeNull();
    assert(globals);
    expect(globals.maxParallelism).toBe(DEFAULT_MAX_PARALLELISM);
    expect(globals.logLevel).toBe(DEFAULT_LOG_LEVEL);
    await getOrUpdateGlobals(ctx, {
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
