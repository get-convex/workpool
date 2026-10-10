import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { internal } from "../_generated/api";
import { initConvexTest } from "../setup.test";

describe("benchmark cleanup", () => {
  let t: ReturnType<typeof initConvexTest>;
  beforeEach(() => {
    vi.useFakeTimers();
    t = initConvexTest();
  });
  afterEach(() => vi.useRealTimers());

  it.each([0, -1, 1.5, Infinity, 1001])(
    "rejects invalid limit %s before scheduling",
    async (limit) => {
      await expect(
        t.mutation(internal.test.cleanup.start, { limit }),
      ).rejects.toThrow("limit must be an integer between 1 and 1000");
      await expect(
        t.mutation(internal.test.cleanup.step, {
          index: 0,
          cursor: null,
          limit,
        }),
      ).rejects.toThrow("limit must be an integer between 1 and 1000");
      await t.run(async (ctx) => {
        expect(
          await ctx.db.system.query("_scheduled_functions").collect(),
        ).toHaveLength(0);
      });
    },
  );

  it("clears multiple pages and advances past empty tables", async () => {
    for (let i = 0; i < 3; i++) {
      await t.run(async (ctx) => {
        await ctx.db.insert("data", { data: i });
        await ctx.db.insert("schedulingProbes", {
          label: "test",
          at: Date.now(),
        });
      });
      vi.advanceTimersByTime(1);
    }
    await t.mutation(internal.test.cleanup.start, { limit: 1 });
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    expect(await t.query(internal.test.cleanup.counts)).toEqual({
      tasks: 0,
      latencyTasks: 0,
      runs: 0,
      schedulingProbes: 0,
      data: 0,
    });
  });

  it("clears rows sharing a creation time across page boundaries", async () => {
    // At this clock, convex-test's 0.001 ms creation-time increment rounds away.
    vi.setSystemTime(2 ** 44);
    await t.run(async (ctx) => {
      for (let i = 0; i < 3; i++) await ctx.db.insert("data", { data: i });
      const rows = await ctx.db.query("data").collect();
      expect(new Set(rows.map((row) => row._creationTime)).size).toBe(1);
    });
    await t.mutation(internal.test.cleanup.start, { limit: 1 });
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    expect((await t.query(internal.test.cleanup.counts)).data).toBe(0);
  });
});
