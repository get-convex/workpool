import { convexTest } from "convex-test";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { internal } from "./_generated/api.js";
import type { Id } from "./_generated/dataModel.js";
import { createLogger } from "./logging.js";
import { INITIAL_STATE } from "./loop.js";
import schema from "./schema.js";
import { toSegment, toTimestamp } from "./shared.js";
import { generateReport } from "./stats.js";

const modules = import.meta.glob("./**/*.ts");
const setupTest = () => convexTest(schema, modules);

describe("stats reports", () => {
  let t: ReturnType<typeof setupTest>;
  let stateId: Id<"internalState">;
  const reports = () =>
    vi.mocked(console.info).mock.calls.map(([line]) => JSON.parse(line));

  beforeEach(async () => {
    vi.useFakeTimers();
    vi.spyOn(console, "info").mockImplementation(() => {});
    t = setupTest();
    stateId = await t.run((ctx) =>
      ctx.db.insert("internalState", {
        ...INITIAL_STATE,
        report: {
          ...INITIAL_STATE.report,
          completed: 10,
          succeeded: 6,
          failed: 2,
          retries: 2,
        },
      }),
    );
  });
  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  async function enqueue(segment: bigint, count = 1, legacy = false) {
    await t.run(async (ctx) => {
      const workIds: Id<"work">[] = [];
      for (let i = 0; i < count; i++) {
        workIds.push(
          await ctx.db.insert("work", {
            fnType: "action",
            fnHandle: "test",
            fnName: "test",
            attempts: 0,
          }),
        );
      }
      await ctx.db.insert("pendingStart", {
        segment,
        ...(legacy ? { workId: workIds[0] } : { workIds }),
      });
    });
  }

  async function report(logLevel: "REPORT" | "WARN" = "REPORT") {
    await t.run(async (ctx) => {
      const state = (await ctx.db.get("internalState", stateId))!;
      await generateReport(ctx, createLogger(logLevel), state, {
        maxParallelism: 10,
        logLevel,
      });
    });
  }

  it("skips reporting when disabled", async () => {
    await report("WARN");
    expect(reports()).toEqual([]);
    await t.run(async (ctx) => {
      expect(
        await ctx.db.system.query("_scheduled_functions").collect(),
      ).toHaveLength(0);
    });
  });

  it("reports a small eligible backlog without scheduling a count", async () => {
    await enqueue(toTimestamp(Date.now() - 500), 30);
    await enqueue(toSegment(Date.now()), 1, true);
    await enqueue(toTimestamp(Date.now() - 1000), 2);
    await enqueue(toSegment(Date.now() + 60_000), 1, true);
    await enqueue(toTimestamp(Date.now() + 60_000), 50);
    await t.run(async (ctx) =>
      ctx.db.patch("internalState", stateId, {
        segmentCursors: {
          ...INITIAL_STATE.segmentCursors,
          incoming: toTimestamp(Date.now() - 1500),
        },
      }),
    );
    await report();
    expect(reports()).toEqual([
      expect.objectContaining({
        event: "report",
        backlog: 2,
        running: 0,
        failureRate: 0.4,
        permanentFailureRate: 0.25,
      }),
    ]);
    await t.run(async (ctx) => {
      expect(
        await ctx.db.system.query("_scheduled_functions").collect(),
      ).toHaveLength(0);
    });
  });

  it("defers a large backlog count after a bounded read", async () => {
    for (let i = 0; i < 300; i++)
      await enqueue(toTimestamp(Date.now() - 1000), 2);
    await t.run(async (ctx) => {
      const state = (await ctx.db.get("internalState", stateId))!;
      const before = await ctx.meta.getTransactionMetrics();
      await generateReport(ctx, createLogger("REPORT"), state, {
        maxParallelism: 10,
        logLevel: "REPORT",
      });
      const after = await ctx.meta.getTransactionMetrics();
      expect(
        after.documentsRead.used - before.documentsRead.used,
      ).toBeLessThanOrEqual(11);
      const scheduled = await ctx.db.system
        .query("_scheduled_functions")
        .collect();
      expect(scheduled).toHaveLength(1);
      expect(scheduled[0].name).toBe("stats:calculateBacklogAndReport");
    });
    expect(reports()).toEqual([]);
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    expect(reports()).toEqual([expect.objectContaining({ backlog: 300 })]);
  });

  it("accepts reports scheduled by older versions", async () => {
    await enqueue(toSegment(Date.now()), 1, true);
    await enqueue(toSegment(Date.now() + 60_000), 1, true);
    await t.mutation(internal.stats.calculateBacklogAndReport, {
      startSegment: 0n,
      endSegment: toSegment(Date.now()),
      cursor: "old cursor",
      report: INITIAL_STATE.report,
      running: 0,
      logLevel: "REPORT",
    });
    expect(reports()).toEqual([expect.objectContaining({ backlog: 2 })]);
  });
});
