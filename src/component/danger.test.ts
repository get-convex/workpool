import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { TransactionLimits } from "convex/server";
import { internal } from "./_generated/api.js";
import type { Id } from "./_generated/dataModel.js";
import { INITIAL_STATE } from "./loop.js";
import { setupTest } from "./setup.test.js";
import { toSegment, toTimestamp } from "./shared.js";

describe("danger cleanup", () => {
  let t: ReturnType<typeof setupTest>;
  beforeEach(() => {
    vi.useFakeTimers();
    t = setupTest();
  });
  afterEach(() => vi.useRealTimers());

  async function enqueueWork(count: number, payloadBytes = 10) {
    return t.run(async (ctx) => {
      const workIds: Id<"work">[] = [];
      for (let i = 0; i < count; i++) {
        const payloadId = await ctx.db.insert("payload", {
          args: { data: "x".repeat(payloadBytes) },
        });
        const workId = await ctx.db.insert("work", {
          fnType: "action",
          fnHandle: "test",
          fnName: "test",
          attempts: 0,
          payloadId,
          payloadSize: payloadBytes,
        });
        workIds.push(workId);
        const pendingStartId = await ctx.db.insert("pendingStart", {
          workId,
          segment: toTimestamp(Date.now()),
        });
        await ctx.db.patch("work", workId, { pendingStartId });
      }
      return workIds;
    });
  }

  it.each([
    [250, 10, { documentsRead: 100 }],
    [250, 10, { documentsWritten: 100 }],
    [8, 800_000, { bytesRead: 4_000_000 }],
  ])(
    "resumes cleanup when a transaction budget is reached (%i works)",
    async (count, payloadBytes, transactionLimits) => {
      const ids = await enqueueWork(count, payloadBytes);
      const before = Date.now() + 1;
      vi.advanceTimersByTime(10);
      const newerIds = await enqueueWork(1);

      await t.run((ctx) =>
        ctx.runMutation(
          internal.danger.clearPending,
          { before },
          {
            transactionLimits: transactionLimits as TransactionLimits,
          },
        ),
      );
      await t.run(async (ctx) => {
        const remaining = (await ctx.db.query("pendingStart").collect())
          .map((p) => p.workId)
          .filter((id) => ids.includes(id));
        expect(remaining.length).toBeGreaterThan(0);
        expect(remaining.length).toBeLessThan(count);
        for (const id of remaining)
          expect(await ctx.db.get("work", id)).not.toBeNull();
      });
      await t.finishAllScheduledFunctions(vi.runAllTimers);
      await t.run(async (ctx) => {
        expect(
          (await ctx.db.query("work").collect()).map((w) => w._id),
        ).toEqual(newerIds);
        expect((await ctx.db.query("pendingStart").unique())!.workId).toBe(
          newerIds[0],
        );
        expect(await ctx.db.query("payload").collect()).toHaveLength(1);
      });
    },
  );

  it("removes legacy queue entries even below a persisted cursor", async () => {
    await t.run(async (ctx) => {
      const workId = await ctx.db.insert("work", {
        fnType: "action",
        fnHandle: "test",
        fnName: "test",
        attempts: 0,
      });
      await ctx.db.insert("pendingStart", {
        workId,
        segment: toSegment(Date.now()),
      });
      await ctx.db.insert("internalState", {
        ...INITIAL_STATE,
        segmentCursors: {
          incoming: toSegment(Date.now() + 60_000),
          completion: 0n,
          cancelation: 0n,
        },
      });
    });
    await t.mutation(internal.danger.clearOldWork, { before: Date.now() + 1 });
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    await t.run(async (ctx) => {
      expect(await ctx.db.query("work").collect()).toHaveLength(0);
      expect(await ctx.db.query("pendingStart").collect()).toHaveLength(0);
    });
  });
});
