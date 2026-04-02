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
import schema from "./schema.js";
import {
  DEFAULT_MAX_PARALLELISM,
  getCurrentSegment,
  getNextSegment,
  toSegment,
} from "./shared.js";
import { RECOVERY_PERIOD_SEGMENTS } from "./loop.js";

const modules = import.meta.glob("./**/*.ts");

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type WorkConfig = {
  attempts: number;
  canceled?: boolean;
  hasRetryBehavior: boolean;
  fnType: "action" | "mutation";
};

type PendingCompletionConfig =
  | false
  | { retry: boolean; resultKind: "success" | "failed" | "canceled" };

/** Declarative description of a job's composite state across all tables. */
type CompositeState = {
  work: false | WorkConfig;
  pendingStart: boolean;
  running: boolean;
  pendingCompletion: PendingCompletionConfig;
  pendingCancelation: boolean;
};

/** What we actually observe after reading the DB. */
type ObservedState = {
  work:
    | false
    | {
        attempts: number;
        canceled: boolean;
        hasRetryBehavior: boolean;
        fnType: string;
      };
  pendingStart: boolean;
  running: boolean;
  pendingCompletion: false | { retry: boolean; resultKind: string };
  pendingCancelation: boolean;
};

// ---------------------------------------------------------------------------
// Named states
// ---------------------------------------------------------------------------

const S1_ENQUEUED: CompositeState = {
  work: { attempts: 0, hasRetryBehavior: false, fnType: "action" },
  pendingStart: true,
  running: false,
  pendingCompletion: false,
  pendingCancelation: false,
};

const S2_RUNNING: CompositeState = {
  work: { attempts: 0, hasRetryBehavior: false, fnType: "action" },
  pendingStart: false,
  running: true,
  pendingCompletion: false,
  pendingCancelation: false,
};

const S2_RUNNING_WITH_RETRIES: CompositeState = {
  ...S2_RUNNING,
  work: { attempts: 0, hasRetryBehavior: true, fnType: "action" },
};

const S3_COMPLETING_NO_RETRY: CompositeState = {
  work: false,
  pendingStart: false,
  running: true,
  pendingCompletion: { retry: false, resultKind: "success" },
  pendingCancelation: false,
};

const S3_COMPLETING_FAILED_FINAL: CompositeState = {
  work: false,
  pendingStart: false,
  running: true,
  pendingCompletion: { retry: false, resultKind: "failed" },
  pendingCancelation: false,
};

const S5_COMPLETING_WILL_RETRY: CompositeState = {
  work: { attempts: 1, hasRetryBehavior: true, fnType: "action" },
  pendingStart: false,
  running: true,
  pendingCompletion: { retry: true, resultKind: "failed" },
  pendingCancelation: false,
};

const S7_CANCEL_PENDING: CompositeState = {
  work: { attempts: 0, hasRetryBehavior: false, fnType: "action" },
  pendingStart: true,
  running: false,
  pendingCompletion: false,
  pendingCancelation: true,
};

const S8_CANCEL_RUNNING: CompositeState = {
  work: { attempts: 0, hasRetryBehavior: false, fnType: "action" },
  pendingStart: false,
  running: true,
  pendingCompletion: false,
  pendingCancelation: true,
};

const S9_CANCELED_RETRYING: CompositeState = {
  work: {
    attempts: 1,
    canceled: true,
    hasRetryBehavior: true,
    fnType: "action",
  },
  pendingStart: false,
  running: true,
  pendingCompletion: { retry: true, resultKind: "failed" },
  pendingCancelation: false,
};

const S10_REENQUEUED: CompositeState = {
  work: { attempts: 1, hasRetryBehavior: true, fnType: "action" },
  pendingStart: true,
  running: false,
  pendingCompletion: false,
  pendingCancelation: false,
};

const S12_CANCELED_AWAITING_COMPLETE: CompositeState = {
  work: {
    attempts: 0,
    canceled: true,
    hasRetryBehavior: false,
    fnType: "action",
  },
  pendingStart: false,
  running: true,
  pendingCompletion: false,
  pendingCancelation: false,
};

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

const ACTION_RECOVERY_THRESHOLD_MS = 5 * 60 * 1000;

describe("state machine", () => {
  let t: ReturnType<typeof convexTest>;
  let generation: bigint;

  function nextGen() {
    return generation++;
  }

  async function makeDummyWork(
    ctx: MutationCtx,
    overrides: Partial<WithoutSystemFields<Doc<"work">>> = {},
  ) {
    return ctx.db.insert("work", {
      fnType: "action",
      fnHandle: "test_handle",
      fnName: "test_fn",
      fnArgs: {},
      attempts: 0,
      ...overrides,
    });
  }

  async function makeDummyScheduledFunction(
    ctx: MutationCtx,
    workId: Id<"work">,
  ) {
    return ctx.scheduler.runAfter(0, internal.worker.runActionWrapper, {
      workId,
      fnHandle: "test_handle",
      fnArgs: {},
      logLevel: "WARN",
      attempt: 0,
    });
  }

  /**
   * Materialize a CompositeState in the DB.
   * Returns the workId (a real one if work exists, a deleted one if absent).
   */
  async function setupState(
    state: CompositeState,
    opts?: {
      /** Make running jobs old enough for recovery. */
      oldForRecovery?: boolean;
      /** Override the segment for pending entries. */
      segment?: bigint;
    },
  ): Promise<{ workId: Id<"work">; segment: bigint }> {
    const seg = opts?.segment ?? getNextSegment();
    const workId = await t.run<Id<"work">>(async (ctx) => {
      let wId: Id<"work">;
      if (state.work) {
        wId = await makeDummyWork(ctx, {
          fnType: state.work.fnType,
          attempts: state.work.attempts,
          canceled: state.work.canceled,
          retryBehavior: state.work.hasRetryBehavior
            ? { maxAttempts: 5, initialBackoffMs: 100, base: 2 }
            : undefined,
        });
      } else {
        // Create and delete to get a valid Id
        wId = await makeDummyWork(ctx);
        await ctx.db.delete(wId);
      }

      // Set up running array
      const runningEntry: Doc<"internalState">["running"] = [];
      if (state.running) {
        const scheduledId = await makeDummyScheduledFunction(ctx, wId);
        const started = opts?.oldForRecovery
          ? Date.now() - ACTION_RECOVERY_THRESHOLD_MS - 10000
          : Date.now();
        runningEntry.push({ workId: wId, scheduledId, started });
      }

      // Set up internalState
      const lastRecovery = opts?.oldForRecovery
        ? getCurrentSegment() - RECOVERY_PERIOD_SEGMENTS - 1n
        : getCurrentSegment();
      await ctx.db.insert("internalState", {
        generation,
        segmentCursors: { incoming: 0n, completion: 0n, cancelation: 0n },
        lastRecovery,
        report: {
          completed: 0,
          succeeded: 0,
          failed: 0,
          retries: 0,
          canceled: 0,
          conflicted: 0,
          lastReportTs: Date.now(),
        },
        running: runningEntry,
      });

      // runStatus
      await ctx.db.insert("runStatus", { state: { kind: "running" } });

      // pendingStart
      if (state.pendingStart) {
        await ctx.db.insert("pendingStart", { workId: wId, segment: seg });
      }

      // pendingCompletion
      if (state.pendingCompletion) {
        const resultMap = {
          success: { kind: "success" as const, returnValue: null },
          failed: { kind: "failed" as const, error: "test error" },
          canceled: { kind: "canceled" as const },
        };
        await ctx.db.insert("pendingCompletion", {
          workId: wId,
          segment: seg,
          retry: state.pendingCompletion.retry,
          runResult: resultMap[state.pendingCompletion.resultKind],
        });
      }

      // pendingCancelation
      if (state.pendingCancelation) {
        await ctx.db.insert("pendingCancelation", {
          workId: wId,
          segment: seg,
        });
      }

      return wId;
    });

    return { workId, segment: seg };
  }

  /** Read the DB and return the observed state for a given workId. */
  async function observeState(workId: Id<"work">): Promise<ObservedState> {
    return t.run(async (ctx) => {
      const work = await ctx.db.get(workId);
      const ps = await ctx.db
        .query("pendingStart")
        .withIndex("workId", (q) => q.eq("workId", workId))
        .first();
      const state = await ctx.db.query("internalState").unique();
      const inRunning =
        state?.running.some((r) => r.workId === workId) ?? false;
      const pc = await ctx.db
        .query("pendingCompletion")
        .withIndex("workId", (q) => q.eq("workId", workId))
        .first();
      const pcancel = await ctx.db
        .query("pendingCancelation")
        .withIndex("workId", (q) => q.eq("workId", workId))
        .first();

      return {
        work: work
          ? {
              attempts: work.attempts,
              canceled: !!work.canceled,
              hasRetryBehavior: !!work.retryBehavior,
              fnType: work.fnType,
            }
          : false,
        pendingStart: !!ps,
        running: inRunning,
        pendingCompletion: pc
          ? { retry: pc.retry, resultKind: pc.runResult.kind }
          : false,
        pendingCancelation: !!pcancel,
      };
    });
  }

  /** Run main loop at given segment. */
  async function runMain(segment: bigint) {
    await t.mutation(internal.loop.main, {
      generation: nextGen(),
      segment,
    });
  }

  /** Run complete with given result. */
  async function runComplete(
    workId: Id<"work">,
    result:
      | { kind: "success" }
      | { kind: "failed" }
      | { kind: "canceled" },
    attempt: number,
  ) {
    const runResult =
      result.kind === "success"
        ? { kind: "success" as const, returnValue: null }
        : result.kind === "failed"
          ? { kind: "failed" as const, error: "test error" }
          : { kind: "canceled" as const };
    await t.mutation(internal.complete.complete, {
      jobs: [{ workId, runResult, attempt }],
    });
  }

  beforeEach(async () => {
    vi.useFakeTimers();
    t = convexTest(schema, modules);
    generation = 1n;
    await t.run(async (ctx) => {
      await ctx.db.insert("globals", {
        logLevel: "ERROR",
        maxParallelism: DEFAULT_MAX_PARALLELISM,
      });
    });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  // =========================================================================
  // main loop transitions
  // =========================================================================

  describe("main loop transitions", () => {
    it("S1 enqueued -> S2 running: starts work", async () => {
      const { workId, segment } = await setupState(S1_ENQUEUED);
      await runMain(segment);
      const s = await observeState(workId);
      expect(s.work).toBeTruthy();
      expect(s.pendingStart).toBe(false);
      expect(s.running).toBe(true);
      expect(s.pendingCompletion).toBe(false);
      expect(s.pendingCancelation).toBe(false);
    });

    it("S10 re-enqueued -> S2 running: starts retry work", async () => {
      const { workId, segment } = await setupState(S10_REENQUEUED);
      await runMain(segment);
      const s = await observeState(workId);
      expect(s.work).toBeTruthy();
      expect(s.pendingStart).toBe(false);
      expect(s.running).toBe(true);
    });

    it("S3 completing (success, no retry) -> finished: removes from running", async () => {
      const { workId, segment } = await setupState(S3_COMPLETING_NO_RETRY);
      await runMain(segment);
      const s = await observeState(workId);
      expect(s.work).toBe(false);
      expect(s.pendingStart).toBe(false);
      expect(s.running).toBe(false);
      expect(s.pendingCompletion).toBe(false);
      expect(s.pendingCancelation).toBe(false);
    });

    it("S3 completing (failed, no retry) -> finished: removes from running", async () => {
      const { workId, segment } = await setupState(S3_COMPLETING_FAILED_FINAL);
      await runMain(segment);
      const s = await observeState(workId);
      expect(s.work).toBe(false);
      expect(s.running).toBe(false);
      expect(s.pendingCompletion).toBe(false);
    });

    it("S5 completing (will retry) -> retried: reschedules and may restart immediately", async () => {
      const { workId, segment } = await setupState(S5_COMPLETING_WILL_RETRY);
      await runMain(segment);
      const s = await observeState(workId);
      expect(s.work).toBeTruthy();
      expect(s.pendingCompletion).toBe(false);
      // With small backoff (100ms), the retry pendingStart may be picked up
      // immediately by handleStart in the same main pass. Either way, the
      // job should be progressing: either in pendingStart or running.
      expect(s.pendingStart || s.running).toBe(true);
    });

    it("S7 cancel-pending -> canceled: deletes pendingStart, marks canceled", async () => {
      const { workId, segment } = await setupState(S7_CANCEL_PENDING);
      await runMain(segment);
      const s = await observeState(workId);
      expect(s.pendingStart).toBe(false);
      expect(s.pendingCancelation).toBe(false);
      expect(s.running).toBe(false);
      if (s.work) {
        expect(s.work.canceled).toBe(true);
      }
    });

    it("S8 cancel-running -> S12 canceled, still running", async () => {
      const { workId, segment } = await setupState(S8_CANCEL_RUNNING);
      await runMain(segment);
      const s = await observeState(workId);
      expect(s.pendingCancelation).toBe(false);
      expect(s.running).toBe(true);
      assert(s.work);
      expect(s.work.canceled).toBe(true);
    });

    it("S9 canceled+retrying -> does not retry, cancels instead", async () => {
      const { workId, segment } = await setupState(S9_CANCELED_RETRYING);
      await runMain(segment);
      const s = await observeState(workId);
      expect(s.running).toBe(false);
      expect(s.pendingCompletion).toBe(false);
      // Should not have re-enqueued
      expect(s.pendingStart).toBe(false);
    });

    it("S2 running (no pending events) -> S2 no change", async () => {
      const { workId, segment } = await setupState(S2_RUNNING);
      await runMain(segment);
      const s = await observeState(workId);
      expect(s.running).toBe(true);
      assert(s.work);
      expect(s.work.attempts).toBe(0);
    });

    it("S12 canceled awaiting complete -> no change (still running)", async () => {
      const { workId, segment } = await setupState(
        S12_CANCELED_AWAITING_COMPLETE,
      );
      await runMain(segment);
      const s = await observeState(workId);
      expect(s.running).toBe(true);
      assert(s.work);
      expect(s.work.canceled).toBe(true);
    });
  });

  // =========================================================================
  // complete.complete transitions
  // =========================================================================

  describe("complete.complete transitions", () => {
    it("S2 running + complete(success) -> work deleted, pendingCompletion(retry=false)", async () => {
      const { workId } = await setupState(S2_RUNNING);
      await runComplete(workId, { kind: "success" }, 0);
      const s = await observeState(workId);
      expect(s.work).toBe(false);
      assert(s.pendingCompletion);
      expect(s.pendingCompletion.retry).toBe(false);
      expect(s.pendingCompletion.resultKind).toBe("success");
    });

    it("S2 running (with retries) + complete(failed) -> work kept, pendingCompletion(retry=true)", async () => {
      const { workId } = await setupState(S2_RUNNING_WITH_RETRIES);
      await runComplete(workId, { kind: "failed" }, 0);
      const s = await observeState(workId);
      assert(s.work);
      expect(s.work.attempts).toBe(1);
      assert(s.pendingCompletion);
      expect(s.pendingCompletion.retry).toBe(true);
    });

    it("S2 running (no retries) + complete(failed) -> work deleted, pendingCompletion(retry=false)", async () => {
      const { workId } = await setupState(S2_RUNNING);
      await runComplete(workId, { kind: "failed" }, 0);
      const s = await observeState(workId);
      expect(s.work).toBe(false);
      assert(s.pendingCompletion);
      expect(s.pendingCompletion.retry).toBe(false);
      expect(s.pendingCompletion.resultKind).toBe("failed");
    });

    it("S2 running + complete(canceled) -> work deleted, NO pendingCompletion", async () => {
      const { workId } = await setupState(S2_RUNNING);
      await runComplete(workId, { kind: "canceled" }, 0);
      const s = await observeState(workId);
      expect(s.work).toBe(false);
      expect(s.pendingCompletion).toBe(false);
    });

    it("S12 canceled running + complete(success) -> work deleted (complete ignores canceled flag)", async () => {
      const { workId } = await setupState(S12_CANCELED_AWAITING_COMPLETE);
      await runComplete(workId, { kind: "success" }, 0);
      const s = await observeState(workId);
      expect(s.work).toBe(false);
      assert(s.pendingCompletion);
      expect(s.pendingCompletion.retry).toBe(false);
    });

    it("complete with wrong attempt number -> no-op", async () => {
      const { workId } = await setupState(S2_RUNNING);
      await runComplete(workId, { kind: "success" }, 999);
      const s = await observeState(workId);
      // Work should still exist (complete ignored the mismatched attempt)
      assert(s.work);
      expect(s.work.attempts).toBe(0);
      expect(s.pendingCompletion).toBe(false);
    });

    it("complete on absent work -> no-op", async () => {
      const { workId } = await setupState({
        work: false,
        pendingStart: false,
        running: true,
        pendingCompletion: false,
        pendingCancelation: false,
      });
      // Should not throw
      await runComplete(workId, { kind: "success" }, 0);
      const s = await observeState(workId);
      expect(s.work).toBe(false);
      expect(s.pendingCompletion).toBe(false);
    });

    it("duplicate complete -> second one is no-op (attempt mismatch)", async () => {
      const { workId } = await setupState(S2_RUNNING_WITH_RETRIES);
      await runComplete(workId, { kind: "failed" }, 0);
      const s1 = await observeState(workId);
      assert(s1.work);
      expect(s1.work.attempts).toBe(1);

      // Second complete with attempt=0 -> mismatched, should be no-op
      await runComplete(workId, { kind: "failed" }, 0);
      const s2 = await observeState(workId);
      assert(s2.work);
      expect(s2.work.attempts).toBe(1); // unchanged
    });

    it("duplicate complete with correct attempt -> BUG: attempts incremented before dedup check", async () => {
      const { workId } = await setupState(S2_RUNNING_WITH_RETRIES);
      await runComplete(workId, { kind: "failed" }, 0);
      const s1 = await observeState(workId);
      assert(s1.work);
      expect(s1.work.attempts).toBe(1);

      // Second complete with attempt=1 -> pendingCompletion already exists.
      // BUG: complete.complete increments work.attempts BEFORE checking for
      // existing pendingCompletion. So attempts goes to 2 even though the
      // second complete was effectively a no-op for pendingCompletion.
      await runComplete(workId, { kind: "failed" }, 1);
      const s2 = await observeState(workId);
      assert(s2.work);
      expect(s2.work.attempts).toBe(2); // Should ideally be 1
    });
  });

  // =========================================================================
  // lib.cancel transitions
  // =========================================================================

  describe("lib.cancel transitions", () => {
    it("S1 enqueued + cancel -> adds pendingCancelation", async () => {
      const { workId } = await setupState(S1_ENQUEUED);
      await t.mutation(internal.lib.cancel, { id: workId });
      const s = await observeState(workId);
      assert(s.work);
      expect(s.pendingStart).toBe(true);
      expect(s.pendingCancelation).toBe(true);
    });

    it("S2 running + cancel -> adds pendingCancelation", async () => {
      const { workId } = await setupState(S2_RUNNING);
      await t.mutation(internal.lib.cancel, { id: workId });
      const s = await observeState(workId);
      assert(s.work);
      expect(s.running).toBe(true);
      expect(s.pendingCancelation).toBe(true);
    });

    it("S10 re-enqueued + cancel -> adds pendingCancelation", async () => {
      const { workId } = await setupState(S10_REENQUEUED);
      await t.mutation(internal.lib.cancel, { id: workId });
      const s = await observeState(workId);
      expect(s.pendingStart).toBe(true);
      expect(s.pendingCancelation).toBe(true);
    });

    it("absent work + cancel -> no-op", async () => {
      const { workId } = await setupState({
        work: false,
        pendingStart: false,
        running: false,
        pendingCompletion: false,
        pendingCancelation: false,
      });
      await t.mutation(internal.lib.cancel, { id: workId });
      const s = await observeState(workId);
      expect(s.pendingCancelation).toBe(false);
    });

    it("S7 already cancel-pending + cancel -> no-op (dedup)", async () => {
      const { workId } = await setupState(S7_CANCEL_PENDING);
      await t.mutation(internal.lib.cancel, { id: workId });
      const count = await t.run(async (ctx) => {
        const all = await ctx.db
          .query("pendingCancelation")
          .withIndex("workId", (q) => q.eq("workId", workId))
          .collect();
        return all.length;
      });
      expect(count).toBe(1);
    });
  });

  // =========================================================================
  // Recovery transitions
  // =========================================================================

  describe("recovery transitions", () => {
    it("work absent + running (old, no pendingCompletion) -> removed from running", async () => {
      const { workId } = await setupState(
        {
          work: false,
          pendingStart: false,
          running: true,
          pendingCompletion: false,
          pendingCancelation: false,
        },
        { oldForRecovery: true },
      );
      const recoverySeg = getCurrentSegment() + RECOVERY_PERIOD_SEGMENTS + 1n;
      await runMain(recoverySeg);
      const s = await observeState(workId);
      expect(s.running).toBe(false);
    });

    it("work absent + running (old) + pendingCompletion -> handleCompletions processes first", async () => {
      // handleCompletions runs before handleRecovery. Since work is absent and
      // the completion has retry=false, it will log an error but still remove
      // from running.
      const { workId } = await setupState(
        {
          work: false,
          pendingStart: false,
          running: true,
          pendingCompletion: { retry: false, resultKind: "success" },
          pendingCancelation: false,
        },
        { oldForRecovery: true },
      );
      const recoverySeg = getCurrentSegment() + RECOVERY_PERIOD_SEGMENTS + 1n;
      await runMain(recoverySeg);
      const s = await observeState(workId);
      expect(s.pendingCompletion).toBe(false);
      expect(s.running).toBe(false);
    });

    it("S2 running action (old, not yet completed) -> recovery checks scheduled function", async () => {
      // In convex-test the scheduled function runs immediately, so recovery
      // will likely see it as succeeded. The key thing is it doesn't crash.
      const { workId } = await setupState(S2_RUNNING, {
        oldForRecovery: true,
      });
      const recoverySeg = getCurrentSegment() + RECOVERY_PERIOD_SEGMENTS + 1n;
      // Should not throw
      await runMain(recoverySeg);
      const s = await observeState(workId);
      // Job should still be tracked (recovery saw function as completed/running)
      expect(s.running).toBe(true);
    });

    it("S12 canceled running (old) -> recovery doesn't crash on canceled work", async () => {
      const { workId } = await setupState(S12_CANCELED_AWAITING_COMPLETE, {
        oldForRecovery: true,
      });
      const recoverySeg = getCurrentSegment() + RECOVERY_PERIOD_SEGMENTS + 1n;
      await runMain(recoverySeg);
      const s = await observeState(workId);
      // Should still be in a valid state
      assert(s.work);
      expect(s.work.canceled).toBe(true);
    });
  });

  // =========================================================================
  // Invalid / undefined states
  // =========================================================================

  describe("invalid states", () => {
    it("work absent + pendingStart -> main handles gracefully (no throw)", async () => {
      const { workId, segment } = await setupState({
        work: false,
        pendingStart: true,
        running: false,
        pendingCompletion: false,
        pendingCancelation: false,
      });
      // After fix-missing-items, beginWork returns null instead of throwing
      await runMain(segment);
      const s = await observeState(workId);
      // pendingStart should be consumed
      expect(s.pendingStart).toBe(false);
      // work should still be absent
      expect(s.work).toBe(false);
      // should NOT be in running (beginWork returned null)
      expect(s.running).toBe(false);
    });

    it("pendingStart + running for same workId -> skips start but leaves pendingStart", async () => {
      const { workId, segment } = await setupState({
        work: { attempts: 0, hasRetryBehavior: false, fnType: "action" },
        pendingStart: true,
        running: true,
        pendingCompletion: false,
        pendingCancelation: false,
      });
      await runMain(segment);
      const s = await observeState(workId);
      expect(s.running).toBe(true);
      // BUG: handleStart skips the start but does NOT delete the pendingStart
      // entry (returns null before the delete call). This means the orphaned
      // pendingStart will be picked up again on the next main loop iteration.
      expect(s.pendingStart).toBe(true);
    });

    it("duplicate pendingCompletion via complete.complete -> BUG: attempts still incremented", async () => {
      const { workId } = await setupState(S2_RUNNING_WITH_RETRIES);
      await runComplete(workId, { kind: "failed" }, 0);
      // Now attempt=1, and there's already a pendingCompletion.
      // BUG: complete.complete increments attempts BEFORE checking for
      // existing pendingCompletion, so attempts goes to 2.
      await runComplete(workId, { kind: "failed" }, 1);
      const s = await observeState(workId);
      assert(s.work);
      expect(s.work.attempts).toBe(2); // Should ideally be 1
      // But only one pendingCompletion exists (dedup worked for that)
      const pcCount = await t.run(async (ctx) => {
        return (
          await ctx.db
            .query("pendingCompletion")
            .withIndex("workId", (q) => q.eq("workId", workId))
            .collect()
        ).length;
      });
      expect(pcCount).toBe(1);
    });
  });

  // =========================================================================
  // Multi-step / interleaved transitions
  // =========================================================================

  describe("multi-step / interleaved transitions", () => {
    it("completion + new enqueue -> main processes both in one pass", async () => {
      const seg = getNextSegment();
      const { workId: w1 } = await setupState(S1_ENQUEUED, { segment: seg });
      await runMain(seg);
      // w1 is now running
      await runComplete(w1, { kind: "success" }, 0);

      // Enqueue second job directly in DB
      const w2 = await t.run<Id<"work">>(async (ctx) => {
        const id = await ctx.db.insert("work", {
          fnType: "action",
          fnHandle: "test_handle",
          fnName: "test_fn2",
          fnArgs: {},
          attempts: 0,
        });
        await ctx.db.insert("pendingStart", { workId: id, segment: seg });
        return id;
      });

      // Run main again - should process completion of w1 AND start w2
      await runMain(seg);
      const s1 = await observeState(w1);
      const s2 = await observeState(w2);

      expect(s1.work).toBe(false);
      expect(s1.running).toBe(false);
      expect(s2.running).toBe(true);
    });

    it("two jobs complete(retry) before main -> main retries both", async () => {
      const seg = getNextSegment();
      const ids = await t.run(async (ctx) => {
        const w1 = await ctx.db.insert("work", {
          fnType: "action",
          fnHandle: "h1",
          fnName: "fn1",
          fnArgs: {},
          attempts: 0,
          retryBehavior: { maxAttempts: 5, initialBackoffMs: 100, base: 2 },
        });
        const w2 = await ctx.db.insert("work", {
          fnType: "action",
          fnHandle: "h2",
          fnName: "fn2",
          fnArgs: {},
          attempts: 0,
          retryBehavior: { maxAttempts: 5, initialBackoffMs: 100, base: 2 },
        });
        const s1 = await ctx.scheduler.runAfter(
          0,
          internal.worker.runActionWrapper,
          {
            workId: w1,
            fnHandle: "h1",
            fnArgs: {},
            logLevel: "WARN",
            attempt: 0,
          },
        );
        const s2 = await ctx.scheduler.runAfter(
          0,
          internal.worker.runActionWrapper,
          {
            workId: w2,
            fnHandle: "h2",
            fnArgs: {},
            logLevel: "WARN",
            attempt: 0,
          },
        );
        await ctx.db.insert("internalState", {
          generation,
          segmentCursors: { incoming: 0n, completion: 0n, cancelation: 0n },
          lastRecovery: getCurrentSegment(),
          report: {
            completed: 0,
            succeeded: 0,
            failed: 0,
            retries: 0,
            canceled: 0,
            conflicted: 0,
            lastReportTs: Date.now(),
          },
          running: [
            { workId: w1, scheduledId: s1, started: Date.now() },
            { workId: w2, scheduledId: s2, started: Date.now() },
          ],
        });
        await ctx.db.insert("runStatus", { state: { kind: "running" } });
        return { w1, w2 };
      });

      // Both complete with failure before main runs
      await runComplete(ids.w1, { kind: "failed" }, 0);
      await runComplete(ids.w2, { kind: "failed" }, 0);

      const s1Before = await observeState(ids.w1);
      const s2Before = await observeState(ids.w2);
      assert(s1Before.pendingCompletion);
      assert(s2Before.pendingCompletion);
      expect(s1Before.pendingCompletion.retry).toBe(true);
      expect(s2Before.pendingCompletion.retry).toBe(true);

      // Now main processes both completions
      await runMain(seg);
      const s1After = await observeState(ids.w1);
      const s2After = await observeState(ids.w2);

      // Both pendingCompletions should be consumed
      expect(s1After.pendingCompletion).toBe(false);
      expect(s2After.pendingCompletion).toBe(false);
      // With small backoff (100ms), retry pendingStarts are immediately picked up
      // by handleStart in the same main pass, so both jobs are running again
      expect(s1After.running).toBe(true);
      expect(s2After.running).toBe(true);
      expect(s1After.pendingStart).toBe(false);
      expect(s2After.pendingStart).toBe(false);
      assert(s1After.work);
      assert(s2After.work);
      expect(s1After.work.attempts).toBe(1);
      expect(s2After.work.attempts).toBe(1);
    });

    it("cancel arrives while retry completion is pending -> cancel wins", async () => {
      const { workId, segment } = await setupState(S5_COMPLETING_WILL_RETRY);

      // Cancel arrives before main processes the completion
      await t.run(async (ctx) => {
        await ctx.db.insert("pendingCancelation", {
          workId,
          segment,
        });
      });

      await runMain(segment);
      const s = await observeState(workId);
      expect(s.pendingStart).toBe(false);
      expect(s.running).toBe(false);
      expect(s.pendingCompletion).toBe(false);
    });

    it("multiple cancels for same work -> BUG: crashes with double delete", async () => {
      const seg = getNextSegment();
      const workId = await t.run<Id<"work">>(async (ctx) => {
        const wId = await ctx.db.insert("work", {
          fnType: "action",
          fnHandle: "h",
          fnName: "fn",
          fnArgs: {},
          attempts: 0,
        });
        await ctx.db.insert("pendingStart", { workId: wId, segment: seg });
        await ctx.db.insert("pendingCancelation", {
          workId: wId,
          segment: seg,
        });
        await ctx.db.insert("pendingCancelation", {
          workId: wId,
          segment: seg,
        });

        await ctx.db.insert("internalState", {
          generation,
          segmentCursors: { incoming: 0n, completion: 0n, cancelation: 0n },
          lastRecovery: getCurrentSegment(),
          report: {
            completed: 0,
            succeeded: 0,
            failed: 0,
            retries: 0,
            canceled: 0,
            conflicted: 0,
            lastReportTs: Date.now(),
          },
          running: [],
        });
        await ctx.db.insert("runStatus", { state: { kind: "running" } });
        return wId;
      });

      // BUG: handleCancelation processes duplicate pendingCancelation entries
      // in parallel. Both find the same pendingStart and try to delete it,
      // causing a "Delete on non-existent doc" crash.
      await expect(runMain(seg)).rejects.toThrow();
    });

    it("complete(success) + cancel interleaved -> cancel is no-op (work already gone)", async () => {
      const { workId, segment } = await setupState(S2_RUNNING);
      await runComplete(workId, { kind: "success" }, 0);

      // Cancel after work was already deleted by complete
      await t.mutation(internal.lib.cancel, { id: workId });

      await runMain(segment);
      const s = await observeState(workId);
      expect(s.work).toBe(false);
      expect(s.running).toBe(false);
    });

    it("retry with large backoff -> pendingStart not picked up in same main pass", async () => {
      const seg = getNextSegment();
      const workId = await t.run<Id<"work">>(async (ctx) => {
        const wId = await ctx.db.insert("work", {
          fnType: "action",
          fnHandle: "h",
          fnName: "fn",
          fnArgs: {},
          attempts: 1,
          retryBehavior: { maxAttempts: 5, initialBackoffMs: 60000, base: 2 },
        });
        const sId = await ctx.scheduler.runAfter(
          0,
          internal.worker.runActionWrapper,
          {
            workId: wId,
            fnHandle: "h",
            fnArgs: {},
            logLevel: "WARN",
            attempt: 0,
          },
        );
        await ctx.db.insert("internalState", {
          generation,
          segmentCursors: { incoming: 0n, completion: 0n, cancelation: 0n },
          lastRecovery: getCurrentSegment(),
          report: {
            completed: 0,
            succeeded: 0,
            failed: 0,
            retries: 0,
            canceled: 0,
            conflicted: 0,
            lastReportTs: Date.now(),
          },
          running: [{ workId: wId, scheduledId: sId, started: Date.now() }],
        });
        await ctx.db.insert("runStatus", { state: { kind: "running" } });
        await ctx.db.insert("pendingCompletion", {
          workId: wId,
          segment: seg,
          retry: true,
          runResult: { kind: "failed", error: "test" },
        });
        return wId;
      });

      await runMain(seg);
      const s = await observeState(workId);
      expect(s.pendingCompletion).toBe(false);
      expect(s.running).toBe(false);
      // Re-enqueued with future segment, not picked up
      expect(s.pendingStart).toBe(true);
      assert(s.work);
    });

    it("main crash recovery: state recoverable after restart", async () => {
      const seg = getNextSegment();
      const { workId } = await setupState(S1_ENQUEUED, { segment: seg });

      await runMain(seg);
      expect((await observeState(workId)).running).toBe(true);

      await runComplete(workId, { kind: "success" }, 0);

      // New main picks up completion
      const seg2 = getNextSegment();
      await runMain(seg2);
      const s = await observeState(workId);
      expect(s.work).toBe(false);
      expect(s.running).toBe(false);
    });

    it("all three pending queues populated -> main processes all in order", async () => {
      const seg = getNextSegment();
      const ids = await t.run(async (ctx) => {
        // Job 1: in running, has pendingCompletion
        const w1 = await ctx.db.insert("work", {
          fnType: "action",
          fnHandle: "h1",
          fnName: "fn1",
          fnArgs: {},
          attempts: 0,
        });
        const s1 = await ctx.scheduler.runAfter(
          0,
          internal.worker.runActionWrapper,
          {
            workId: w1,
            fnHandle: "h1",
            fnArgs: {},
            logLevel: "WARN",
            attempt: 0,
          },
        );

        // Job 2: has pendingStart + pendingCancelation
        const w2 = await ctx.db.insert("work", {
          fnType: "action",
          fnHandle: "h2",
          fnName: "fn2",
          fnArgs: {},
          attempts: 0,
        });

        // Job 3: has pendingStart only
        const w3 = await ctx.db.insert("work", {
          fnType: "action",
          fnHandle: "h3",
          fnName: "fn3",
          fnArgs: {},
          attempts: 0,
        });

        await ctx.db.insert("internalState", {
          generation,
          segmentCursors: { incoming: 0n, completion: 0n, cancelation: 0n },
          lastRecovery: getCurrentSegment(),
          report: {
            completed: 0,
            succeeded: 0,
            failed: 0,
            retries: 0,
            canceled: 0,
            conflicted: 0,
            lastReportTs: Date.now(),
          },
          running: [{ workId: w1, scheduledId: s1, started: Date.now() }],
        });
        await ctx.db.insert("runStatus", { state: { kind: "running" } });

        await ctx.db.insert("pendingCompletion", {
          workId: w1,
          segment: seg,
          retry: false,
          runResult: { kind: "success", returnValue: null },
        });
        await ctx.db.insert("pendingStart", { workId: w2, segment: seg });
        await ctx.db.insert("pendingCancelation", {
          workId: w2,
          segment: seg,
        });
        await ctx.db.insert("pendingStart", { workId: w3, segment: seg });

        return { w1, w2, w3 };
      });

      await runMain(seg);

      const s1 = await observeState(ids.w1);
      const s2 = await observeState(ids.w2);
      const s3 = await observeState(ids.w3);

      // w1: pendingCompletion(retry=false) processed, removed from running.
      // Note: handleCompletions does NOT delete work - that's done by complete.complete.
      // Since we set up the state directly (not via complete), work still exists.
      expect(s1.running).toBe(false);
      expect(s1.pendingCompletion).toBe(false);

      // w2: enqueued + canceled -> canceled
      expect(s2.pendingStart).toBe(false);
      expect(s2.pendingCancelation).toBe(false);
      assert(s2.work);
      expect(s2.work.canceled).toBe(true);

      // w3: enqueued -> running
      expect(s3.running).toBe(true);
      expect(s3.pendingStart).toBe(false);
    });

    it("cancel during S5 (completing+retry) with pendingCancelation still present -> no retry", async () => {
      // Like S9 but the pendingCancelation hasn't been processed yet
      const seg = getNextSegment();
      const workId = await t.run<Id<"work">>(async (ctx) => {
        const wId = await ctx.db.insert("work", {
          fnType: "action",
          fnHandle: "h",
          fnName: "fn",
          fnArgs: {},
          attempts: 1,
          retryBehavior: { maxAttempts: 5, initialBackoffMs: 100, base: 2 },
        });
        const sId = await ctx.scheduler.runAfter(
          0,
          internal.worker.runActionWrapper,
          {
            workId: wId,
            fnHandle: "h",
            fnArgs: {},
            logLevel: "WARN",
            attempt: 0,
          },
        );
        await ctx.db.insert("internalState", {
          generation,
          segmentCursors: { incoming: 0n, completion: 0n, cancelation: 0n },
          lastRecovery: getCurrentSegment(),
          report: {
            completed: 0,
            succeeded: 0,
            failed: 0,
            retries: 0,
            canceled: 0,
            conflicted: 0,
            lastReportTs: Date.now(),
          },
          running: [{ workId: wId, scheduledId: sId, started: Date.now() }],
        });
        await ctx.db.insert("runStatus", { state: { kind: "running" } });

        // Pending retry + pending cancel at the same time
        await ctx.db.insert("pendingCompletion", {
          workId: wId,
          segment: seg,
          retry: true,
          runResult: { kind: "failed", error: "test" },
        });
        await ctx.db.insert("pendingCancelation", {
          workId: wId,
          segment: seg,
        });

        return wId;
      });

      await runMain(seg);
      const s = await observeState(workId);
      // Cancel should win: no retry, no pendingStart
      expect(s.pendingStart).toBe(false);
      expect(s.running).toBe(false);
      expect(s.pendingCompletion).toBe(false);
      expect(s.pendingCancelation).toBe(false);
    });
  });
});
