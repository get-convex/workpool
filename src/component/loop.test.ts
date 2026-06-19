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
import { api, internal } from "./_generated/api.js";
import type { Doc, Id } from "./_generated/dataModel.js";
import { DEFAULT_MAX_PARALLELISM, getCurrentSegment } from "./shared.js";
import { WORKER_NAME } from "./shared.js";
import { setupTest } from "./setup.test.js";

const SECOND = 1000;
const MINUTE = 60 * SECOND;

/**
 * Behavior tests for the main loop, now driven by @convex-dev/batch-worker.
 * Designed around what an external observer can see:
 *
 *   - api.lib.status     — public-facing state of a single work item
 *   - pending* tables    — work in flight that the loop will process
 *   - state.running      — slots currently occupied by workers
 *
 * The loop's lifecycle (running/idle/scheduled), generation guard, and
 * liveness recovery are owned by batch-worker and are NOT asserted here.
 *
 * Setup conventions:
 *   - vi.useFakeTimers() so time advances deterministically
 *   - The loop is driven manually via runLoop(): query `getBatch`, and if it
 *     returns work, run the `main` worker mutation with that batch. This is
 *     exactly what batch-worker does for us in production.
 *   - simulateCompletion() pretends a worker finished its job by calling
 *     internal.complete.complete (how production gets work into
 *     pendingCompletion), so it's the correct seam for testing.
 */
describe("loop", () => {
  let t: ReturnType<typeof setupTest>;

  beforeEach(async () => {
    vi.useFakeTimers();
    t = setupTest();
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

  // ── helpers ──────────────────────────────────────────────────────────

  /** Seed an empty internalState singleton. */
  async function initialize(opts: { maxParallelism?: number } = {}) {
    if (opts.maxParallelism !== undefined) {
      await t.run(async (ctx) => {
        const g = await ctx.db.query("globals").unique();
        assert(g);
        await ctx.db.patch("globals", g._id, {
          maxParallelism: opts.maxParallelism!,
        });
      });
    }
    await t.run(async (ctx) => {
      await ctx.db.insert("internalState", {
        generation: 0n,
        segmentCursors: { incoming: 0n, completion: 0n, cancelation: 0n },
        lastRecovery: 0n,
        report: {
          completed: 0,
          succeeded: 0,
          failed: 0,
          retries: 0,
          canceled: 0,
          lastReportTs: Date.now(),
        },
        running: [],
      });
    });
  }

  /**
   * Insert a work doc + pendingStart at the given segment (default: now).
   * Bypasses the public enqueue API to keep tests focused on the loop.
   */
  async function enqueueWork(
    overrides: Partial<WithoutSystemFields<Doc<"work">>> = {},
    segment = getCurrentSegment(),
  ): Promise<Id<"work">> {
    return t.run(async (ctx) => {
      const workId = await ctx.db.insert("work", {
        fnType: "action",
        fnHandle: "test_handle",
        fnName: "test_handle",
        fnArgs: {},
        attempts: 0,
        ...overrides,
      });
      await ctx.db.insert("pendingStart", { workId, segment });
      return workId;
    });
  }

  /**
   * Drive one loop iteration the way batch-worker does: get the next batch,
   * and if there's work, run the worker mutation with it. Returns the batch
   * result so tests can inspect the idle/work decision.
   */
  async function runLoop() {
    const result = await t.query(internal.loop.getBatch, {
      name: WORKER_NAME,
    });
    if (result.kind === "work") {
      await t.mutation(internal.loop.run, result.batch);
    }
    return result;
  }

  /** Pretend a worker finished a job by inserting pendingCompletion. */
  async function simulateCompletion(
    workId: Id<"work">,
    result:
      | { kind: "success"; returnValue: unknown }
      | { kind: "failed"; error: string }
      | { kind: "canceled" },
    attempt = 0,
  ) {
    await t.mutation(internal.complete.complete, {
      jobs: [{ workId, runResult: result, attempt }],
    });
  }

  /** Snapshot of everything an outside observer might check. */
  async function observe() {
    return t.run(async (ctx) => {
      const state = await ctx.db.query("internalState").unique();
      const pendingStart = await ctx.db.query("pendingStart").collect();
      const pendingCompletion = await ctx.db
        .query("pendingCompletion")
        .collect();
      const pendingCancelation = await ctx.db
        .query("pendingCancelation")
        .collect();
      return {
        running: state?.running ?? [],
        lastRecovery: state?.lastRecovery ?? 0n,
        pendingStart,
        pendingCompletion,
        pendingCancelation,
      };
    });
  }

  async function statusOf(workId: Id<"work">) {
    return t.query(api.lib.status, { id: workId });
  }

  // ────────────────────────────────────────────────────────────────────
  // Forward progress: work moves through the pipeline
  // ────────────────────────────────────────────────────────────────────

  describe("forward progress", () => {
    it("starts a pending work item when the loop runs", async () => {
      await initialize();
      const workId = await enqueueWork();

      await runLoop();

      const o = await observe();
      expect(o.pendingStart).toHaveLength(0);
      expect(o.running.map((r) => r.workId)).toEqual([workId]);
      expect(await statusOf(workId)).toMatchObject({ state: "running" });
    });

    it("removes work from running once a successful completion is processed", async () => {
      await initialize();
      const workId = await enqueueWork();
      await runLoop();

      await simulateCompletion(
        workId,
        { kind: "success", returnValue: null },
        0,
      );
      await runLoop();

      const o = await observe();
      expect(o.running).toHaveLength(0);
      expect(o.pendingCompletion).toHaveLength(0);
      // Work doc deleted → status reports "finished".
      expect(await statusOf(workId)).toMatchObject({ state: "finished" });
    });

    it("treats a final failure (no retry policy) as terminal", async () => {
      await initialize();
      const workId = await enqueueWork();
      await runLoop();

      await simulateCompletion(workId, { kind: "failed", error: "boom" }, 0);
      await runLoop();

      const o = await observe();
      expect(o.running).toHaveLength(0);
      expect(await statusOf(workId)).toMatchObject({ state: "finished" });
    });

    it("processes multiple work items concurrently within capacity", async () => {
      await initialize({ maxParallelism: 5 });
      const ids = [];
      for (let i = 0; i < 3; i++) ids.push(await enqueueWork());

      await runLoop();

      const o = await observe();
      expect(o.running).toHaveLength(3);
      expect(new Set(o.running.map((r) => r.workId))).toEqual(new Set(ids));
    });
  });

  // ────────────────────────────────────────────────────────────────────
  // Capacity: maxParallelism is respected
  // ────────────────────────────────────────────────────────────────────

  describe("capacity", () => {
    it("never starts more than maxParallelism in one iteration", async () => {
      await initialize({ maxParallelism: 3 });
      for (let i = 0; i < 7; i++) await enqueueWork();

      await runLoop();

      const o = await observe();
      expect(o.running).toHaveLength(3);
      expect(o.pendingStart).toHaveLength(7 - 3);
    });

    it("picks up overflow on subsequent iterations as slots free", async () => {
      await initialize({ maxParallelism: 2 });
      const ids = [];
      for (let i = 0; i < 4; i++) ids.push(await enqueueWork());

      await runLoop();
      let o = await observe();
      expect(o.running).toHaveLength(2);
      expect(o.pendingStart).toHaveLength(2);

      // Complete one running job; another should take its place.
      const finished = o.running[0].workId;
      await simulateCompletion(
        finished,
        { kind: "success", returnValue: null },
        0,
      );
      await runLoop();

      o = await observe();
      expect(o.running).toHaveLength(2);
      expect(o.pendingStart).toHaveLength(1);
      // The completed one is gone.
      expect(o.running.map((r) => r.workId)).not.toContain(finished);
    });

    it("does not start new work when running.length already exceeds maxParallelism", async () => {
      // Edge case: maxParallelism was lowered while jobs were running.
      await initialize({ maxParallelism: 2 });
      // Pre-populate state.running with 4 entries.
      const runningIds: {
        workId: Id<"work">;
        scheduledId: Id<"_scheduled_functions">;
      }[] = [];
      for (let i = 0; i < 4; i++) {
        const workId = await t.run(async (ctx) => {
          return ctx.db.insert("work", {
            fnType: "action",
            fnHandle: "h",
            fnName: "h",
            fnArgs: {},
            attempts: 0,
          });
        });
        const scheduledId = await t.run(async (ctx) => {
          return ctx.scheduler.runAfter(0, internal.worker.runActionWrapper, {
            workId,
            fnHandle: "h",
            fnArgs: {},
            logLevel: "WARN",
            attempt: 0,
          });
        });
        runningIds.push({ workId, scheduledId });
      }
      await t.run(async (ctx) => {
        const s = await ctx.db.query("internalState").unique();
        assert(s);
        await ctx.db.patch("internalState", s._id, {
          running: runningIds.map((r) => ({
            ...r,
            started: Date.now(),
          })),
        });
      });
      // New pending work arrives while we're already over capacity.
      await enqueueWork();

      await runLoop();

      const o = await observe();
      // No new starts — already over capacity.
      expect(o.running).toHaveLength(4);
      expect(o.pendingStart).toHaveLength(1);
    });
  });

  // ────────────────────────────────────────────────────────────────────
  // Retry: failed work is retried per the retry policy
  // ────────────────────────────────────────────────────────────────────

  describe("retry", () => {
    it("re-enqueues a failed job that has a retry policy with attempts left", async () => {
      await initialize();
      const workId = await enqueueWork({
        retryBehavior: {
          maxAttempts: 3,
          initialBackoffMs: 100,
          base: 2,
        },
      });
      await runLoop();

      // Worker reports failure on first attempt.
      await simulateCompletion(workId, { kind: "failed", error: "boom" }, 0);
      await runLoop();

      // Work doc still exists; pendingStart was re-inserted with backoff segment.
      const o = await observe();
      expect(o.pendingStart).toHaveLength(1);
      expect(o.pendingStart[0].workId).toBe(workId);
      expect(await statusOf(workId)).toMatchObject({
        state: "pending",
        previousAttempts: 1,
      });
    });

    it("does NOT re-enqueue a failed job that was canceled before retry processed", async () => {
      await initialize();
      const workId = await enqueueWork({
        retryBehavior: {
          maxAttempts: 3,
          initialBackoffMs: 100,
          base: 2,
        },
      });
      await runLoop();

      // Worker reports failure (would normally retry).
      await simulateCompletion(workId, { kind: "failed", error: "boom" }, 0);
      // Cancel arrives before main can process the retry.
      await t.mutation(api.lib.cancel, { id: workId });

      await runLoop();

      const o = await observe();
      // Loop's direct effect: no retry was queued, work is marked canceled.
      // (A follow-up `complete` mutation is scheduled to finalize the work
      // doc deletion — that's complete.ts's responsibility, not the loop's.)
      expect(o.pendingStart).toHaveLength(0);
      const work = await t.run(async (ctx) => ctx.db.get("work", workId));
      expect(work?.canceled).toBe(true);
    });
  });

  // ────────────────────────────────────────────────────────────────────
  // Cancellation
  // ────────────────────────────────────────────────────────────────────

  describe("cancellation", () => {
    it("removes a pendingStart cancellation before the work runs", async () => {
      await initialize();
      const workId = await enqueueWork();
      await t.mutation(api.lib.cancel, { id: workId });

      await runLoop();

      const o = await observe();
      expect(o.pendingStart).toHaveLength(0);
      expect(o.running).toHaveLength(0);
      // Work is marked canceled by the loop. Final deletion happens when
      // the scheduled `complete` mutation runs (separate concern).
      const work = await t.run(async (ctx) => ctx.db.get("work", workId));
      expect(work?.canceled).toBe(true);
    });

    it("marks an already-running work as canceled", async () => {
      await initialize();
      const workId = await enqueueWork();
      await runLoop(); // start it
      expect((await observe()).running).toHaveLength(1);

      await t.mutation(api.lib.cancel, { id: workId });
      await runLoop(); // process the cancellation

      const work = await t.run(async (ctx) => ctx.db.get("work", workId));
      expect(work?.canceled).toBe(true);
    });

    it("is a graceful no-op for already-finished work", async () => {
      await initialize();
      const workId = await enqueueWork();
      await runLoop();
      await simulateCompletion(
        workId,
        { kind: "success", returnValue: null },
        0,
      );
      await runLoop();

      // Work doc already gone — cancel should not throw.
      await t.mutation(api.lib.cancel, { id: workId });
      const o = await observe();
      expect(o.pendingCancelation).toHaveLength(0);
    });
  });

  // ────────────────────────────────────────────────────────────────────
  // getBatch: the work-vs-idle decision and its scheduling hints
  // ────────────────────────────────────────────────────────────────────

  describe("getBatch", () => {
    it("returns idle when there's nothing to do", async () => {
      await initialize();
      const result = await t.query(internal.loop.getBatch, {
        name: WORKER_NAME,
      });
      expect(result.kind).toBe("idle");
      // Nothing running and no future work → no wake-up hint.
      if (result.kind === "idle") {
        expect(result.timeoutMs).toBeUndefined();
      }
    });

    it("returns a work batch when a pending start is ready", async () => {
      await initialize();
      const workId = await enqueueWork();
      const result = await t.query(internal.loop.getBatch, {
        name: WORKER_NAME,
      });
      assert(result.kind === "work");
      expect(result.batch.starts.map((s) => s.workId)).toEqual([workId]);
      expect(result.batch.recovery).toBe(false);
    });

    it("idles with a timeoutMs when only future-scheduled work remains", async () => {
      await initialize();
      const future = getCurrentSegment() + 1000n; // 100s out
      await enqueueWork({}, future);

      const result = await t.query(internal.loop.getBatch, {
        name: WORKER_NAME,
      });
      assert(result.kind === "idle");
      expect(result.timeoutMs).toBeGreaterThan(0);
    });
  });

  // ────────────────────────────────────────────────────────────────────
  // Recovery: stuck running jobs get cleaned up
  // ────────────────────────────────────────────────────────────────────

  describe("recovery", () => {
    it("flags a recovery iteration and advances lastRecovery for silent workers", async () => {
      await initialize();
      // Pre-populate state.running with an old entry.
      const workId = await t.run(async (ctx) => {
        const wid = await ctx.db.insert("work", {
          fnType: "action",
          fnHandle: "h",
          fnName: "h",
          fnArgs: {},
          attempts: 0,
        });
        const scheduledId = await ctx.scheduler.runAfter(
          0,
          internal.worker.runActionWrapper,
          {
            workId: wid,
            fnHandle: "h",
            fnArgs: {},
            logLevel: "WARN",
            attempt: 0,
          },
        );
        const s = await ctx.db.query("internalState").unique();
        assert(s);
        await ctx.db.patch("internalState", s._id, {
          running: [
            {
              workId: wid,
              scheduledId,
              // Started 10 minutes ago — past 5-minute recovery threshold.
              started: Date.now() - 10 * MINUTE,
            },
          ],
          // Force recovery to be eligible to run this iteration.
          lastRecovery: 0n,
        });
        return wid;
      });

      // getBatch should flag this as a recovery iteration.
      const batchResult = await t.query(internal.loop.getBatch, {
        name: WORKER_NAME,
      });
      assert(batchResult.kind === "work");
      expect(batchResult.batch.recovery).toBe(true);

      await runLoop();

      const after = await observe();
      // lastRecovery advanced past 0.
      expect(after.lastRecovery).toBeGreaterThan(0n);
      // Work is still in running (recovery removes it via complete, which
      // happens in a separately-scheduled mutation).
      expect(after.running.map((r) => r.workId)).toContain(workId);
    });
  });

  // ────────────────────────────────────────────────────────────────────
  // Backwards compatibility with the pre-migration API
  // ────────────────────────────────────────────────────────────────────

  describe("backwards compatibility", () => {
    it("internal.loop.main forwarder resumes the loop without error", async () => {
      await initialize();
      const workId = await enqueueWork();

      // A pre-migration scheduled call lands here after deploy. It just pings
      // batch-worker (no-op in tests) and must not throw.
      await t.mutation(internal.loop.main, { generation: 1n, segment: 123n });

      // Work is still pending; pumping the loop processes it as usual.
      await runLoop();
      expect((await observe()).running.map((r) => r.workId)).toEqual([workId]);
    });

    it("internal.loop.updateRunStatus forwarder does not throw", async () => {
      await initialize();
      await t.mutation(internal.loop.updateRunStatus, {
        generation: 1n,
        segment: 123n,
      });
    });
  });
});
