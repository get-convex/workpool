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
import { api, internal } from "./_generated/api.js";
import type { Doc, Id } from "./_generated/dataModel.js";
import schema from "./schema.js";
import { DEFAULT_MAX_PARALLELISM, getCurrentSegment } from "./shared.js";
import { STATUS_COOLDOWN } from "./loop.js";

const modules = import.meta.glob("./**/*.ts");
const SECOND = 1000;
const MINUTE = 60 * SECOND;

/**
 * Behavior tests for the main loop, designed from first principles around
 * what an external observer can see:
 *
 *   - api.lib.status     — public-facing state of a single work item
 *   - runStatus.state    — loop lifecycle (running / scheduled / idle)
 *   - pending* tables    — work in flight that the loop will process
 *   - state.running      — slots currently occupied by workers
 *
 * These tests do NOT assert on implementation specifics like cursor
 * positions, segment values, or which scheduler call was made — those
 * change when the loop's internals change, and they're not the contract.
 *
 * Setup conventions:
 *   - vi.useFakeTimers() so time advances deterministically
 *   - The loop is driven manually via runMain(); convex-test doesn't
 *     auto-flush scheduled functions
 *   - simulateCompletion() pretends a worker finished its job by
 *     calling internal.complete.complete; this is how production gets
 *     work into pendingCompletion, so it's the correct seam for testing
 */
describe("loop", () => {
  async function setupTest() {
    const t = convexTest(schema, modules);
    await t.run(async (ctx) => {
      await ctx.db.insert("globals", {
        logLevel: "WARN",
        maxParallelism: DEFAULT_MAX_PARALLELISM,
      });
    });
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

  // ── helpers ──────────────────────────────────────────────────────────

  /** Seed an empty running loop: internalState + runStatus=running. */
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
        generation: 1n,
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
      await ctx.db.insert("runStatus", { state: { kind: "running" } });
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

  /** Drive the main loop one iteration with the current generation. */
  async function runMain() {
    const generation = await t.run(async (ctx) => {
      const s = await ctx.db.query("internalState").unique();
      return s?.generation ?? 0n;
    });
    await t.mutation(internal.loop.main, { generation });
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
      const runStatus = await ctx.db.query("runStatus").unique();
      const pendingStart = await ctx.db.query("pendingStart").collect();
      const pendingCompletion = await ctx.db
        .query("pendingCompletion")
        .collect();
      const pendingCancelation = await ctx.db
        .query("pendingCancelation")
        .collect();
      return {
        running: state?.running ?? [],
        generation: state?.generation ?? 0n,
        runStatus: runStatus?.state,
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
    it("starts a pending work item when main runs", async () => {
      await initialize();
      const workId = await enqueueWork();

      await runMain();

      const o = await observe();
      expect(o.pendingStart).toHaveLength(0);
      expect(o.running.map((r) => r.workId)).toEqual([workId]);
      expect(await statusOf(workId)).toMatchObject({ state: "running" });
    });

    it("removes work from running once a successful completion is processed", async () => {
      await initialize();
      const workId = await enqueueWork();
      await runMain();

      await simulateCompletion(
        workId,
        { kind: "success", returnValue: null },
        0,
      );
      await runMain();

      const o = await observe();
      expect(o.running).toHaveLength(0);
      expect(o.pendingCompletion).toHaveLength(0);
      // Work doc deleted → status reports "finished".
      expect(await statusOf(workId)).toMatchObject({ state: "finished" });
    });

    it("treats a final failure (no retry policy) as terminal", async () => {
      await initialize();
      const workId = await enqueueWork();
      await runMain();

      await simulateCompletion(workId, { kind: "failed", error: "boom" }, 0);
      await runMain();

      const o = await observe();
      expect(o.running).toHaveLength(0);
      expect(await statusOf(workId)).toMatchObject({ state: "finished" });
    });

    it("processes multiple work items concurrently within capacity", async () => {
      await initialize({ maxParallelism: 5 });
      const ids = [];
      for (let i = 0; i < 3; i++) ids.push(await enqueueWork());

      await runMain();

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

      await runMain();

      const o = await observe();
      expect(o.running).toHaveLength(3);
      expect(o.pendingStart).toHaveLength(7 - 3);
    });

    it("picks up overflow on subsequent iterations as slots free", async () => {
      await initialize({ maxParallelism: 2 });
      const ids = [];
      for (let i = 0; i < 4; i++) ids.push(await enqueueWork());

      await runMain();
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
      await runMain();

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

      await runMain();

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
      await runMain();

      // Worker reports failure on first attempt.
      await simulateCompletion(workId, { kind: "failed", error: "boom" }, 0);
      await runMain();

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
      await runMain();

      // Worker reports failure (would normally retry).
      await simulateCompletion(workId, { kind: "failed", error: "boom" }, 0);
      // Cancel arrives before main can process the retry.
      await t.mutation(api.lib.cancel, { id: workId });

      await runMain();

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

      await runMain();

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
      await runMain(); // start it
      expect((await observe()).running).toHaveLength(1);

      await t.mutation(api.lib.cancel, { id: workId });
      await runMain(); // process the cancellation

      const work = await t.run(async (ctx) => ctx.db.get("work", workId));
      expect(work?.canceled).toBe(true);
    });

    it("is a graceful no-op for already-finished work", async () => {
      await initialize();
      const workId = await enqueueWork();
      await runMain();
      await simulateCompletion(
        workId,
        { kind: "success", returnValue: null },
        0,
      );
      await runMain();

      // Work doc already gone — cancel should not throw.
      await t.mutation(api.lib.cancel, { id: workId });
      const o = await observe();
      expect(o.pendingCancelation).toHaveLength(0);
    });
  });

  // ────────────────────────────────────────────────────────────────────
  // Lifecycle: runStatus transitions
  // ────────────────────────────────────────────────────────────────────

  describe("lifecycle", () => {
    it("transitions running -> idle when there's nothing to do (past cooldown)", async () => {
      await initialize();
      // No pending work, cursors at 0 → far in the past, past cooldown.
      vi.setSystemTime(Date.now() + STATUS_COOLDOWN + SECOND);

      await runMain();

      expect((await observe()).runStatus).toMatchObject({ kind: "idle" });
    });

    it("stays running during the cooldown window", async () => {
      await initialize();
      const workId = await enqueueWork();
      await runMain(); // process the work; cursors advance to ~now

      // Complete it so there's no work in flight.
      await simulateCompletion(
        workId,
        { kind: "success", returnValue: null },
        0,
      );
      await runMain(); // process completion; cursors at ~now

      // Within cooldown — should stay running.
      const o = await observe();
      expect(o.runStatus).toMatchObject({ kind: "running" });
    });

    it("transitions to scheduled (saturated=false) when only future-scheduled work remains", async () => {
      await initialize();
      // A retry-style pendingStart in the future.
      const future = getCurrentSegment() + 1000n;
      await enqueueWork({}, future);
      // Cursors at 0 → past cooldown, so we're not held in cooldown.

      await runMain();

      const o = await observe();
      expect(o.runStatus?.kind).toBe("scheduled");
      if (o.runStatus?.kind === "scheduled") {
        expect(o.runStatus.segment).toBeLessThanOrEqual(future);
        // No running jobs; capacity isn't full → saturated must be false.
        expect(o.runStatus.saturated).toBe(false);
      }
    });
  });

  // ────────────────────────────────────────────────────────────────────
  // Saturated state: scheduled with running.length == maxParallelism
  // The flag changes how kickMainLoop behaves (no enqueue-kicks; yes
  // completion-kicks).
  // ────────────────────────────────────────────────────────────────────

  describe("saturated", () => {
    /**
     * Pre-populate state.running with N entries, each backed by a real work
     * doc + scheduled worker (so recovery checks don't fire). Useful for
     * exercising main when the loop is already at-capacity.
     */
    async function fillRunningTo(count: number): Promise<Id<"work">[]> {
      const ids: Id<"work">[] = [];
      const entries: {
        workId: Id<"work">;
        scheduledId: Id<"_scheduled_functions">;
        started: number;
      }[] = [];
      for (let i = 0; i < count; i++) {
        const workId = await t.run(async (ctx) =>
          ctx.db.insert("work", {
            fnType: "action",
            fnHandle: "test_handle",
            fnName: "test_handle",
            fnArgs: {},
            attempts: 0,
          }),
        );
        const scheduledId = await t.run(async (ctx) =>
          ctx.scheduler.runAfter(0, internal.worker.runActionWrapper, {
            workId,
            fnHandle: "test_handle",
            fnArgs: {},
            logLevel: "WARN",
            attempt: 0,
          }),
        );
        ids.push(workId);
        entries.push({ workId, scheduledId, started: Date.now() });
      }
      await t.run(async (ctx) => {
        const s = await ctx.db.query("internalState").unique();
        assert(s);
        await ctx.db.patch("internalState", s._id, { running: entries });
      });
      return ids;
    }

    it("records saturated=true when transitioning to scheduled at full capacity", async () => {
      await initialize({ maxParallelism: 3 });
      // Fill to capacity. No completions, no future starts → main has
      // nothing to do this iteration but jobs are running, so it should
      // schedule itself (e.g. for recovery) with saturated=true.
      await fillRunningTo(3);

      await runMain();

      const o = await observe();
      assert(o.runStatus);
      expect(o.runStatus.kind).toBe("scheduled");
      if (o.runStatus.kind === "scheduled") {
        expect(o.runStatus.saturated).toBe(true);
      }
    });

    it("records saturated=false when scheduling with under-capacity running jobs", async () => {
      await initialize({ maxParallelism: 5 });
      // Fewer running jobs than max → not saturated.
      await fillRunningTo(2);

      await runMain();

      const o = await observe();
      assert(o.runStatus);
      expect(o.runStatus.kind).toBe("scheduled");
      if (o.runStatus.kind === "scheduled") {
        expect(o.runStatus.saturated).toBe(false);
      }
    });

    it("clears saturated when a completion frees a slot", async () => {
      // Saturated → completion arrives → kick wakes main → main runs and
      // sees a freed slot → next scheduled state has saturated=false.
      await initialize({ maxParallelism: 2 });
      const ids = await fillRunningTo(2);
      await runMain(); // first transition: scheduled, saturated=true
      expect((await observe()).runStatus).toMatchObject({
        kind: "scheduled",
        saturated: true,
      });

      // A worker completes — frees a slot.
      await simulateCompletion(
        ids[0],
        { kind: "success", returnValue: null },
        0,
      );
      await runMain();

      const o = await observe();
      assert(o.runStatus);
      // After processing the completion, running.length is 1 < 2, so any
      // subsequent scheduled state should NOT be saturated.
      if (o.runStatus.kind === "scheduled") {
        expect(o.runStatus.saturated).toBe(false);
      } else {
        // Or we might be in 'running' (within cooldown) — also fine; just
        // ensure we did not stay saturated=true.
        expect(o.runStatus.kind).not.toBe("idle");
      }
    });

    it("does not start new work while saturated, even when pendingStart accumulates", async () => {
      // Demonstrates that the capacity-aware query honors the running cap:
      // when running == max, getPending returns zero starts, so new
      // enqueues sit in pendingStart until a slot opens.
      await initialize({ maxParallelism: 2 });
      await fillRunningTo(2);

      // New work arrives while saturated.
      const newWorkId = await enqueueWork();

      await runMain();

      const o = await observe();
      // No new starts — we're at max capacity.
      expect(o.running).toHaveLength(2);
      expect(o.pendingStart.map((p) => p.workId)).toContain(newWorkId);
      assert(o.runStatus);
      if (o.runStatus.kind === "scheduled") {
        expect(o.runStatus.saturated).toBe(true);
      }
    });

    it("stays saturated when a completion frees a slot but more work is waiting", async () => {
      // Externally observable: a completion arriving while saturated, with
      // more pendingStart queued, should leave runStatus = scheduled +
      // saturated=true. The freed slot gets refilled from pendingStart in
      // the same iteration, so running.length stays at max and the visible
      // saturated state doesn't drop.
      await initialize({ maxParallelism: 2 });
      const ids = await fillRunningTo(2);
      // Two more items waiting behind the at-capacity loop.
      await enqueueWork();
      await enqueueWork();

      // First main iteration arrives at the saturated end state.
      await runMain();
      expect((await observe()).runStatus).toMatchObject({
        kind: "scheduled",
        saturated: true,
      });

      // A worker completes — frees a slot, but pendingStart still has work.
      await simulateCompletion(
        ids[0],
        { kind: "success", returnValue: null },
        0,
      );

      // First iteration after the completion does work (processes
      // completion + starts a new pending), so didWork=true and main
      // self-reschedules with runStatus = "running".
      await runMain();
      // Advance past the cooldown so the next iteration actually records
      // the end-of-run state instead of holding "running" via cooldown.
      vi.setSystemTime(Date.now() + STATUS_COOLDOWN + SECOND);
      await runMain();

      const o = await observe();
      assert(o.runStatus);
      // Slot was refilled from pendingStart → running back at max →
      // saturated=true is the externally observed state again.
      expect(o.running).toHaveLength(2);
      expect(o.runStatus).toMatchObject({
        kind: "scheduled",
        saturated: true,
      });
    });
  });

  // ────────────────────────────────────────────────────────────────────
  // Recovery: stuck running jobs get cleaned up
  // ────────────────────────────────────────────────────────────────────

  describe("recovery", () => {
    it("flags running entries whose worker has been silent past the threshold", async () => {
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

      await runMain();

      // We can't directly verify "recovery was scheduled" without inspecting
      // the scheduler queue, but we can verify lastRecovery was advanced.
      const after = await observe();
      const state = await t.run(async (ctx) =>
        ctx.db.query("internalState").unique(),
      );
      assert(state);
      expect(state.lastRecovery).toBeGreaterThan(0n);
      // Work is still in running (recovery removes it via complete, which
      // happens in a separately-scheduled mutation).
      expect(after.running.map((r) => r.workId)).toContain(workId);
    });
  });

  // ────────────────────────────────────────────────────────────────────
  // Generation safety: stale main calls cannot clobber state
  // ────────────────────────────────────────────────────────────────────

  describe("generation safety", () => {
    it("rejects main calls with the wrong generation", async () => {
      await initialize();
      // Current generation is 1n. Calling with 99n should error.
      await expect(
        t.mutation(internal.loop.main, { generation: 99n }),
      ).rejects.toThrow(/generation mismatch/);
    });

    it("increments the generation each time main runs", async () => {
      await initialize();
      const before = (await observe()).generation;
      await runMain();
      const after = (await observe()).generation;
      expect(after).toBeGreaterThan(before);
    });
  });

  // ────────────────────────────────────────────────────────────────────
  // Backwards compatibility with the pre-merge API
  // ────────────────────────────────────────────────────────────────────

  describe("backwards compatibility", () => {
    it("main accepts (and ignores) a legacy `segment` arg", async () => {
      await initialize();
      const workId = await enqueueWork();

      // The legacy callsites pass `segment`; the new main treats it as
      // optional. Calls should still process work as expected.
      await t.mutation(internal.loop.main, {
        generation: 1n,
        segment: 12345n,
      });

      expect((await observe()).running.map((r) => r.workId)).toEqual([workId]);
    });

    it("updateRunStatus schedules a main call (forwards in-flight upgrade traffic)", async () => {
      await initialize();
      // A pre-upgrade scheduled call lands here after deploy.
      await t.mutation(internal.loop.updateRunStatus, {
        generation: 1n,
        segment: 12345n,
      });
      // The forwarder should have scheduled main; we don't drain the
      // full pipeline (that's covered by the other tests). Just verify
      // a main call was queued.
      const scheduled = await t.run(async (ctx) =>
        ctx.db.system.query("_scheduled_functions").collect(),
      );
      const mainCalls = scheduled.filter((s) => s.name.endsWith("loop:main"));
      expect(mainCalls.length).toBeGreaterThan(0);
    });
  });
});
