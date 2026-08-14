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
import { api, components, internal } from "./_generated/api.js";
import type { Doc, Id } from "./_generated/dataModel.js";
import { enqueueHandler } from "./lib.js";
import { createLogger } from "./logging.js";
import { RECOVERY_PERIOD_SEGMENTS } from "./loop.js";
import { setupTest } from "./setup.test.js";
import {
  DEFAULT_MAX_PARALLELISM,
  fromSegment,
  fromTimestamp,
  toSegment,
  toTimestamp,
  WORKER_NAME,
} from "./shared.js";

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
          lastReportTs: Date.now(),
        },
        running: [],
      });
    });
  }

  /**
   * Enqueue work through the real `enqueueHandler`, so what the tests write
   * can't drift from what the public API writes. `runAt` holds the work until
   * then; the default runs it as soon as the loop sees it. `segment` instead
   * writes a raw entry pinned to that ordering value, e.g. to give several
   * entries the same one the way a single batch enqueue does.
   */
  async function enqueueWork(
    overrides: Partial<WithoutSystemFields<Doc<"work">>> = {},
    { runAt, segment }: { runAt?: number; segment?: bigint } = {},
  ): Promise<Id<"work">> {
    return t.run(async (ctx) => {
      if (segment !== undefined) {
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
      }
      return enqueueHandler(ctx, createLogger("WARN"), {
        fnType: "action",
        fnHandle: "test_handle",
        fnName: "test_handle",
        fnArgs: {},
        runAt: runAt ?? Date.now(),
        ...overrides,
      });
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
        segmentCursors: state?.segmentCursors,
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

    it("starts action and query work through one batch action wrapper", async () => {
      await initialize({ maxParallelism: 5 });
      const actionId = await enqueueWork({ fnType: "action" });
      const queryId = await enqueueWork({ fnType: "query" });
      const mutationId = await enqueueWork({ fnType: "mutation" });

      await runLoop();

      const o = await observe();
      const actionRunning = o.running.find((r) => r.workId === actionId);
      const queryRunning = o.running.find((r) => r.workId === queryId);
      const mutationRunning = o.running.find((r) => r.workId === mutationId);
      assert(actionRunning);
      assert(queryRunning);
      assert(mutationRunning);
      expect(actionRunning.scheduledId).toBe(queryRunning.scheduledId);
      expect(mutationRunning.scheduledId).not.toBe(actionRunning.scheduledId);
    });

    it("chunks action and query starts into batches of 32", async () => {
      await initialize({ maxParallelism: 64 });
      for (let i = 0; i < 33; i++) {
        await enqueueWork({ fnType: i % 2 === 0 ? "action" : "query" });
      }

      await runLoop();

      const o = await observe();
      expect(o.running).toHaveLength(33);
      const countsByScheduledId = new Map<string, number>();
      for (const { scheduledId } of o.running) {
        countsByScheduledId.set(
          scheduledId,
          (countsByScheduledId.get(scheduledId) ?? 0) + 1,
        );
      }
      expect([...countsByScheduledId.values()].sort((a, b) => a - b)).toEqual([
        1, 32,
      ]);
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

    it("doesn't drop work that shares a commit timestamp with the cursor", async () => {
      // A batch enqueue commits every row at the same commit timestamp, so the
      // cursor lands in the middle of a group whenever capacity cuts one short.
      await initialize({ maxParallelism: 2 });
      const sharedTimestamp = 1_000n;
      const ids = [];
      for (let i = 0; i < 4; i++) {
        ids.push(await enqueueWork({}, { segment: sharedTimestamp }));
      }

      const started: Id<"work">[] = [];
      for (let i = 0; i < 4; i++) {
        await runLoop();
        const o = await observe();
        for (const r of o.running) {
          if (!started.includes(r.workId)) started.push(r.workId);
        }
        for (const r of o.running) {
          await simulateCompletion(
            r.workId,
            { kind: "success", returnValue: null },
            0,
          );
        }
      }
      await runLoop();

      // All four ran even though the cursor sat on their shared timestamp.
      expect(new Set(started)).toEqual(new Set(ids));
      expect((await observe()).pendingStart).toHaveLength(0);
    });
  });

  // ────────────────────────────────────────────────────────────────────
  // Entries written before commit-timestamp ordering
  // ────────────────────────────────────────────────────────────────────

  describe("upgrade from segment ordering", () => {
    /** A pendingStart as an older version wrote it: a 100ms bucket, no runAt. */
    async function enqueueLegacyWork(runAt: number): Promise<Id<"work">> {
      return t.run(async (ctx) => {
        const workId = await ctx.db.insert("work", {
          fnType: "action",
          fnHandle: "test_handle",
          fnName: "test_handle",
          fnArgs: {},
          attempts: 0,
        });
        await ctx.db.insert("pendingStart", {
          workId,
          // max(toSegment(runAt), now), as `enqueue` used to compute it.
          segment: toSegment(Math.max(runAt, Date.now())),
        });
        return workId;
      });
    }

    it("starts work that was already due", async () => {
      await initialize();
      const workId = await enqueueLegacyWork(Date.now());

      await runLoop();

      const o = await observe();
      expect(o.pendingStart).toHaveLength(0);
      expect(o.running.map((r) => r.workId)).toEqual([workId]);
    });

    it("moves near-future work a pre-lane version left in the incoming lane", async () => {
      await initialize();
      const runAt = Date.now() + 100 * SECOND;
      // Held at its commit position with `runAt` but no `hasRunAt`: the shape
      // written before the scheduled lane existed.
      const workId = await t.run(async (ctx) => {
        const wid = await ctx.db.insert("work", {
          fnType: "action",
          fnHandle: "test_handle",
          fnName: "test_handle",
          fnArgs: {},
          attempts: 0,
        });
        await ctx.db.insert("pendingStart", {
          workId: wid,
          segment: ctx.db.vars.commitTs,
          runAt,
        });
        return wid;
      });

      await runLoop();

      const o = await observe();
      expect(o.running).toHaveLength(0);
      expect(o.pendingStart[0].segment).toBe(toTimestamp(runAt));

      vi.advanceTimersByTime(100 * SECOND);
      await runLoop();
      expect((await observe()).running.map((r) => r.workId)).toEqual([workId]);
    });

    it("keeps holding work scheduled for later, rather than starting it early", async () => {
      await initialize();
      const runAt = Date.now() + 100 * SECOND;
      const workId = await enqueueLegacyWork(runAt);
      // The old format only recorded the time to a 100ms bucket.
      const startsAt = fromSegment(toSegment(runAt));

      await runLoop();

      // Not started — and rewritten as a real timestamp, so the next scan
      // leaves it alone until it's due.
      let o = await observe();
      expect(o.running).toHaveLength(0);
      expect(o.pendingStart[0].segment).toBe(toTimestamp(startsAt));

      await runLoop(); // the sweep verifies the re-keyed entry, once
      const idle = await runLoop();
      assert(idle.kind === "idle");
      expect(idle.timeoutMs).toBe(startsAt - Date.now());

      vi.advanceTimersByTime(startsAt - Date.now());
      await runLoop();
      o = await observe();
      expect(o.pendingStart).toHaveLength(0);
      expect(o.running.map((r) => r.workId)).toEqual([workId]);
    });
  });

  describe("capacity", () => {
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

    it("starts the retry once its backoff elapses, and advances past it", async () => {
      await initialize();
      const workId = await enqueueWork({
        retryBehavior: { maxAttempts: 3, initialBackoffMs: 100, base: 2 },
      });
      await runLoop();
      await simulateCompletion(workId, { kind: "failed", error: "boom" }, 0);
      await runLoop();
      await runLoop(); // the sweep verifies the retry's entry, once

      // Still held back by the backoff: nothing to start yet.
      expect((await runLoop()).kind).toBe("idle");
      expect((await observe()).pendingStart).toHaveLength(1);

      // Once the backoff elapses it starts from the not-yet-due lane, and the
      // lane's cursor moves past it so its tombstone isn't rescanned.
      vi.advanceTimersByTime(SECOND);
      await runLoop();
      const o = await observe();
      expect(o.pendingStart).toHaveLength(0);
      expect(o.running.map((r) => r.workId)).toEqual([workId]);
      expect(o.segmentCursors?.incoming).toBeGreaterThan(0n);
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

    it("idles with a timeoutMs when only far-future work remains", async () => {
      await initialize();
      // Ordered by its start time, so beyond the sweep's one-time
      // verification the loop never has to look at it.
      await enqueueWork({}, { runAt: Date.now() + 10 * MINUTE });
      await runLoop(); // the sweep verifies the entry, once

      const result = await t.query(internal.loop.getBatch, {
        name: WORKER_NAME,
      });
      assert(result.kind === "idle");
      expect(result.timeoutMs).toBeGreaterThan(0);
    });

    it("sweeps near-future work once, then idles until it's due", async () => {
      await initialize();
      const runAt = Date.now() + 100 * SECOND;
      // Keyed at its start time directly; the sweep just verifies its enqueue
      // committed in order, once, and never rewrites it.
      const workId = await enqueueWork({}, { runAt });

      const first = await runLoop();
      assert(first.kind === "work");
      expect(first.batch.starts).toHaveLength(0);
      expect(first.batch.sweep).toHaveLength(1);
      expect(first.batch.sweep![0].starts).toHaveLength(0);

      let o = await observe();
      expect(o.running).toHaveLength(0);
      expect(o.pendingStart[0].segment).toBe(toTimestamp(runAt));
      // The sweep cursor covers its commit stamp now, so it's never re-read.
      expect(o.segmentCursors!.scheduled).toBe(
        o.pendingStart[0].scheduledAt as bigint,
      );

      const second = await runLoop();
      assert(second.kind === "idle");
      expect(second.timeoutMs).toBe(100 * SECOND);

      // It starts when it comes due, untouched in the meantime.
      vi.advanceTimersByTime(100 * SECOND);
      await runLoop();
      o = await observe();
      expect(o.pendingStart).toHaveLength(0);
      expect(o.running.map((r) => r.workId)).toEqual([workId]);
    });

    it("idles with a timeoutMs for the next recovery scan when that's sooner than future work", async () => {
      await initialize();
      await enqueueWork();
      await runLoop(); // start it: running=1, lastRecovery=now

      // Future work well past the ~1min recovery period.
      await enqueueWork({}, { runAt: Date.now() + 10 * MINUTE });
      await runLoop(); // the sweep verifies the entry, once

      const result = await t.query(internal.loop.getBatch, {
        name: WORKER_NAME,
      });
      assert(result.kind === "idle");
      const { lastRecovery } = await observe();
      const untilNextRecovery =
        fromSegment(lastRecovery + RECOVERY_PERIOD_SEGMENTS) - Date.now();
      expect(result.timeoutMs).toBe(untilNextRecovery);
      expect(result.timeoutMs).toBeLessThan(5 * MINUTE);
    });
  });

  // ────────────────────────────────────────────────────────────────────
  // Scheduled work: keyed at its start time, invisible until due
  // ────────────────────────────────────────────────────────────────────

  describe("scheduled work", () => {
    it("never lets a scheduled backlog sit in front of ready work", async () => {
      // A bulk enqueue of near-future work sorts above the eligibility bound,
      // so it costs the ready item below nothing — no re-keying, no waiting.
      await initialize({ maxParallelism: 3 });
      const runAt = Date.now() + 100 * SECOND;
      for (let i = 0; i < 70; i++) await enqueueWork({}, { runAt });
      const readyId = await enqueueWork();

      await runLoop();

      const o = await observe();
      expect(o.running.map((r) => r.workId)).toEqual([readyId]);
      expect(o.pendingStart).toHaveLength(70);
      for (const p of o.pendingStart) {
        expect(p.segment).toBe(toTimestamp(runAt));
        expect(p.scheduledAt).toBeDefined();
      }
      // The sweep verified all 70 in one pass; nothing left to do before the
      // sooner of their due time and the next stuck-job recovery check.
      const idle = await runLoop();
      assert(idle.kind === "idle");
      expect(idle.timeoutMs).toBeLessThanOrEqual(100 * SECOND);
    });

    it("starts due scheduled work and ready work in eligibility order", async () => {
      await initialize({ maxParallelism: 1 });
      const scheduledId = await enqueueWork({}, { runAt: Date.now() + SECOND });
      const readyId = await enqueueWork();
      vi.advanceTimersByTime(SECOND);

      // The ready item became eligible first: its commit precedes the runAt.
      await runLoop();
      let o = await observe();
      expect(o.running.map((r) => r.workId)).toEqual([readyId]);

      await simulateCompletion(
        readyId,
        { kind: "success", returnValue: null },
        0,
      );
      await runLoop();
      o = await observe();
      expect(o.pendingStart).toHaveLength(0);
      expect(o.running.map((r) => r.workId)).toEqual([scheduledId]);
    });

    it("starts scheduled work once it's due, without ever rewriting it", async () => {
      await initialize();
      const workId = await enqueueWork({}, { runAt: Date.now() + SECOND });

      vi.advanceTimersByTime(SECOND);
      await runLoop();

      const o = await observe();
      expect(o.pendingStart).toHaveLength(0);
      expect(o.running.map((r) => r.workId)).toEqual([workId]);
    });

    it("holds due work while saturated, and starts it as capacity frees", async () => {
      await initialize({ maxParallelism: 1 });
      const firstId = await enqueueWork();
      await runLoop(); // fills the only slot
      const dueId = await enqueueWork({}, { runAt: Date.now() + SECOND });
      vi.advanceTimersByTime(2 * SECOND);

      await runLoop(); // the sweep verifies the new entry, once
      const idle = await runLoop();
      expect(idle.kind).toBe("idle"); // saturated: nothing can start
      expect((await observe()).pendingStart).toHaveLength(1);

      // A completion frees the slot; the same iteration starts the work.
      await simulateCompletion(
        firstId,
        { kind: "success", returnValue: null },
        0,
      );
      await runLoop();
      const o = await observe();
      expect(o.pendingStart).toHaveLength(0);
      expect(o.running.map((r) => r.workId)).toEqual([dueId]);
    });
  });

  // ────────────────────────────────────────────────────────────────────
  // The out-of-order sweep: entries whose enqueue lost the race
  // ────────────────────────────────────────────────────────────────────

  describe("out-of-order sweep", () => {
    /**
     * A near-future enqueue whose commit took longer than its delay: the
     * entry appears with a `segment` the cursor already passed, where the
     * segment scan will never see it. Its `scheduledAt` commit stamp is how
     * the sweep finds it.
     */
    async function enqueueBehindCursor(cursor: bigint): Promise<Id<"work">> {
      return t.run(async (ctx) => {
        const workId = await ctx.db.insert("work", {
          fnType: "action",
          fnHandle: "test_handle",
          fnName: "test_handle",
          fnArgs: {},
          attempts: 0,
        });
        await ctx.db.insert("pendingStart", {
          workId,
          segment: cursor - 1n,
          runAt: fromTimestamp(cursor - 1n),
          scheduledAt: ctx.db.vars.commitTs,
        });
        return workId;
      });
    }

    it("starts an entry whose enqueue lost the race with the cursor", async () => {
      await initialize({ maxParallelism: 2 });
      // Run something so the incoming cursor is somewhere real.
      const readyId = await enqueueWork();
      await runLoop();
      const cursor = (await observe()).segmentCursors!.incoming;
      expect(cursor).toBeGreaterThan(0n);

      const lostId = await enqueueBehindCursor(cursor);

      await runLoop();
      const o = await observe();
      expect(o.running.map((r) => r.workId)).toContain(lostId);
      expect(o.pendingStart).toHaveLength(0);
      // Its commit stamp is covered, so the sweep never re-reads it.
      expect(o.segmentCursors!.scheduled).toBeGreaterThan(0n);
      expect(readyId).toBeDefined();
    });

    it("waits for capacity to retire an out-of-order entry, without losing it", async () => {
      await initialize({ maxParallelism: 1 });
      const firstId = await enqueueWork();
      await runLoop(); // fills the only slot
      const cursor = (await observe()).segmentCursors!.incoming;
      const lostId = await enqueueBehindCursor(cursor);

      // Saturated: the entry can't start, so the sweep leaves its stamp
      // uncovered rather than passing it.
      const idle = await runLoop();
      expect(idle.kind).toBe("idle");
      expect((await observe()).pendingStart.map((p) => p.workId)).toEqual([
        lostId,
      ]);

      await simulateCompletion(
        firstId,
        { kind: "success", returnValue: null },
        0,
      );
      await runLoop();
      const o = await observe();
      expect(o.pendingStart).toHaveLength(0);
      expect(o.running.map((r) => r.workId)).toEqual([lostId]);
    });

    it("caps the cursor at observed commit stamps when starting wall-keyed work", async () => {
      await initialize();
      const runAt = Date.now() + SECOND;
      const workId = await enqueueWork({}, { runAt });
      await runLoop(); // sweep verifies the entry
      vi.advanceTimersByTime(SECOND);
      await runLoop(); // starts it, keyed at its start time

      const o = await observe();
      expect(o.running.map((r) => r.workId)).toEqual([workId]);
      // The cursor stops at the freshest commit stamp this run had seen, not
      // at the wall-clock key: a commit racing this iteration could land
      // below that key, and nothing relates the two clocks.
      expect(o.segmentCursors!.incoming).toBeLessThan(toTimestamp(runAt));
    });
  });

  // ────────────────────────────────────────────────────────────────────
  // Cursor invariant: an entry is never left behind the cursor
  //
  // Nothing rescans a lane behind its cursor, so an entry written below it is
  // lost for good. Both cases below are ones where the time the loop wants to
  // write is behind a cursor that holds a commit timestamp from the same
  // millisecond — the two clocks aren't the same, so the loop places its
  // writes against the cursor rather than against `Date.now()`.
  // ────────────────────────────────────────────────────────────────────

  describe("cursor invariant", () => {
    it("starts a retry whose backoff is shorter than the cursor's resolution", async () => {
      await initialize();
      const workId = await enqueueWork({
        retryBehavior: { maxAttempts: 3, initialBackoffMs: 0, base: 2 },
      });
      await runLoop();
      await simulateCompletion(workId, { kind: "failed", error: "boom" }, 0);
      // A ready entry enqueued after the completion: its commit timestamp is
      // above the retry's `Date.now()`-derived one, so an unclamped cursor
      // would advance past the retry in the same iteration that wrote it.
      const laterId = await enqueueWork();

      await runLoop();
      let o = await observe();
      expect(o.running.map((r) => r.workId)).toEqual([laterId]);
      const retry = o.pendingStart.find((p) => p.workId === workId);
      assert(retry);
      expect(retry.segment).toBeGreaterThanOrEqual(o.segmentCursors!.incoming);

      // Still readable, so the next iteration starts it.
      await runLoop();
      o = await observe();
      expect(o.pendingStart).toHaveLength(0);
      expect(o.running.map((r) => r.workId)).toContain(workId);
    });

    it("keys a sub-millisecond runAt to the next whole millisecond", async () => {
      await initialize();
      // Truncated to whole milliseconds this `runAt` would be readable while
      // `isDue` still says no; rounded up, it's invisible until the first
      // millisecond the clock can call it due.
      const runAt = Date.now() + 0.5;
      const workId = await enqueueWork({}, { runAt });
      const readyId = await enqueueWork();

      await runLoop();
      const o = await observe();
      expect(o.running.map((r) => r.workId)).toEqual([readyId]);
      const entry = o.pendingStart.find((p) => p.workId === workId);
      assert(entry);
      expect(entry.segment).toBe(toTimestamp(Math.ceil(runAt)));

      vi.advanceTimersByTime(1);
      await runLoop();
      expect((await observe()).running.map((r) => r.workId)).toContain(workId);
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
    async function workerStatus() {
      const status = await t.query(components.batchWorker.lib.status, {
        name: WORKER_NAME,
      });
      return status?.kind ?? null;
    }

    it("internal.loop.main forwarder resumes the batch-worker loop", async () => {
      await initialize();
      expect(await workerStatus()).toBeNull(); // loop not running yet

      // A pre-migration scheduled call lands here after deploy. It pings
      // batch-worker, which registers and starts the loop.
      await t.mutation(internal.loop.main, { generation: 1n, segment: 123n });

      expect(await workerStatus()).toBe("running");
    });

    it("internal.loop.updateRunStatus forwarder resumes the batch-worker loop", async () => {
      await initialize();
      expect(await workerStatus()).toBeNull();

      await t.mutation(internal.loop.updateRunStatus, {
        generation: 1n,
        segment: 123n,
      });

      expect(await workerStatus()).toBe("running");
    });
  });
});
