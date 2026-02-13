import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";
import { _runExecutorLoop, type _ExecutorDeps } from "./batch.js";

// ─── Helpers ────────────────────────────────────────────────────────────────

/** Create a promise that resolves when `resolve()` is called externally. */
function deferred<T = void>() {
  let resolve!: (v: T) => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

/** Track completed/failed/released task IDs for assertions. */
function createTracker() {
  const completed: string[] = [];
  const failed: Array<{ id: string; error: string }> = [];
  const released: string[] = [];
  let executorDoneCalled = false;
  let executorDoneStartMore = false;
  return {
    completed,
    failed,
    released,
    markDone(startMore: boolean) {
      executorDoneCalled = true;
      executorDoneStartMore = startMore;
    },
    get done() {
      return executorDoneCalled;
    },
    get startMore() {
      return executorDoneStartMore;
    },
  };
}

function makeDeps(overrides: Partial<_ExecutorDeps> = {}): _ExecutorDeps {
  return {
    claimBatch: vi.fn().mockResolvedValue([]),
    complete: vi.fn().mockResolvedValue(undefined),
    fail: vi.fn().mockResolvedValue(undefined),
    countPending: vi.fn().mockResolvedValue(0),
    releaseClaims: vi.fn().mockResolvedValue(undefined),
    executorDone: vi.fn().mockResolvedValue(undefined),
    getHandler: vi.fn().mockReturnValue(undefined),
    getHeapUsedBytes: () => 0,
    isNode: true,
    ...overrides,
  };
}

// Short deadlines for testing. With actionTimeout = 600_000:
//   softDeadline = start + 600_000 - softDeadlineMs
// So softDeadlineMs: 599_000 → softDeadline = start + 1_000 (1 second)
const SHORT_OPTIONS = {
  claimDeadlineMs: 500,
  softDeadlineMs: 599_000, // → 1s soft deadline
  maxConcurrencyPerWorker: 10,
  pollIntervalMs: 50,
  maxHeapMB: 100, // 100 MB
};

// ─── Tests ──────────────────────────────────────────────────────────────────

describe("_runExecutorLoop", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  // ─── Soft deadline ──────────────────────────────────────────────────────

  describe("soft deadline", () => {
    it("releases in-flight tasks when deadline arrives", async () => {
      const slow = deferred();
      const tracker = createTracker();

      const deps = makeDeps({
        claimBatch: vi.fn()
          .mockResolvedValueOnce([
            { _id: "slow-1", name: "slow", args: {}, attempt: 0 },
            { _id: "slow-2", name: "slow", args: {}, attempt: 0 },
          ])
          .mockResolvedValue([]),
        getHandler: () => () => slow.promise,
        releaseClaims: vi.fn().mockImplementation(async (ids: string[]) => {
          tracker.released.push(...ids);
        }),
        executorDone: vi.fn().mockImplementation(async (startMore: boolean) => {
          tracker.markDone(startMore);
        }),
      });

      const loopDone = _runExecutorLoop(deps, SHORT_OPTIONS);
      await vi.advanceTimersByTimeAsync(2_000);
      await loopDone;

      // Both slow tasks must be released — not left as orphans
      expect(tracker.released.sort()).toEqual(["slow-1", "slow-2"]);
      expect(tracker.done).toBe(true);

      slow.resolve();
    });

    it("completes fast tasks before deadline, releases only slow ones", async () => {
      const slow = deferred();
      const tracker = createTracker();

      const deps = makeDeps({
        claimBatch: vi.fn()
          .mockResolvedValueOnce([
            { _id: "fast-1", name: "fast", args: {}, attempt: 0 },
            { _id: "slow-1", name: "slow", args: {}, attempt: 0 },
          ])
          .mockResolvedValue([]),
        getHandler: (name: string) => {
          if (name === "fast") return async () => "done";
          if (name === "slow") return () => slow.promise;
          return undefined;
        },
        complete: vi.fn().mockImplementation(async (taskId: string) => {
          tracker.completed.push(taskId);
        }),
        releaseClaims: vi.fn().mockImplementation(async (ids: string[]) => {
          tracker.released.push(...ids);
        }),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, SHORT_OPTIONS);
      await vi.advanceTimersByTimeAsync(2_000);
      await loopDone;

      expect(tracker.completed).toContain("fast-1");
      expect(tracker.released).toEqual(["slow-1"]);
      expect(tracker.released).not.toContain("fast-1");

      slow.resolve();
    });
  });

  // ─── Claim deadline ─────────────────────────────────────────────────────

  describe("claim deadline", () => {
    it("stops claiming new tasks after deadline but drains in-flight", async () => {
      const taskDeferred = deferred();
      const tracker = createTracker();
      let claimCallCount = 0;

      const deps = makeDeps({
        claimBatch: vi.fn().mockImplementation(async () => {
          claimCallCount++;
          if (claimCallCount === 1) {
            return [{ _id: "t1", name: "handler", args: {}, attempt: 0 }];
          }
          return [];
        }),
        getHandler: () => () => taskDeferred.promise,
        complete: vi.fn().mockImplementation(async (taskId: string) => {
          tracker.completed.push(taskId);
        }),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, {
        ...SHORT_OPTIONS,
        claimDeadlineMs: 200,
        softDeadlineMs: 595_000, // 5s soft deadline — plenty of drain time
      });

      // Advance past claim deadline
      await vi.advanceTimersByTimeAsync(300);
      const callsBefore = claimCallCount;

      // Resolve the in-flight task — it should complete even past claim deadline
      taskDeferred.resolve("result");
      await vi.advanceTimersByTimeAsync(100);
      await loopDone;

      expect(tracker.completed).toEqual(["t1"]);
      expect(claimCallCount).toBe(callsBefore);
    });
  });

  // ─── Memory pressure ───────────────────────────────────────────────────

  describe("memory pressure", () => {
    it("pauses claiming under memory pressure, resumes when memory drops", async () => {
      let heapMB = 50; // Under 100 MB limit
      const tracker = createTracker();
      let claimCallCount = 0;

      const deps = makeDeps({
        claimBatch: vi.fn().mockImplementation(async () => {
          claimCallCount++;
          if (claimCallCount === 1) {
            return [{ _id: "t1", name: "spike", args: {}, attempt: 0 }];
          }
          if (claimCallCount === 2) {
            return [{ _id: "t2", name: "normal", args: {}, attempt: 0 }];
          }
          return [];
        }),
        getHandler: (name: string) => {
          if (name === "spike") {
            return async () => {
              heapMB = 200; // Over limit during execution
              return "spiked";
            };
          }
          return async () => "done";
        },
        getHeapUsedBytes: () => heapMB * 1024 * 1024,
        isNode: true,
        complete: vi.fn().mockImplementation(async (taskId: string) => {
          tracker.completed.push(taskId);
          if (taskId === "t1") heapMB = 30; // Memory drops after t1 completes
        }),
        countPending: vi.fn().mockImplementation(async () => {
          return tracker.completed.length < 2 ? 1 : 0;
        }),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, SHORT_OPTIONS);

      // First claim + handler run
      await vi.advanceTimersByTimeAsync(100);
      expect(tracker.completed).toContain("t1");

      // Memory was high, then dropped via complete callback.
      // Advance enough for the loop to poll and re-claim.
      await vi.advanceTimersByTimeAsync(1_200);
      await loopDone;

      // t2 claimed and completed after memory dropped back under limit
      expect(tracker.completed).toContain("t2");
    });

    it("skips memory check entirely on non-Node runtime", async () => {
      const tracker = createTracker();

      const deps = makeDeps({
        claimBatch: vi.fn()
          .mockResolvedValueOnce([
            { _id: "t1", name: "handler", args: {}, attempt: 0 },
          ])
          .mockResolvedValue([]),
        getHandler: () => async () => "result",
        getHeapUsedBytes: () => 999 * 1024 * 1024, // absurdly high — should be ignored
        isNode: false,
        complete: vi.fn().mockImplementation(async (taskId: string) => {
          tracker.completed.push(taskId);
        }),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, SHORT_OPTIONS);
      await vi.advanceTimersByTimeAsync(2_000);
      await loopDone;

      expect(tracker.completed).toEqual(["t1"]);
    });

    it("disables memory check with maxHeapMB: 0", async () => {
      const tracker = createTracker();

      const deps = makeDeps({
        claimBatch: vi.fn()
          .mockResolvedValueOnce([
            { _id: "t1", name: "handler", args: {}, attempt: 0 },
          ])
          .mockResolvedValue([]),
        getHandler: () => async () => "result",
        getHeapUsedBytes: () => 999 * 1024 * 1024,
        isNode: true, // Node but maxHeapMB=0 disables it
        complete: vi.fn().mockImplementation(async (taskId: string) => {
          tracker.completed.push(taskId);
        }),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, { ...SHORT_OPTIONS, maxHeapMB: 0 });
      await vi.advanceTimersByTimeAsync(2_000);
      await loopDone;

      expect(tracker.completed).toEqual(["t1"]);
    });
  });

  // ─── Error handling / crash paths ──────────────────────────────────────

  describe("error handling", () => {
    it("reports failure for unknown handler and continues with other tasks", async () => {
      const tracker = createTracker();

      const deps = makeDeps({
        claimBatch: vi.fn()
          .mockResolvedValueOnce([
            { _id: "unknown-1", name: "doesNotExist", args: {}, attempt: 0 },
            { _id: "known-1", name: "myHandler", args: {}, attempt: 0 },
          ])
          .mockResolvedValue([]),
        getHandler: (name: string) => {
          if (name === "myHandler") return async () => "ok";
          return undefined;
        },
        fail: vi.fn().mockImplementation(async (taskId: string, error: string) => {
          tracker.failed.push({ id: taskId, error });
        }),
        complete: vi.fn().mockImplementation(async (taskId: string) => {
          tracker.completed.push(taskId);
        }),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, SHORT_OPTIONS);
      await vi.advanceTimersByTimeAsync(2_000);
      await loopDone;

      expect(tracker.failed).toEqual([
        { id: "unknown-1", error: "Unknown handler: doesNotExist" },
      ]);
      expect(tracker.completed).toEqual(["known-1"]);
    });

    it("reports handler errors via fail() mutation", async () => {
      const tracker = createTracker();

      const deps = makeDeps({
        claimBatch: vi.fn()
          .mockResolvedValueOnce([
            { _id: "crash-1", name: "crasher", args: {}, attempt: 0 },
          ])
          .mockResolvedValue([]),
        getHandler: () => async () => {
          throw new Error("handler exploded");
        },
        fail: vi.fn().mockImplementation(async (taskId: string, error: string) => {
          tracker.failed.push({ id: taskId, error });
        }),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, SHORT_OPTIONS);
      await vi.advanceTimersByTimeAsync(2_000);
      await loopDone;

      expect(tracker.failed).toEqual([
        { id: "crash-1", error: "Error: handler exploded" },
      ]);
    });

    it("survives double failure (handler throws + fail() throws)", async () => {
      // Handler crashes, then fail() mutation also throws (task was deleted).
      // The executor must NOT crash — other tasks must still complete.
      const tracker = createTracker();

      const deps = makeDeps({
        claimBatch: vi.fn()
          .mockResolvedValueOnce([
            { _id: "double-fail", name: "crasher", args: {}, attempt: 0 },
            { _id: "healthy", name: "ok", args: {}, attempt: 0 },
          ])
          .mockResolvedValue([]),
        getHandler: (name: string) => {
          if (name === "crasher") return async () => { throw new Error("boom"); };
          return async () => "fine";
        },
        fail: vi.fn().mockImplementation(async (taskId: string) => {
          if (taskId === "double-fail") throw new Error("fail mutation crashed");
        }),
        complete: vi.fn().mockImplementation(async (taskId: string) => {
          tracker.completed.push(taskId);
        }),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, SHORT_OPTIONS);
      await vi.advanceTimersByTimeAsync(2_000);
      await loopDone;

      // Healthy task must still complete
      expect(tracker.completed).toContain("healthy");
      expect(console.error).toHaveBeenCalled();
    });

    it("finally block releases in-flight claims even when loop crashes", async () => {
      // First claim returns [fast, slow]. Fast completes immediately,
      // triggering another iteration. Second claimBatch throws.
      // The finally block must release the slow task.
      const slow = deferred();
      const tracker = createTracker();

      const deps = makeDeps({
        claimBatch: vi.fn()
          .mockResolvedValueOnce([
            { _id: "fast-1", name: "fast", args: {}, attempt: 0 },
            { _id: "slow-1", name: "slow", args: {}, attempt: 0 },
          ])
          .mockImplementation(async () => { throw new Error("db crash"); }),
        getHandler: (name: string) => {
          if (name === "fast") return async () => "done";
          return () => slow.promise;
        },
        complete: vi.fn().mockResolvedValue(undefined),
        releaseClaims: vi.fn().mockImplementation(async (ids: string[]) => {
          tracker.released.push(...ids);
        }),
        executorDone: vi.fn(),
      });

      // Attach catch immediately to prevent unhandled rejection warning.
      // The error still propagates — we inspect it below.
      const loopPromise = _runExecutorLoop(deps, SHORT_OPTIONS)
        .then(() => null)
        .catch((e: unknown) => e);

      // Flush microtasks: fast-1 completes, loop tries second claim → crash
      await vi.advanceTimersByTimeAsync(100);

      const result = await loopPromise;

      expect(result).toBeInstanceOf(Error);
      expect((result as Error).message).toBe("db crash");
      expect(tracker.released).toEqual(["slow-1"]);

      slow.resolve();
    });
  });

  // ─── Concurrency ───────────────────────────────────────────────────────

  describe("concurrency", () => {
    it("respects maxConcurrencyPerWorker limit", async () => {
      const tracker = createTracker();

      const deps = makeDeps({
        claimBatch: vi.fn()
          .mockResolvedValueOnce(
            Array.from({ length: 5 }, (_, i) => ({
              _id: `t${i}`, name: "handler", args: {}, attempt: 0,
            })),
          )
          .mockResolvedValue([]),
        getHandler: () => async () => {
          await new Promise((r) => setTimeout(r, 50));
          return "done";
        },
        complete: vi.fn().mockImplementation(async (taskId: string) => {
          tracker.completed.push(taskId);
        }),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, {
        ...SHORT_OPTIONS,
        maxConcurrencyPerWorker: 3,
      });

      await vi.advanceTimersByTimeAsync(2_000);
      await loopDone;

      expect(tracker.completed.length).toBe(5);
      // First claim was capped at 3 (not 5)
      expect(deps.claimBatch).toHaveBeenCalledWith(3);
    });
  });

  // ─── Retry scenarios (executor perspective) ───────────────────────────

  describe("retry scenarios", () => {
    it("re-claims a task that was retried (failed then re-queued by component)", async () => {
      // Simulates: task fails on attempt 0, component re-queues it with
      // attempt=1, executor claims it again and it succeeds.
      const tracker = createTracker();
      let claimCount = 0;
      let handlerCallCount = 0;

      const deps = makeDeps({
        claimBatch: vi.fn().mockImplementation(async () => {
          claimCount++;
          if (claimCount === 1) {
            return [{ _id: "retry-task", name: "flaky", args: {}, attempt: 0 }];
          }
          if (claimCount === 2) {
            // Same task ID re-appears after component re-queued it
            return [{ _id: "retry-task", name: "flaky", args: {}, attempt: 1 }];
          }
          return [];
        }),
        // Shared counter across handler invocations
        getHandler: () => async () => {
          handlerCallCount++;
          if (handlerCallCount === 1) throw new Error("transient failure");
          return "success on retry";
        },
        fail: vi.fn().mockImplementation(async (taskId: string, error: string) => {
          tracker.failed.push({ id: taskId, error });
        }),
        complete: vi.fn().mockImplementation(async (taskId: string) => {
          tracker.completed.push(taskId);
        }),
        countPending: vi.fn().mockImplementation(async () => {
          return tracker.completed.length === 0 ? 1 : 0;
        }),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, SHORT_OPTIONS);
      await vi.advanceTimersByTimeAsync(2_000);
      await loopDone;

      // First attempt failed, second succeeded
      expect(tracker.failed).toHaveLength(1);
      expect(tracker.completed).toContain("retry-task");
    });

    it("handler returning undefined becomes null (Convex serialization)", async () => {
      // Convex can't serialize undefined — the executor must coerce to null.
      const completedWith: Array<{ id: string; result: unknown }> = [];

      const deps = makeDeps({
        claimBatch: vi.fn()
          .mockResolvedValueOnce([
            { _id: "t1", name: "void-handler", args: {}, attempt: 0 },
          ])
          .mockResolvedValue([]),
        getHandler: () => async () => undefined,
        complete: vi.fn().mockImplementation(async (taskId: string, result: unknown) => {
          completedWith.push({ id: taskId, result });
        }),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, SHORT_OPTIONS);
      await vi.advanceTimersByTimeAsync(2_000);
      await loopDone;

      expect(completedWith).toEqual([{ id: "t1", result: null }]);
    });
  });

  // ─── Exit conditions ──────────────────────────────────────────────────

  describe("exit conditions", () => {
    it("exits immediately when no pending work and nothing in flight", async () => {
      const deps = makeDeps({
        claimBatch: vi.fn().mockResolvedValue([]),
        countPending: vi.fn().mockResolvedValue(0),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, SHORT_OPTIONS);
      await vi.advanceTimersByTimeAsync(100);
      await loopDone;

      expect(deps.executorDone).toHaveBeenCalledWith(false);
    });

    it("signals startMore=true when pending work remains at exit", async () => {
      const deps = makeDeps({
        claimBatch: vi.fn().mockResolvedValue([]),
        countPending: vi.fn().mockResolvedValue(5), // always 5 pending with future readyAt
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, {
        ...SHORT_OPTIONS,
        claimDeadlineMs: 100,
        softDeadlineMs: 599_500, // soft deadline at 500ms
      });

      await vi.advanceTimersByTimeAsync(1_000);
      await loopDone;

      // Executor couldn't claim anything, but there's still pending work
      expect(deps.executorDone).toHaveBeenCalledWith(true);
    });

    it("polls at pollIntervalMs when tasks exist with future readyAt", async () => {
      // claimBatch returns nothing (all tasks have future readyAt),
      // but countPending returns >0. The executor should poll, not exit.
      let pollCount = 0;
      const deps = makeDeps({
        claimBatch: vi.fn().mockResolvedValue([]),
        countPending: vi.fn().mockImplementation(async () => {
          pollCount++;
          // After 3 polls, no more pending
          return pollCount <= 3 ? 1 : 0;
        }),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, {
        ...SHORT_OPTIONS,
        pollIntervalMs: 100,
      });

      await vi.advanceTimersByTimeAsync(2_000);
      await loopDone;

      // Should have polled at least 3 times before exiting
      expect(pollCount).toBeGreaterThanOrEqual(3);
      expect(deps.executorDone).toHaveBeenCalledWith(false);
    });
  });
});
