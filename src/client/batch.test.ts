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

type ClaimedTask = { _id: string; name: string; args: any; attempt: number };
type OnCompleteItem = {
  fnHandle: string;
  workId: string;
  context?: unknown;
  result: unknown;
};

/** Track completed/failed/released task IDs for assertions. */
function createTracker() {
  const completed: string[] = [];
  const failed: Array<{ id: string; error: string }> = [];
  const released: string[] = [];
  const onCompleteDispatched: OnCompleteItem[] = [];
  let executorDoneCalled = false;
  let executorDoneStartMore = false;
  return {
    completed,
    failed,
    released,
    onCompleteDispatched,
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

/**
 * Helper: creates listPending + claimByIds mocks from a vi.fn() mock
 * that returns ClaimedTask[] (same pattern as the old claimBatch mock).
 */
function claimMocks(mock: (...args: unknown[]) => Promise<ClaimedTask[]>) {
  let pendingResult: ClaimedTask[] = [];
  return {
    listPending: vi.fn().mockImplementation(async (limit: number) => {
      const batch: ClaimedTask[] = await mock(limit);
      pendingResult = batch;
      return batch.map((t) => t._id);
    }),
    claimByIds: vi.fn().mockImplementation(async () => {
      const result = pendingResult;
      pendingResult = [];
      return result;
    }),
  };
}

function makeDeps(overrides: Partial<_ExecutorDeps> = {}): _ExecutorDeps {
  return {
    listPending: vi.fn().mockResolvedValue([]),
    claimByIds: vi.fn().mockResolvedValue([]),
    completeBatch: vi.fn().mockResolvedValue([]),
    failBatch: vi.fn().mockResolvedValue([]),
    dispatchOnCompleteBatch: vi.fn().mockResolvedValue(undefined),
    countPending: vi.fn().mockResolvedValue(0),
    releaseClaims: vi.fn().mockResolvedValue(undefined),
    executorDone: vi.fn().mockResolvedValue(undefined),
    getHandler: vi.fn().mockReturnValue(undefined),
    getHeapUsedBytes: () => 0,
    isNode: true,
    ...overrides,
  };
}

/** Helper: create completeBatch/failBatch mocks that feed into a tracker */
function batchMocks(tracker: ReturnType<typeof createTracker>) {
  return {
    completeBatch: vi.fn().mockImplementation(
      async (items: { taskId: string; result: unknown }[]) => {
        for (const item of items) tracker.completed.push(item.taskId);
        return []; // no onComplete items
      },
    ),
    failBatch: vi.fn().mockImplementation(
      async (items: { taskId: string; error: string }[]) => {
        for (const item of items) tracker.failed.push({ id: item.taskId, error: item.error });
        return []; // no onComplete items
      },
    ),
  };
}

// Short deadlines for testing. With actionTimeout = 600_000:
//   softDeadline = start + 600_000 - softDeadlineMs
// So softDeadlineMs: 599_000 → softDeadline = start + 1_000 (1 second)
const SHORT_OPTIONS = {
  claimDeadlineMs: 500,
  softDeadlineMs: 599_000, // → 1s soft deadline
  maxConcurrencyPerWorker: 10,
  maxWorkers: 10,
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
        ...claimMocks(
          vi.fn()
            .mockResolvedValueOnce([
              { _id: "slow-1", name: "slow", args: {}, attempt: 0 },
              { _id: "slow-2", name: "slow", args: {}, attempt: 0 },
            ])
            .mockResolvedValue([]),
        ),
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

      expect(tracker.released.sort()).toEqual(["slow-1", "slow-2"]);
      expect(tracker.done).toBe(true);

      slow.resolve();
    });

    it("completes fast tasks before deadline, releases only slow ones", async () => {
      const slow = deferred();
      const tracker = createTracker();

      const deps = makeDeps({
        ...claimMocks(
          vi.fn()
            .mockResolvedValueOnce([
              { _id: "fast-1", name: "fast", args: {}, attempt: 0 },
              { _id: "slow-1", name: "slow", args: {}, attempt: 0 },
            ])
            .mockResolvedValue([]),
        ),
        getHandler: (name: string) => {
          if (name === "fast") return async () => "done";
          if (name === "slow") return () => slow.promise;
          return undefined;
        },
        ...batchMocks(tracker),
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
        ...claimMocks(vi.fn().mockImplementation(async () => {
          claimCallCount++;
          if (claimCallCount === 1) {
            return [{ _id: "t1", name: "handler", args: {}, attempt: 0 }];
          }
          return [];
        })),
        getHandler: () => () => taskDeferred.promise,
        ...batchMocks(tracker),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, {
        ...SHORT_OPTIONS,
        claimDeadlineMs: 400,
        softDeadlineMs: 595_000, // 5s soft deadline
      });

      await vi.advanceTimersByTimeAsync(500);
      const callsBefore = claimCallCount;

      taskDeferred.resolve();
      await vi.advanceTimersByTimeAsync(200);
      await loopDone;

      expect(tracker.completed).toEqual(["t1"]);
      expect(claimCallCount).toBe(callsBefore);
    });
  });

  // ─── Memory pressure ───────────────────────────────────────────────────

  describe("memory pressure", () => {
    it("pauses claiming under memory pressure, resumes when memory drops", async () => {
      let heapMB = 50;
      const tracker = createTracker();
      let claimCallCount = 0;

      const deps = makeDeps({
        ...claimMocks(vi.fn().mockImplementation(async () => {
          claimCallCount++;
          if (claimCallCount === 1) {
            return [{ _id: "t1", name: "spike", args: {}, attempt: 0 }];
          }
          if (claimCallCount === 2) {
            return [{ _id: "t2", name: "normal", args: {}, attempt: 0 }];
          }
          return [];
        })),
        getHandler: (name: string) => {
          if (name === "spike") {
            return async () => {
              heapMB = 200;
              return "spiked";
            };
          }
          return async () => "done";
        },
        getHeapUsedBytes: () => heapMB * 1024 * 1024,
        isNode: true,
        completeBatch: vi.fn().mockImplementation(
          async (items: { taskId: string; result: unknown }[]) => {
            for (const item of items) {
              tracker.completed.push(item.taskId);
              if (item.taskId === "t1") heapMB = 30;
            }
            return [];
          },
        ),
        countPending: vi.fn().mockImplementation(async () => {
          return tracker.completed.length < 2 ? 1 : 0;
        }),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, SHORT_OPTIONS);

      await vi.advanceTimersByTimeAsync(500);
      expect(tracker.completed).toContain("t1");

      await vi.advanceTimersByTimeAsync(1_500);
      await loopDone;

      expect(tracker.completed).toContain("t2");
    });

    it("skips memory check entirely on non-Node runtime", async () => {
      const tracker = createTracker();

      const deps = makeDeps({
        ...claimMocks(
          vi.fn()
            .mockResolvedValueOnce([
              { _id: "t1", name: "handler", args: {}, attempt: 0 },
            ])
            .mockResolvedValue([]),
        ),
        getHandler: () => async () => "result",
        getHeapUsedBytes: () => 999 * 1024 * 1024,
        isNode: false,
        ...batchMocks(tracker),
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
        ...claimMocks(
          vi.fn()
            .mockResolvedValueOnce([
              { _id: "t1", name: "handler", args: {}, attempt: 0 },
            ])
            .mockResolvedValue([]),
        ),
        getHandler: () => async () => "result",
        getHeapUsedBytes: () => 999 * 1024 * 1024,
        isNode: true,
        ...batchMocks(tracker),
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
        ...claimMocks(
          vi.fn()
            .mockResolvedValueOnce([
              { _id: "unknown-1", name: "doesNotExist", args: {}, attempt: 0 },
              { _id: "known-1", name: "myHandler", args: {}, attempt: 0 },
            ])
            .mockResolvedValue([]),
        ),
        getHandler: (name: string) => {
          if (name === "myHandler") return async () => "ok";
          return undefined;
        },
        ...batchMocks(tracker),
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
        ...claimMocks(
          vi.fn()
            .mockResolvedValueOnce([
              { _id: "crash-1", name: "crasher", args: {}, attempt: 0 },
            ])
            .mockResolvedValue([]),
        ),
        getHandler: () => async () => {
          throw new Error("handler exploded");
        },
        ...batchMocks(tracker),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, SHORT_OPTIONS);
      await vi.advanceTimersByTimeAsync(2_000);
      await loopDone;

      expect(tracker.failed).toEqual([
        { id: "crash-1", error: "Error: handler exploded" },
      ]);
    });

    it("survives double failure (handler throws + failBatch throws)", async () => {
      const tracker = createTracker();

      const deps = makeDeps({
        ...claimMocks(
          vi.fn()
            .mockResolvedValueOnce([
              { _id: "double-fail", name: "crasher", args: {}, attempt: 0 },
              { _id: "healthy", name: "ok", args: {}, attempt: 0 },
            ])
            .mockResolvedValue([]),
        ),
        getHandler: (name: string) => {
          if (name === "crasher") return async () => { throw new Error("boom"); };
          return async () => "fine";
        },
        failBatch: vi.fn().mockImplementation(async () => {
          throw new Error("failBatch mutation crashed");
        }),
        completeBatch: vi.fn().mockImplementation(
          async (items: { taskId: string; result: unknown }[]) => {
            for (const item of items) tracker.completed.push(item.taskId);
            return [];
          },
        ),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, SHORT_OPTIONS);
      await vi.advanceTimersByTimeAsync(2_000);
      await loopDone;

      expect(tracker.completed).toContain("healthy");
      expect(console.error).toHaveBeenCalled();
    });

    it("finally block releases in-flight claims even when loop crashes", async () => {
      const slow = deferred();
      const tracker = createTracker();
      let listCallCount = 0;

      const deps = makeDeps({
        listPending: vi.fn().mockImplementation(async () => {
          listCallCount++;
          if (listCallCount === 1) return ["fast-1", "slow-1"];
          throw new Error("db crash");
        }),
        claimByIds: vi.fn().mockImplementation(async (ids: string[]) => {
          if (ids.includes("fast-1")) {
            return [
              { _id: "fast-1", name: "fast", args: {}, attempt: 0 },
              { _id: "slow-1", name: "slow", args: {}, attempt: 0 },
            ];
          }
          return [];
        }),
        getHandler: (name: string) => {
          if (name === "fast") return async () => "done";
          return () => slow.promise;
        },
        releaseClaims: vi.fn().mockImplementation(async (ids: string[]) => {
          tracker.released.push(...ids);
        }),
        executorDone: vi.fn(),
      });

      const loopPromise = _runExecutorLoop(deps, SHORT_OPTIONS)
        .then(() => null)
        .catch((e: unknown) => e);

      await vi.advanceTimersByTimeAsync(500);

      const result = await loopPromise;

      expect(result).toBeInstanceOf(Error);
      expect((result as Error).message).toBe("db crash");
      expect(tracker.released).toEqual(["slow-1"]);

      slow.resolve();
    });

    it("retries completeBatch on transient errors", async () => {
      const tracker = createTracker();
      let completeBatchCalls = 0;

      const deps = makeDeps({
        ...claimMocks(
          vi.fn()
            .mockResolvedValueOnce([
              { _id: "t1", name: "handler", args: {}, attempt: 0 },
            ])
            .mockResolvedValue([]),
        ),
        getHandler: () => async () => "result",
        completeBatch: vi.fn().mockImplementation(async (items: { taskId: string; result: unknown }[]) => {
          completeBatchCalls++;
          if (completeBatchCalls === 1) {
            throw new Error("Too many concurrent commits in a short period of time.");
          }
          for (const item of items) tracker.completed.push(item.taskId);
          return [];
        }),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, SHORT_OPTIONS);
      await vi.advanceTimersByTimeAsync(2_000);
      await loopDone;

      expect(completeBatchCalls).toBeGreaterThanOrEqual(2);
      expect(tracker.completed).toEqual(["t1"]);
    });

    it("retries executorDone on transient errors", async () => {
      let executorDoneCalls = 0;

      const deps = makeDeps({
        ...claimMocks(vi.fn().mockResolvedValue([])),
        countPending: vi.fn().mockResolvedValue(0),
        executorDone: vi.fn().mockImplementation(async () => {
          executorDoneCalls++;
          if (executorDoneCalls <= 2) {
            throw new Error("Documents read from or written to the \"batchConfig\" table changed while this mutation was being run");
          }
        }),
      });

      const loopDone = _runExecutorLoop(deps, SHORT_OPTIONS);
      await vi.advanceTimersByTimeAsync(2_000);
      await loopDone;

      expect(executorDoneCalls).toBe(3);
    });
  });

  // ─── OnComplete dispatch ──────────────────────────────────────────────

  describe("onComplete dispatch", () => {
    it("dispatches onComplete items returned from completeBatch", async () => {
      const tracker = createTracker();
      const onCompleteItem = {
        fnHandle: "function:pipeline/afterStep1",
        workId: "t1",
        context: { jobId: "j1" },
        result: { kind: "success", returnValue: "hello" },
      };

      const deps = makeDeps({
        ...claimMocks(
          vi.fn()
            .mockResolvedValueOnce([
              { _id: "t1", name: "handler", args: {}, attempt: 0 },
            ])
            .mockResolvedValue([]),
        ),
        getHandler: () => async () => "hello",
        completeBatch: vi.fn().mockImplementation(async () => {
          tracker.completed.push("t1");
          return [onCompleteItem];
        }),
        dispatchOnCompleteBatch: vi.fn().mockImplementation(async (items) => {
          tracker.onCompleteDispatched.push(...items);
        }),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, SHORT_OPTIONS);
      await vi.advanceTimersByTimeAsync(2_000);
      await loopDone;

      expect(tracker.completed).toEqual(["t1"]);
      expect(tracker.onCompleteDispatched).toEqual([onCompleteItem]);
    });

    it("dispatches onComplete items returned from failBatch", async () => {
      const tracker = createTracker();
      const onCompleteItem = {
        fnHandle: "function:pipeline/afterFail",
        workId: "t1",
        context: { jobId: "j1" },
        result: { kind: "failed", error: "boom" },
      };

      const deps = makeDeps({
        ...claimMocks(
          vi.fn()
            .mockResolvedValueOnce([
              { _id: "t1", name: "handler", args: {}, attempt: 0 },
            ])
            .mockResolvedValue([]),
        ),
        getHandler: () => async () => { throw new Error("boom"); },
        failBatch: vi.fn().mockImplementation(async () => {
          tracker.failed.push({ id: "t1", error: "boom" });
          return [onCompleteItem];
        }),
        dispatchOnCompleteBatch: vi.fn().mockImplementation(async (items) => {
          tracker.onCompleteDispatched.push(...items);
        }),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, SHORT_OPTIONS);
      await vi.advanceTimersByTimeAsync(2_000);
      await loopDone;

      expect(tracker.onCompleteDispatched).toEqual([onCompleteItem]);
    });
  });

  // ─── Concurrency ───────────────────────────────────────────────────────

  describe("concurrency", () => {
    it("respects maxConcurrencyPerWorker limit", async () => {
      const tracker = createTracker();
      const claimImpl = vi.fn()
        .mockResolvedValueOnce(
          Array.from({ length: 5 }, (_, i) => ({
            _id: `t${i}`, name: "handler", args: {}, attempt: 0,
          })),
        )
        .mockResolvedValue([]);
      const mocks = claimMocks(claimImpl);

      const deps = makeDeps({
        ...mocks,
        getHandler: () => async () => {
          await new Promise((r) => setTimeout(r, 50));
          return "done";
        },
        ...batchMocks(tracker),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, {
        ...SHORT_OPTIONS,
        maxConcurrencyPerWorker: 3,
      });

      await vi.advanceTimersByTimeAsync(2_000);
      await loopDone;

      expect(tracker.completed.length).toBe(5);
      expect(mocks.listPending).toHaveBeenCalledWith(3);
    });
  });

  // ─── Retry scenarios (executor perspective) ───────────────────────────

  describe("retry scenarios", () => {
    it("re-claims a task that was retried (failed then re-queued by component)", async () => {
      const tracker = createTracker();
      let claimCount = 0;
      let handlerCallCount = 0;

      const deps = makeDeps({
        ...claimMocks(vi.fn().mockImplementation(async () => {
          claimCount++;
          if (claimCount === 1) {
            return [{ _id: "retry-task", name: "flaky", args: {}, attempt: 0 }];
          }
          if (claimCount === 2) {
            return [{ _id: "retry-task", name: "flaky", args: {}, attempt: 1 }];
          }
          return [];
        })),
        getHandler: () => async () => {
          handlerCallCount++;
          if (handlerCallCount === 1) throw new Error("transient failure");
          return "success on retry";
        },
        ...batchMocks(tracker),
        countPending: vi.fn().mockImplementation(async () => {
          return tracker.completed.length === 0 ? 1 : 0;
        }),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, SHORT_OPTIONS);
      await vi.advanceTimersByTimeAsync(2_000);
      await loopDone;

      expect(tracker.failed).toHaveLength(1);
      expect(tracker.completed).toContain("retry-task");
    });

    it("handler returning undefined becomes null (Convex serialization)", async () => {
      const completedWith: Array<{ taskId: string; result: unknown }> = [];

      const deps = makeDeps({
        ...claimMocks(
          vi.fn()
            .mockResolvedValueOnce([
              { _id: "t1", name: "void-handler", args: {}, attempt: 0 },
            ])
            .mockResolvedValue([]),
        ),
        getHandler: () => async () => undefined,
        completeBatch: vi.fn().mockImplementation(
          async (items: { taskId: string; result: unknown }[]) => {
            completedWith.push(...items);
            return [];
          },
        ),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, SHORT_OPTIONS);
      await vi.advanceTimersByTimeAsync(2_000);
      await loopDone;

      expect(completedWith).toEqual([{ taskId: "t1", result: null }]);
    });
  });

  // ─── Exit conditions ──────────────────────────────────────────────────

  describe("exit conditions", () => {
    it("exits immediately when no pending work and nothing in flight", async () => {
      const deps = makeDeps({
        ...claimMocks(vi.fn().mockResolvedValue([])),
        countPending: vi.fn().mockResolvedValue(0),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, SHORT_OPTIONS);
      await vi.advanceTimersByTimeAsync(500);
      await loopDone;

      expect(deps.executorDone).toHaveBeenCalledWith(false);
    });

    it("signals startMore=true when pending work remains at exit", async () => {
      const deps = makeDeps({
        ...claimMocks(vi.fn().mockResolvedValue([])),
        countPending: vi.fn().mockResolvedValue(5),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, {
        ...SHORT_OPTIONS,
        claimDeadlineMs: 300,
        softDeadlineMs: 599_500,
      });

      await vi.advanceTimersByTimeAsync(1_000);
      await loopDone;

      expect(deps.executorDone).toHaveBeenCalledWith(true);
    });

    it("polls at pollIntervalMs when tasks exist with future readyAt", async () => {
      let pollCount = 0;
      const deps = makeDeps({
        ...claimMocks(vi.fn().mockResolvedValue([])),
        countPending: vi.fn().mockImplementation(async () => {
          pollCount++;
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

      expect(pollCount).toBeGreaterThanOrEqual(3);
      expect(deps.executorDone).toHaveBeenCalledWith(false);
    });
  });

  // ─── Black-box behavioral edge cases ──────────────────────────────────

  describe("all tasks fail", () => {
    it("every handler throws but executor completes cleanly", async () => {
      const tracker = createTracker();

      const deps = makeDeps({
        ...claimMocks(
          vi.fn()
            .mockResolvedValueOnce([
              { _id: "f1", name: "broken", args: {}, attempt: 0 },
              { _id: "f2", name: "broken", args: {}, attempt: 0 },
              { _id: "f3", name: "broken", args: {}, attempt: 0 },
            ])
            .mockResolvedValue([]),
        ),
        getHandler: () => async () => {
          throw new Error("everything is broken");
        },
        ...batchMocks(tracker),
        executorDone: vi.fn().mockImplementation(async (startMore: boolean) => {
          tracker.markDone(startMore);
        }),
      });

      const loopDone = _runExecutorLoop(deps, SHORT_OPTIONS);
      await vi.advanceTimersByTimeAsync(2_000);
      await loopDone;

      expect(tracker.failed).toHaveLength(3);
      expect(tracker.completed).toHaveLength(0);
      expect(tracker.done).toBe(true);
    });
  });

  describe("mixed success and failure", () => {
    it("some handlers succeed, some fail, all reported correctly", async () => {
      const tracker = createTracker();

      const deps = makeDeps({
        ...claimMocks(
          vi.fn()
            .mockResolvedValueOnce([
              { _id: "ok-1", name: "good", args: {}, attempt: 0 },
              { _id: "bad-1", name: "bad", args: {}, attempt: 0 },
              { _id: "ok-2", name: "good", args: {}, attempt: 0 },
              { _id: "bad-2", name: "bad", args: {}, attempt: 0 },
            ])
            .mockResolvedValue([]),
        ),
        getHandler: (name: string) => {
          if (name === "good") return async () => "success";
          return async () => { throw new Error("fail"); };
        },
        ...batchMocks(tracker),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, SHORT_OPTIONS);
      await vi.advanceTimersByTimeAsync(2_000);
      await loopDone;

      expect(tracker.completed.sort()).toEqual(["ok-1", "ok-2"]);
      expect(tracker.failed.map((f) => f.id).sort()).toEqual(["bad-1", "bad-2"]);
    });
  });

  describe("handler return values", () => {
    it("passes handler result through to completeBatch", async () => {
      const completedWith: Array<{ taskId: string; result: unknown }> = [];

      const deps = makeDeps({
        ...claimMocks(
          vi.fn()
            .mockResolvedValueOnce([
              { _id: "t1", name: "handler", args: {}, attempt: 0 },
            ])
            .mockResolvedValue([]),
        ),
        getHandler: () => async () => ({
          large: "object",
          nested: { array: [1, 2, 3] },
          number: 42,
        }),
        completeBatch: vi.fn().mockImplementation(
          async (items: { taskId: string; result: unknown }[]) => {
            completedWith.push(...items);
            return [];
          },
        ),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, SHORT_OPTIONS);
      await vi.advanceTimersByTimeAsync(2_000);
      await loopDone;

      expect(completedWith).toEqual([{
        taskId: "t1",
        result: { large: "object", nested: { array: [1, 2, 3] }, number: 42 },
      }]);
    });

    it("null return value is passed through as-is", async () => {
      const completedWith: Array<{ taskId: string; result: unknown }> = [];

      const deps = makeDeps({
        ...claimMocks(
          vi.fn()
            .mockResolvedValueOnce([
              { _id: "t1", name: "handler", args: {}, attempt: 0 },
            ])
            .mockResolvedValue([]),
        ),
        getHandler: () => async () => null,
        completeBatch: vi.fn().mockImplementation(
          async (items: { taskId: string; result: unknown }[]) => {
            completedWith.push(...items);
            return [];
          },
        ),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, SHORT_OPTIONS);
      await vi.advanceTimersByTimeAsync(2_000);
      await loopDone;

      expect(completedWith[0].result).toBeNull();
    });
  });

  describe("listPending returns more IDs than claimByIds gets", () => {
    it("handles race where some tasks were claimed between list and claim", async () => {
      const tracker = createTracker();

      const deps = makeDeps({
        // listPending returns 5 IDs, but claimByIds only gets 2
        // (the other 3 were claimed by another executor)
        listPending: vi.fn()
          .mockResolvedValueOnce(["t1", "t2", "t3", "t4", "t5"])
          .mockResolvedValue([]),
        claimByIds: vi.fn()
          .mockResolvedValueOnce([
            { _id: "t1", name: "handler", args: {}, attempt: 0 },
            { _id: "t4", name: "handler", args: {}, attempt: 0 },
          ])
          .mockResolvedValue([]),
        getHandler: () => async () => "done",
        ...batchMocks(tracker),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, SHORT_OPTIONS);
      await vi.advanceTimersByTimeAsync(2_000);
      await loopDone;

      // Should complete the 2 it actually got
      expect(tracker.completed.sort()).toEqual(["t1", "t4"]);
    });
  });

  describe("onComplete dispatch failure", () => {
    it("logs error but doesn't crash executor when dispatch fails", async () => {
      const tracker = createTracker();

      const deps = makeDeps({
        ...claimMocks(
          vi.fn()
            .mockResolvedValueOnce([
              { _id: "t1", name: "handler", args: {}, attempt: 0 },
            ])
            .mockResolvedValue([]),
        ),
        getHandler: () => async () => "result",
        completeBatch: vi.fn().mockResolvedValue([
          {
            fnHandle: "function://broken",
            workId: "t1",
            context: {},
            result: { kind: "success", returnValue: "result" },
          },
        ]),
        dispatchOnCompleteBatch: vi.fn().mockRejectedValue(
          new Error("onComplete handler crashed"),
        ),
        executorDone: vi.fn().mockImplementation(async (startMore: boolean) => {
          tracker.markDone(startMore);
        }),
      });

      const loopDone = _runExecutorLoop(deps, SHORT_OPTIONS);
      await vi.advanceTimersByTimeAsync(2_000);
      await loopDone;

      // Executor should still finish cleanly
      expect(tracker.done).toBe(true);
      expect(console.error).toHaveBeenCalled();
    });
  });

  describe("releaseClaims failure at shutdown", () => {
    it("logs error but doesn't crash when releaseClaims fails", async () => {
      const slow = deferred();

      const deps = makeDeps({
        ...claimMocks(
          vi.fn()
            .mockResolvedValueOnce([
              { _id: "slow-1", name: "slow", args: {}, attempt: 0 },
            ])
            .mockResolvedValue([]),
        ),
        getHandler: () => () => slow.promise,
        releaseClaims: vi.fn().mockRejectedValue(
          new Error("release mutation failed"),
        ),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, SHORT_OPTIONS);
      await vi.advanceTimersByTimeAsync(2_000);
      await loopDone;

      // Should have tried to release
      expect(deps.releaseClaims).toHaveBeenCalled();
      expect(console.error).toHaveBeenCalled();

      slow.resolve();
    });
  });

  describe("countPending failure", () => {
    it("treats countPending failure as no work (avoids infinite retry loop)", async () => {
      const deps = makeDeps({
        ...claimMocks(vi.fn().mockResolvedValue([])),
        countPending: vi.fn().mockRejectedValue(
          new Error("query timed out"),
        ),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, SHORT_OPTIONS);
      await vi.advanceTimersByTimeAsync(2_000);
      await loopDone;

      // Should exit cleanly despite countPending failure
      expect(deps.executorDone).toHaveBeenCalledWith(false);
    });
  });

  describe("multiple claim rounds", () => {
    it("claims new tasks while previous tasks are still running", async () => {
      const tracker = createTracker();
      let claimRound = 0;
      const slow = deferred();

      const deps = makeDeps({
        ...claimMocks(vi.fn().mockImplementation(async () => {
          claimRound++;
          if (claimRound === 1) {
            // First round: slow task fills 1 of 3 slots
            return [{ _id: "slow", name: "slow", args: {}, attempt: 0 }];
          }
          if (claimRound === 2) {
            // Second round: 2 fast tasks fill remaining slots
            return [
              { _id: "fast-1", name: "fast", args: {}, attempt: 0 },
              { _id: "fast-2", name: "fast", args: {}, attempt: 0 },
            ];
          }
          return [];
        })),
        getHandler: (name: string) => {
          if (name === "slow") return () => slow.promise;
          return async () => "done";
        },
        ...batchMocks(tracker),
        countPending: vi.fn().mockImplementation(async () => {
          return claimRound < 2 ? 1 : 0;
        }),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, {
        ...SHORT_OPTIONS,
        maxConcurrencyPerWorker: 3,
      });

      await vi.advanceTimersByTimeAsync(500);

      // Fast tasks should complete while slow one is still running
      expect(tracker.completed).toContain("fast-1");
      expect(tracker.completed).toContain("fast-2");

      slow.resolve();
      await vi.advanceTimersByTimeAsync(1_500);
      await loopDone;

      expect(tracker.completed).toContain("slow");
    });
  });

  describe("transient error on listPending", () => {
    it("retries claiming after transient listPending error", async () => {
      const tracker = createTracker();
      let listCallCount = 0;

      const deps = makeDeps({
        listPending: vi.fn().mockImplementation(async () => {
          listCallCount++;
          if (listCallCount === 1) {
            throw new Error("couldn't be completed because of a conflict");
          }
          if (listCallCount === 2) {
            return ["t1"];
          }
          return [];
        }),
        claimByIds: vi.fn().mockImplementation(async (ids: string[]) => {
          if (ids.includes("t1")) {
            return [{ _id: "t1", name: "handler", args: {}, attempt: 0 }];
          }
          return [];
        }),
        getHandler: () => async () => "recovered",
        ...batchMocks(tracker),
        executorDone: vi.fn().mockResolvedValue(undefined),
      });

      const loopDone = _runExecutorLoop(deps, SHORT_OPTIONS);
      await vi.advanceTimersByTimeAsync(2_000);
      await loopDone;

      expect(tracker.completed).toContain("t1");
    });
  });

  describe("executorDone fatal error", () => {
    it("throws when executorDone fails with non-transient error", async () => {
      const deps = makeDeps({
        ...claimMocks(vi.fn().mockResolvedValue([])),
        countPending: vi.fn().mockResolvedValue(0),
        executorDone: vi.fn().mockRejectedValue(
          new Error("Internal server error"),
        ),
      });

      const loopPromise = _runExecutorLoop(deps, SHORT_OPTIONS)
        .then(() => null)
        .catch((e: unknown) => e);

      await vi.advanceTimersByTimeAsync(2_000);
      const result = await loopPromise;

      expect(result).toBeInstanceOf(Error);
      expect((result as Error).message).toBe("Internal server error");
    });
  });
});
