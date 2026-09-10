import { convexTest } from "convex-test";
import batchWorker from "@convex-dev/batch-worker/test";
import {
  anyApi,
  createFunctionHandle,
  type ApiFromModules,
  type FunctionArgs,
} from "convex/server";
import { v } from "convex/values";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import { internal } from "./_generated/api.js";
import { internalMutation } from "./_generated/server.js";
import type { Id } from "./_generated/dataModel.js";
import { recoveryHandler } from "./recovery.js";
import schema from "./schema.js";
import { vResult, type RunResult } from "./shared.js";
import * as kick from "./kick.js";
import { completionTransactionLimits } from "./limits.js";

const modules = import.meta.glob("./**/*.ts");

const calls = vi.fn<(kind: string) => void>();
const fixtures = {
  work: internalMutation({
    args: { fail: v.boolean(), padding: v.optional(v.string()) },
    returns: v.id("payload"),
    handler: async (ctx, { fail }) => {
      calls("work");
      const id = await ctx.db.insert("payload", { args: { effect: "work" } });
      if (fail) throw new Error("work failed");
      return id;
    },
  }),
  callback: internalMutation({
    args: {
      workId: v.id("work"),
      context: v.object({
        failOnSuccess: v.boolean(),
        padding: v.optional(v.string()),
      }),
      result: vResult,
    },
    returns: v.null(),
    handler: async (ctx, { context, result }) => {
      calls(result.kind);
      if (result.kind === "success") {
        // The callback must be able to read the nested work's writes.
        const effect = await ctx.db.get(
          "payload",
          result.returnValue as Id<"payload">,
        );
        expect(effect?.args?.effect).toBe("work");
      }
      await ctx.db.insert("payload", { args: { effect: result.kind } });
      if (context.failOnSuccess && result.kind === "success") {
        throw new Error("callback failed");
      }
      return null;
    },
  }),
};
const refs = (
  anyApi as unknown as ApiFromModules<{ workerFixtures: typeof fixtures }>
).workerFixtures;

describe("transactional mutation completion", () => {
  function setup() {
    const t = convexTest({
      schema,
      modules: { ...modules, "./workerFixtures.ts": async () => fixtures },
      transactionLimits: true,
    });
    batchWorker.register(t);
    return t;
  }
  let t: ReturnType<typeof setup>;
  beforeEach(() => {
    vi.useFakeTimers();
    calls.mockClear();
    t = setup();
  });
  afterEach(async () => {
    vi.restoreAllMocks();
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    vi.useRealTimers();
  });
  const drain = () => t.finishAllScheduledFunctions(vi.runAllTimers);
  const effects = () =>
    t.run(async (ctx) =>
      (await ctx.db.query("payload").collect()).flatMap((p) =>
        p.args?.effect ? [p.args.effect] : [],
      ),
    );
  async function start({
    transactional = true,
    failWork = false,
    failCallback = false,
    callback = true,
    statuses,
    largePayload = false,
  }: {
    transactional?: boolean;
    failWork?: boolean;
    failCallback?: boolean;
    callback?: boolean;
    statuses?: RunResult["kind"][];
    largePayload?: boolean;
  } = {}) {
    return t.run(async (ctx) => {
      const fnHandle = await createFunctionHandle(refs.work);
      const fnArgs = {
        fail: failWork,
        ...(largePayload ? { padding: "a".repeat(12_000) } : {}),
      };
      const context = {
        failOnSuccess: failCallback,
        ...(largePayload ? { padding: "c".repeat(12_000) } : {}),
      };
      const payloadId = largePayload
        ? await ctx.db.insert("payload", { args: fnArgs, context })
        : undefined;
      const workId = await ctx.db.insert("work", {
        fnHandle,
        fnName: "work",
        fnType: "mutation",
        attempts: 0,
        fnArgs: largePayload ? undefined : fnArgs,
        payloadId,
        payloadSize: largePayload ? 25_000 : undefined,
        completeTransactionally: transactional,
        onComplete: callback
          ? {
              fnHandle: await createFunctionHandle(refs.callback),
              context: largePayload ? undefined : context,
              statuses,
            }
          : undefined,
      });
      const args: FunctionArgs<typeof internal.worker.runMutationWrapper> = {
        workId,
        fnHandle,
        fnArgs: largePayload ? undefined : fnArgs,
        payloadId,
        fnType: "mutation",
        attempt: 0,
        logLevel: "ERROR",
        completeTransactionally: transactional,
      };
      const scheduledId = await ctx.scheduler.runAfter(
        0,
        internal.worker.runMutationWrapper,
        args,
      );
      await ctx.db.insert("globals", { maxParallelism: 1, logLevel: "ERROR" });
      // The main loop normally records this when it schedules the wrapper.
      await ctx.db.insert("internalState", {
        generation: 0n,
        segmentCursors: { incoming: 0n, completion: 0n, cancelation: 0n },
        lastRecovery: 0n,
        report: {
          completed: 0,
          succeeded: 0,
          failed: 0,
          canceled: 0,
          retries: 0,
          lastReportTs: Date.now(),
        },
        running: [{ workId, scheduledId, started: Date.now() }],
      });
      return {
        workId,
        scheduledId,
        attempt: 0,
        started: Date.now(),
        payloadId,
        args,
      };
    });
  }
  async function recover(
    job: Awaited<ReturnType<typeof start>>,
    error: string,
  ) {
    const { workId, scheduledId, attempt, started } = job;
    const jobs = [{ workId, scheduledId, attempt, started }];
    await t.run(async (ctx) => {
      const scheduled = await ctx.db.system.get(
        "_scheduled_functions",
        job.scheduledId,
      );
      expect(scheduled?.state.kind).toBe("failed");
      if (!scheduled) throw new Error("Missing scheduled job");
      // convex-test 0.0.53 records real scheduler failures without the required
      // error string. Supply only that missing field; recovery runs unchanged.
      const get = vi.spyOn(ctx.db.system, "get").mockResolvedValue({
        ...scheduled,
        state: { kind: "failed", error },
      });
      await recoveryHandler(ctx, { jobs });
      get.mockRestore();
    });
    await drain();
    // A second recovery scan must not deliver another callback.
    await t.mutation(internal.recovery.recover, { jobs });
    await drain();
  }
  async function expectFinished(job: Awaited<ReturnType<typeof start>>) {
    await t.run(async (ctx) => {
      expect(await ctx.db.get("work", job.workId)).toBeNull();
      if (job.payloadId)
        expect(await ctx.db.get("payload", job.payloadId)).toBeNull();
      expect(await ctx.db.query("pendingCompletion").collect()).toEqual([]);
      expect((await ctx.db.query("internalState").unique())?.running).toEqual(
        [],
      );
    });
  }

  test.each([false, true])(
    "commits work, callback, and cleanup together (large payload: %s)",
    async (largePayload) => {
      const job = await start({ largePayload });
      await drain();
      expect(await effects()).toEqual(["work", "success"]);
      expect(calls.mock.calls.flat()).toEqual(["work", "success"]);
      await expectFinished(job);
      await t.run(async (ctx) => {
        const scheduled = await ctx.db.system
          .query("_scheduled_functions")
          .collect();
        expect(
          scheduled.some(
            (s) =>
              s.name === "complete:complete" ||
              s.name === "workerFixtures:callback",
          ),
        ).toBe(false);
      });
    },
  );

  test.each([{}, { statuses: ["failed"] as const }, { statuses: [] as const }])(
    "finishes without a success callback ($statuses)",
    async (options) => {
      const job = await start({
        callback: "statuses" in options,
        statuses: options.statuses && [...options.statuses],
      });
      await drain();
      expect(await effects()).toEqual(["work"]);
      expect(calls.mock.calls.flat()).toEqual(["work"]);
      await expectFinished(job);
    },
  );

  test.each(["callback", "bookkeeping"] as const)(
    "%s failure rolls back all writes, then recovery records failure once",
    async (failure) => {
      const job = await start({
        failCallback: failure === "callback",
        largePayload: true,
      });
      if (failure === "bookkeeping") {
        // Fail after the work and callback wrote data and cleanup deleted the job.
        vi.spyOn(kick, "kickMainLoop").mockRejectedValueOnce(
          new Error("bookkeeping failed"),
        );
      }
      await drain();
      expect(await effects()).toEqual([]);
      expect(calls.mock.calls.flat()).toEqual(["work", "success"]);
      await t.run(async (ctx) => {
        expect(await ctx.db.get("work", job.workId)).toMatchObject({
          attempts: 0,
        });
        expect(await ctx.db.get("payload", job.payloadId!)).not.toBeNull();
        expect(await ctx.db.query("pendingCompletion").collect()).toEqual([]);
      });
      vi.restoreAllMocks();
      await recover(job, `${failure} failed`);
      expect(await effects()).toEqual(["failed"]);
      expect(calls.mock.calls.flat()).toEqual(["work", "success", "failed"]);
      await expectFinished(job);
    },
  );

  test("recovery respects a success-only filter after rollback", async () => {
    const job = await start({ failCallback: true, statuses: ["success"] });
    await drain();
    await recover(job, "callback failed");
    expect(await effects()).toEqual([]);
    expect(calls.mock.calls.flat()).toEqual(["work", "success"]);
    await expectFinished(job);
  });

  test("work failure keeps the usual failure-completion path", async () => {
    const job = await start({ failWork: true });
    await drain();
    expect(await effects()).toEqual(["failed"]);
    expect(calls.mock.calls.flat()).toEqual(["work", "failed"]);
    await expectFinished(job);
  });

  test("default completion preserves work when a success callback fails", async () => {
    const job = await start({ transactional: false, failCallback: true });
    await drain();
    expect(await effects()).toEqual(["work"]);
    expect(calls.mock.calls.flat()).toEqual(["work", "success"]);
    await expectFinished(job);
  });

  test("replaying a completed transactional wrapper does not repeat the work", async () => {
    const job = await start();
    await drain();
    await t.mutation(internal.worker.runMutationWrapper, job.args);
    expect(await effects()).toEqual(["work", "success"]);
    expect(calls.mock.calls.flat()).toEqual(["work", "success"]);
  });

  test("nested budgets use remaining capacity after earlier writes", async () => {
    await t.run(async (ctx) => {
      const before = await completionTransactionLimits(ctx);
      await ctx.db.insert("payload", {
        args: { padding: "x".repeat(100_000) },
      });
      const after = await completionTransactionLimits(ctx);
      expect(after.bytesWritten).toBeLessThan(before.bytesWritten! - 100_000);
      expect(after.documentsWritten).toBe(before.documentsWritten! - 1);
      const metrics = await ctx.meta.getTransactionMetrics();
      for (const key of Object.keys(after) as (keyof typeof after)[]) {
        expect(after[key]).toBeGreaterThanOrEqual(0);
        expect(after[key]).toBeLessThan(metrics[key].remaining);
      }
    });
  });
});
