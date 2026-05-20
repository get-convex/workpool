/// <reference types="vite/client" />

import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import workpool from "@convex-dev/workpool/test";
import { api } from "../_generated/api";
import { initConvexTest } from "../setup.test";

describe("NonRetryableError", () => {
  async function setupTest() {
    const t = initConvexTest();
    workpool.register(t, "smallPool");
    return t;
  }

  let t: Awaited<ReturnType<typeof setupTest>>;

  beforeEach(async () => {
    vi.useFakeTimers();
    t = await setupTest();
  });

  afterEach(async () => {
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    vi.useRealTimers();
  });

  test("skips remaining action retries", async () => {
    const id = await t.mutation(
      api.test.nonRetryable.enqueueTerminalAction,
      {},
    );

    await t.finishAllScheduledFunctions(vi.runAllTimers);

    expect(await t.query(api.example.status, { id })).toEqual({
      state: "finished",
    });
    // The action can persist an attempt marker before throwing.
    expect(await t.query(api.test.nonRetryable.terminalAttemptCount, {})).toBe(
      1,
    );
    // onComplete only receives the client-visible RunResult message.
    expect(
      await t.query(api.test.nonRetryable.terminalCompletionErrors, {}),
    ).toEqual(["terminal failure"]);
  });

  test("passes mutation errors through to onComplete", async () => {
    const id = await t.mutation(
      api.test.nonRetryable.enqueueTerminalMutationWithRetry,
      {},
    );

    await t.finishAllScheduledFunctions(vi.runAllTimers);

    expect(await t.query(api.example.status, { id })).toEqual({
      state: "finished",
    });
    // Nested mutation errors cross a Convex boundary, so this guards the serialized path.
    expect(
      await t.query(api.test.nonRetryable.terminalCompletionErrors, {}),
    ).toEqual(["terminal mutation failure"]);
  });
});
