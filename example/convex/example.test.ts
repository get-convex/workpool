/// <reference types="vite/client" />

import { convexTest } from "convex-test";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import schema from "./schema";
import componentSchema from "../node_modules/@convex-dev/workpool/src/component/schema";
import { api } from "./_generated/api";

const modules = import.meta.glob("./**/*.ts");
const componentModules = import.meta.glob(
  "../node_modules/@convex-dev/workpool/src/component/**/*.ts"
);

describe("workpool", () => {
  async function setupTest() {
    const t = convexTest(schema, modules);
    t.registerComponent("bigPool", componentSchema, componentModules);
    t.registerComponent("smallPool", componentSchema, componentModules);
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

  test("enqueue and get status", async () => {
    const id = await t.mutation(api.example.enqueueOneMutation, { data: 1 });
    expect(await t.query(api.example.status, { id })).toEqual({
      state: "pending",
      previousAttempts: 0,
    });
    expect(await t.query(api.example.queryData, {})).toEqual([]);
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    expect(await t.query(api.example.status, { id })).toEqual({
      state: "finished",
    });
    expect(await t.query(api.example.queryData, {})).toEqual([1]);
  });

  test("enqueueQuery executes successfully", async () => {
    const id = await t.mutation(api.example.enqueueOneQuery, { add: 42 });
    expect(await t.query(api.example.status, { id })).toEqual({
      state: "pending",
      previousAttempts: 0,
    });
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    expect(await t.query(api.example.status, { id })).toEqual({
      state: "finished",
    });
  });

  test("enqueueMany with low parallelism", async () => {
    // 20 is larger than max parallelism 3
    for (let i = 0; i < 20; i++) {
      await t.mutation(api.example.enqueueOneMutation, { data: i });
    }
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    expect(await t.query(api.example.queryData, {})).toEqual(
      expect.arrayContaining([
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
      ])
    );
  });

  test("enqueueMany with high parallelism", async () => {
    // enqueues 20 with max parallelism 30 but batch size 10
    for (let i = 0; i < 20; i++) {
      await t.mutation(api.example.highPriMutation, { data: i });
    }
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    expect(await t.query(api.example.queryData, {})).toEqual([
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
    ]);
  });

  test("cancelation", async () => {
    const id = await t.mutation(api.example.enqueueOneMutation, { data: 1 });
    await t.mutation(api.example.cancelMutation, { id });
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    expect(await t.query(api.example.status, { id })).toEqual({
      state: "finished",
    });
  });
});
