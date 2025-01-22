/// <reference types="vite/client" />

import { convexTest } from "convex-test";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import schema from "./schema";
import componentSchema from "../../src/component/schema";
import cronsSchema from "../../node_modules/@convex-dev/crons/src/component/schema";
import { api } from "./_generated/api";

const modules = import.meta.glob("./**/*.ts");
const componentModules = import.meta.glob("../../src/component/**/*.ts");
const cronsModules = import.meta.glob(
  "../../node_modules/@convex-dev/crons/src/component/**/*.ts"
);

describe("workpool", () => {
  async function setupTest() {
    const t = convexTest(schema, modules);
    t.registerComponent("workpool", componentSchema, componentModules);
    t.registerComponent("workpool/crons", cronsSchema, cronsModules);
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
      kind: "pending",
    });
    expect(await t.query(api.example.queryData, {})).toEqual([]);
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    expect(await t.query(api.example.status, { id })).toEqual({
      kind: "completed",
      completionStatus: "success",
    });
    expect(await t.query(api.example.queryData, {})).toEqual([1]);
  });

  test("enqueueMany with low parallelism", async () => {
    // 20 is larger than max parallelism 3
    for (let i = 0; i < 20; i++) {
      await t.mutation(api.example.enqueueOneMutation, { data: i });
    }
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    expect(await t.query(api.example.queryData, {})).toEqual([
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
    ]);
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
});
