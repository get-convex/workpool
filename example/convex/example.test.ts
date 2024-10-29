/// <reference types="vite/client" />

import { convexTest } from "convex-test";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest"
import schema from "./schema";
import componentSchema from "../../src/component/schema";
import { api } from "./_generated/api";

const modules = import.meta.glob("./**/*.ts");
const componentModules = import.meta.glob("../../src/component/**/*.ts");

describe("workpool", () => {
  async function setupTest() {
    const t = convexTest(schema, modules);
    t.registerComponent("workpool", componentSchema, componentModules);
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
    const id = await t.mutation(api.example.enqueueOneMutation, {data: 1});
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    expect(await t.query(api.example.status, { id })).toEqual({
      kind: "success",
      result: 1,
    });
  })
});
