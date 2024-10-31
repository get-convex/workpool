/// <reference types="vite/client" />

import { convexTest } from "convex-test";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest"
import schema from "./schema";
import componentSchema from "../../src/component/schema";
import cronsSchema from "../../node_modules/@convex-dev/crons/src/component/schema";
import { api, components } from "./_generated/api";

const modules = import.meta.glob("./**/*.ts");
const componentModules = import.meta.glob("../../src/component/**/*.ts");
const cronsModules = import.meta.glob("../../node_modules/@convex-dev/crons/src/component/**/*.ts");

describe("workpool", () => {
  async function setupTest() {
    const t = convexTest(schema, modules);
    t.registerComponent("workpool", componentSchema, componentModules);
    t.registerComponent("workpool/crons", cronsSchema, cronsModules);
    return t;
  }

  let t: Awaited<ReturnType<typeof setupTest>>;

  beforeEach(async () => {
    //vi.useFakeTimers();
    t = await setupTest();
    await t.mutation(components.workpool.public.startMainLoop, {});
  });

  afterEach(async () => {
    //await t.finishAllScheduledFunctions(vi.runAllTimers);
    //vi.useRealTimers();
  });

  test.only("enqueue and get status", async () => {
    const id = await t.mutation(api.example.enqueueOneMutation, {data: 1});
    await sleep(2000);
    expect(await t.query(api.example.status, { id })).toEqual({
      kind: "success",
      result: 1,
    });
  })

  test("drop in replacement for ctx", async () => {
    const result = await t.action(api.example.doSomethingInPool, {});
    expect(result).toEqual(3);
  });
});

async function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
