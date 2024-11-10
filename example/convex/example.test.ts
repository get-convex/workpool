/// <reference types="vite/client" />

import { convexTest } from "convex-test";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import schema from "./schema";
import componentSchema from "../../src/component/schema";
import cronsSchema from "../../node_modules/@convex-dev/crons/src/component/schema";
import { api, components } from "./_generated/api";

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

  async function runToCompletion() {
    // We don't want to advance time so far that the cleanup runs and deletes
    // the work before we can check the status. So stop cleanup altogether.
    await t.mutation(components.workpool.public.stopCleanup, {});
    // Run a few loops of the mainLoop to process the work.
    // Note we can't call `t.finishAllScheduledFunctions` here because that
    // would loop forever.
    for (let i = 0; i < 10; i++) {
      vi.runAllTimers();
      await t.finishInProgressScheduledFunctions();
    }
  }

  beforeEach(async () => {
    vi.useFakeTimers();
    t = await setupTest();
    await t.mutation(components.workpool.public.startMainLoop, {});
  });

  afterEach(async () => {
    await t.mutation(components.workpool.public.stopMainLoop, {});
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    vi.useRealTimers();
  });

  test("enqueue and get status", async () => {
    const id = await t.mutation(api.example.enqueueOneMutation, { data: 1 });
    await runToCompletion();
    expect(await t.query(api.example.status, { id })).toEqual({
      kind: "success",
      result: 1,
    });
  });
});
