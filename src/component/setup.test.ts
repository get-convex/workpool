/// <reference types="vite/client" />
import { test } from "vitest";
import { convexTest } from "convex-test";
import batchWorker from "@convex-dev/batch-worker/test";
import schema from "./schema.js";

export const modules = import.meta.glob("./**/*.*s");

/**
 * Create a convex-test instance for the workpool component with the
 * batch-worker sub-component registered (the loop runs on batch-worker, so
 * enqueue/cancel/complete call into `components.batchWorker.lib.ping`).
 */
export function setupTest() {
  const t = convexTest(schema, modules);
  batchWorker.register(t);
  return t;
}

test("setup", () => {});
