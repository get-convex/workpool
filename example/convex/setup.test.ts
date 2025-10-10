/// <reference types="vite/client" />
import { test } from "vitest";
import { convexTest } from "convex-test";
import schema from "./schema.js";
import workpool from "@convex-dev/workpool/test";

const modules = import.meta.glob("./**/*.*s");
// When users want to write tests that use your component, they need to
// explicitly register it with its schema and modules.
export function initConvexTest() {
  const t = convexTest(schema, modules);
  t.registerComponent("workpool", workpool.schema, workpool.modules);
  return t;
}

test("setup", () => {});
