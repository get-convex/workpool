/// <reference types="vite/client" />
import type { TestConvex } from "convex-test";
import type { GenericSchema, SchemaDefinition } from "convex/server";
import batchWorker from "@convex-dev/batch-worker/test";
import schema from "./component/schema.js";
const modules = import.meta.glob("./component/**/*.ts");

/**
 * Register the component with the test convex instance.
 *
 * Workpool runs its main loop on @convex-dev/batch-worker, so this also
 * registers that nested component under `${name}/batchWorker`.
 *
 * @param t - The test convex instance, e.g. from calling `convexTest`.
 * @param name - The name of the component, as registered in convex.config.ts.
 */
export function register<
  Schema extends SchemaDefinition<GenericSchema, boolean>,
>(t: TestConvex<Schema>, name: string = "workpool") {
  t.registerComponent(name, schema, modules);
  batchWorker.register(
    t as unknown as TestConvex<SchemaDefinition<GenericSchema, boolean>>,
    `${name}/batchWorker`,
  );
}
export default { register, schema, modules };
