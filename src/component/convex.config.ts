import { defineComponent } from "convex/server";
import batchWorker from "@convex-dev/batch-worker/convex.config.js";
import { v } from "convex/values";
import { logLevel } from "./logging.js";

const component = defineComponent("workpool", {
  env: {
    // Optional override for the log level. If set, it takes precedence over the
    // `logLevel` passed to the workpool functions / configured via the client.
    // Bind it from the app, e.g.
    // `app.use(workpool, { env: { LOG_LEVEL: "DEBUG" } })`.
    LOG_LEVEL: v.optional(logLevel),
  },
});
component.use(batchWorker, { env: { LOG_LEVEL: component.env.LOG_LEVEL } });

export default component;
