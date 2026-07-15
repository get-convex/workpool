import { defineComponent } from "convex/server";
import batchWorker from "@convex-dev/batch-worker/convex.config.js";

const component = defineComponent("workpool");
component.use(batchWorker, { env: { LOG_LEVEL: "REPORT" } });

export default component;
