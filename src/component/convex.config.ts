import { defineComponent } from "convex/server";
import crons from "@convex-dev/crons/convex.config";

const component = defineComponent("workpool");

component.use(crons);

export default component;
