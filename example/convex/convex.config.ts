import { defineApp } from "convex/server";
import workpool from "@convex-dev/workpool/convex.config";

const app = defineApp();
app.use(workpool, { name: "smallPool" });
app.use(workpool, { name: "bigPool" });
app.use(workpool, { name: "serializedPool" });
app.use(workpool, { name: "dynamicWorkpool" });

export default app;
