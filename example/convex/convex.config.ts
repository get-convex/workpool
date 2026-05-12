import { defineApp } from "convex/server";
import workpool from "@convex-dev/workpool/convex.config";
import workpoolOld from "@convex-dev/workpool-old/convex.config";

const app = defineApp();
app.use(workpool, { name: "smallPool" });
app.use(workpool, { name: "bigPool" });
app.use(workpool, { name: "serializedPool" });
app.use(workpool, { name: "testWorkpool" });
app.use(workpoolOld, { name: "oldWorkpool" });

export default app;
