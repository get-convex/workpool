import { defineApp } from "convex/server";
import workpool from "@convex-dev/workpool/convex.config";
import workpoolOld from "@convex-dev/workpool-old/convex.config";
import staticHosting from "@convex-dev/static-hosting/convex.config";

const app = defineApp();
app.use(workpool, { name: "smallPool", env: {} });
app.use(workpool, { name: "bigPool", env: {} });
app.use(workpool, { name: "serializedPool", env: {} });
app.use(workpool, { name: "testWorkpool", env: {} });
app.use(workpoolOld, { name: "oldWorkpool" });
app.use(staticHosting);

export default app;
