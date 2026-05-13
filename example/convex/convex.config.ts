import { defineApp } from "convex/server";
import workpool from "@convex-dev/workpool/convex.config";
import workpoolV046 from "@convex-dev/workpool-v046/convex.config";
import workpoolV042 from "@convex-dev/workpool-v042/convex.config";
import staticHosting from "@convex-dev/static-hosting/convex.config";

const app = defineApp();
app.use(workpool, { name: "smallPool" });
app.use(workpool, { name: "bigPool" });
app.use(workpool, { name: "serializedPool" });
// The three benchmark pools — same deployment, three workpool versions:
app.use(workpool, { name: "v047Pool" }); // this branch (0.4.7-α)
app.use(workpoolV046, { name: "v046Pool" });
app.use(workpoolV042, { name: "v042Pool" });
app.use(staticHosting);

export default app;
