import { defineApp } from "convex/server";
import workpool from "@convex-dev/workpool/convex.config";

const app = defineApp();
app.use(workpool, { name: "smallPool" });
app.use(workpool, { name: "bigPool" });
app.use(workpool, { name: "serializedPool" });

// Pools for the standard vs batch comparison test
app.use(workpool, { name: "standardPool" });
app.use(workpool, { name: "batchPool" });

export default app;
