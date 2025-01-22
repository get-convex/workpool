import { defineApp } from "convex/server";
import workpool from "@convex-dev/workpool/convex.config";

const app = defineApp();
app.use(workpool);
app.use(workpool, { name: "lowpriWorkpool" });
app.use(workpool, { name: "highPriWorkpool" });

export default app;
