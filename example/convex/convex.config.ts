import { defineApp } from "convex/server";
import workpool from "@convex-dev/workpool/convex.config";

const app = defineApp();
app.use(workpool);
app.use(workpool, { name: "lowpriWorkpool" });

export default app;
