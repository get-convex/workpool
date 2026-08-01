import { execFileSync } from "node:child_process";

const [runId, component = "oldWorkpool"] = process.argv.slice(2);
if (!runId) {
  console.error("Usage: npm run capture:scheduled -- <runId> [componentPath]");
  process.exit(1);
}

function convex(args) {
  return execFileSync("npx", ["convex", ...args], {
    cwd: new URL("..", import.meta.url),
    encoding: "utf8",
    stdio: ["ignore", "pipe", "inherit"],
  }).trim();
}

const bounds = JSON.parse(
  convex(["run", "test/run:captureBounds", JSON.stringify({ runId })]),
);
if (!bounds) {
  throw new Error(`Run ${runId} has no completed tasks`);
}

function countRange(startTime, endTimeExclusive) {
  const inlineQuery = `const rows = await ctx.db.system.query("_scheduled_functions").withIndex("by_creation_time", q => q.gte("_creationTime", ${startTime}).lt("_creationTime", ${endTimeExclusive})).take(1001); return { count: Math.min(rows.length, 1000), overflow: rows.length > 1000 };`;
  const result = JSON.parse(
    convex(["run", "--component", component, "--inline-query", inlineQuery]),
  );
  if (!result.overflow) return result.count;
  if (endTimeExclusive - startTime <= 1) {
    throw new Error("Too many scheduled functions in a one-millisecond window");
  }
  const midpoint = Math.floor((startTime + endTimeExclusive) / 2);
  return (
    countRange(startTime, midpoint) + countRange(midpoint, endTimeExclusive)
  );
}

const count = countRange(bounds.startTime, bounds.endTime + 1);
if (!Number.isInteger(count) || count < 0) {
  throw new Error(`Unexpected scheduled-function count: ${count}`);
}

convex([
  "run",
  "test/run:setScheduledFunctionCount",
  JSON.stringify({ runId, count }),
]);
console.log(`Stored ${count} scheduled functions for ${runId} (${component}).`);
