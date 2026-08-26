import { v } from "convex/values";
import {
  internalAction,
  internalMutation,
  internalQuery,
} from "../_generated/server";
import { components, internal } from "../_generated/api";
import { Workpool } from "@convex-dev/workpool";

/**
 * End-to-end checks for the two paths the throughput scenarios never touch:
 * work scheduled for later, and work that retries. Both put a wall-clock time
 * into the queue's commit-timestamp-ordered field instead of the placeholder,
 * so they're worth exercising against a real deployment and not just
 * convex-test.
 *
 * Run:
 *   npx convex run test/scheduling:default
 *   npx convex run test/scheduling:default '{"delayMs":400000}'  # past the
 *                                          # safe-future threshold
 */
const pool = new Workpool(components.testWorkpool, { maxParallelism: 10 });

const vProbe = v.object({
  label: v.string(),
  at: v.number(),
  attempt: v.optional(v.number()),
});

export const record = internalMutation({
  args: { label: v.string(), attempt: v.optional(v.number()) },
  returns: v.null(),
  handler: async (ctx, { label, attempt }) => {
    await ctx.db.insert("schedulingProbes", { label, at: Date.now(), attempt });
    return null;
  },
});

export const probes = internalQuery({
  args: {},
  returns: v.array(vProbe),
  handler: async (ctx) => {
    const docs = await ctx.db.query("schedulingProbes").collect();
    return docs
      .map(({ label, at, attempt }) => ({ label, at, attempt }))
      .sort((a, b) => a.at - b.at);
  },
});

export const reset = internalMutation({
  args: {},
  returns: v.null(),
  handler: async (ctx) => {
    for (const doc of await ctx.db.query("schedulingProbes").collect()) {
      await ctx.db.delete("schedulingProbes", doc._id);
    }
    return null;
  },
});

/** Fails its first two attempts so the retry backoff path runs. */
export const failTwice = internalAction({
  args: { label: v.string() },
  returns: v.null(),
  handler: async (ctx, { label }) => {
    const seen = (
      await ctx.runQuery(internal.test.scheduling.probes, {})
    ).filter((p) => p.label === label).length;
    await ctx.runMutation(internal.test.scheduling.record, {
      label,
      attempt: seen + 1,
    });
    if (seen < 2) throw new Error(`attempt ${seen + 1} fails on purpose`);
    return null;
  },
});

export const enqueueBoth = internalMutation({
  args: { delayMs: v.number() },
  returns: v.null(),
  handler: async (ctx, { delayMs }) => {
    await pool.enqueueMutation(
      ctx,
      internal.test.scheduling.record,
      { label: "delayed" },
      { runAfter: delayMs },
    );
    await pool.enqueueAction(
      ctx,
      internal.test.scheduling.failTwice,
      { label: "retry" },
      { retry: { maxAttempts: 4, initialBackoffMs: 500, base: 2 } },
    );
    return null;
  },
});

export default internalAction({
  args: { delayMs: v.optional(v.number()) },
  handler: async (ctx, { delayMs = 8_000 }) => {
    await ctx.runMutation(internal.test.scheduling.reset, {});

    const enqueuedAt = Date.now();
    await ctx.runMutation(internal.test.scheduling.enqueueBoth, { delayMs });

    const deadline = Date.now() + delayMs + 60_000;
    // Keep the number of polls bounded — a long delay at a fixed short interval
    // runs past an action's limit on how many functions it may call.
    const pollMs = Math.max(250, Math.round(delayMs / 100));
    let probes: { label: string; at: number; attempt?: number }[] = [];
    let ranEarly = false;
    while (Date.now() < deadline) {
      probes = await ctx.runQuery(internal.test.scheduling.probes, {});
      const delayed = probes.find((p) => p.label === "delayed");
      if (delayed && delayed.at < enqueuedAt + delayMs) ranEarly = true;
      const retriesDone = probes.filter((p) => p.label === "retry").length >= 3;
      if (delayed && retriesDone) break;
      await new Promise((r) => setTimeout(r, pollMs));
    }

    const delayed = probes.find((p) => p.label === "delayed");
    const retries = probes.filter((p) => p.label === "retry");
    const lateByMs = delayed ? delayed.at - (enqueuedAt + delayMs) : undefined;

    console.log("=== scheduling results ===");
    console.log(
      delayed
        ? `delayed (runAfter ${delayMs}ms): ran ${lateByMs}ms after its runAt` +
            (ranEarly ? " — RAN EARLY" : "")
        : `delayed (runAfter ${delayMs}ms): NEVER RAN`,
    );
    console.log(
      `retry: ${retries.length} attempts, gaps ${
        retries
          .slice(1)
          .map((p, i) => p.at - retries[i].at)
          .join("/") || "n/a"
      }ms`,
    );
    return {
      delayedRan: !!delayed,
      delayedRanEarly: ranEarly,
      delayedLateByMs: lateByMs,
      retryAttempts: retries.length,
    };
  },
});
