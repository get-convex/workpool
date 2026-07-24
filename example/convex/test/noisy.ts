/**
 * Work functions for the noisyNeighbor scenarios: tasks that misbehave in
 * various ways, plus onComplete handlers that record per-class outcomes.
 */
import { v } from "convex/values";
import {
  internalAction,
  internalMutation,
  internalQuery,
} from "../_generated/server";
import { vOnCompleteArgs, NonRetryableError } from "@convex-dev/workpool";
import { internal } from "../_generated/api";
import { generateData } from "./work";

export const vTaskClass = v.object({
  label: v.string(),
  count: v.number(),
  fnType: v.union(v.literal("action"), v.literal("query")),
  behavior: v.union(
    v.literal("succeed"),
    v.literal("fail"),
    v.literal("failNonRetryable"),
  ),
  durationMs: v.optional(v.number()), // actions only
  returnBytes: v.optional(v.number()),
  argBytes: v.optional(v.number()), // pad args; ≥8KB forces the payload path
  retries: v.optional(v.number()), // maxAttempts if set
  contendedOnComplete: v.optional(v.boolean()),
  marker: v.optional(v.boolean()), // write a row per completed attempt
});

export const noisyAction = internalAction({
  args: {
    runId: v.id("runs"),
    label: v.string(),
    behavior: v.union(
      v.literal("succeed"),
      v.literal("fail"),
      v.literal("failNonRetryable"),
    ),
    durationMs: v.optional(v.number()),
    returnBytes: v.optional(v.number()),
    // Padding to push fnArgs over the 8KB inline threshold (payload path).
    filler: v.optional(v.string()),
    // Write a marker row just before returning, to prove this attempt ran to
    // completion even if its result is later lost (e.g. batch worker death).
    marker: v.optional(v.boolean()),
  },
  handler: async (ctx, args) => {
    if (args.durationMs) {
      await new Promise((resolve) => setTimeout(resolve, args.durationMs));
    }
    if (args.behavior === "fail") {
      throw new Error(`noisyAction(${args.label}) failing on purpose`);
    }
    if (args.behavior === "failNonRetryable") {
      throw new NonRetryableError(
        `noisyAction(${args.label}) failing terminally on purpose`,
      );
    }
    if (args.marker) {
      await ctx.runMutation(internal.test.noisy.writeMarker, {
        runId: args.runId,
        label: args.label,
      });
    }
    return generateData(args.returnBytes ?? 10);
  },
});

export const writeMarker = internalMutation({
  args: { runId: v.id("runs"), label: v.string() },
  handler: async (ctx, args) => {
    await ctx.db.insert("data", {
      misc: { kind: "noisyMarker", runId: args.runId, label: args.label },
    });
  },
});

/** Count marker rows (attempts that ran to completion) for a run. */
export const countMarkers = internalQuery({
  args: { runId: v.id("runs") },
  handler: async (ctx, args) => {
    const rows = await ctx.db.query("data").collect();
    return rows.filter(
      (r) =>
        typeof r.misc === "object" &&
        r.misc !== null &&
        (r.misc as { kind?: string }).kind === "noisyMarker" &&
        (r.misc as { runId?: string }).runId === args.runId,
    ).length;
  },
});

export const noisyQuery = internalQuery({
  args: {
    runId: v.id("runs"),
    label: v.string(),
    behavior: v.union(
      v.literal("succeed"),
      v.literal("fail"),
      v.literal("failNonRetryable"),
    ),
    returnBytes: v.optional(v.number()),
  },
  handler: async (_ctx, args) => {
    if (args.behavior === "fail") {
      throw new Error(`noisyQuery(${args.label}) failing on purpose`);
    }
    if (args.behavior === "failNonRetryable") {
      throw new NonRetryableError(
        `noisyQuery(${args.label}) failing terminally on purpose`,
      );
    }
    return generateData(args.returnBytes ?? 10);
  },
});

const onCompleteContext = v.object({
  runId: v.id("runs"),
  label: v.string(),
  type: v.union(v.literal("action"), v.literal("query")),
  enqueuedAt: v.number(),
  contended: v.optional(v.boolean()),
});

/**
 * Records the terminal outcome of a task. When `contended` is set, it also
 * increments a single shared counter doc first, so every concurrent
 * onComplete in the run conflicts with every other one (OCC pressure).
 */
export const noisyOnComplete = internalMutation({
  args: vOnCompleteArgs(onCompleteContext),
  handler: async (ctx, args) => {
    if (args.context.contended) {
      const counter = await ctx.db
        .query("counters")
        .withIndex("name", (q) => q.eq("name", "noisy"))
        .unique();
      if (counter) {
        await ctx.db.patch("counters", counter._id, {
          value: counter.value + 1,
        });
      } else {
        await ctx.db.insert("counters", { name: "noisy", value: 1 });
      }
    }
    await ctx.db.insert("tasks", {
      runId: args.context.runId,
      workId: args.workId,
      type: args.context.type,
      endTime: Date.now(),
      enqueuedAt: args.context.enqueuedAt,
      label: args.context.label,
      resultKind: args.result.kind,
    });
  },
});
