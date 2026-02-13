import { internalMutation, mutation, query } from "./_generated/server";
import { components, internal } from "./_generated/api";
import { v } from "convex/values";
import { standard, batch } from "./setup";
// Import batchActions so that batch.action() registrations and
// batch.setExecutorRef() run before any enqueue() calls.
import "./batchActions";

const CHUNK_SIZE = 2000;

const SENTENCES = [
  "The quick brown fox jumps over the lazy dog",
  "Technology is reshaping how we live and work",
  "The ocean covers more than seventy percent of Earth",
  "Music speaks what cannot be expressed in words",
  "A journey of a thousand miles begins with a single step",
  "Stars are born from clouds of dust and gas",
  "Coffee is the most popular beverage in the world",
  "The human brain contains roughly one hundred billion neurons",
  "Mountains are formed by the movement of tectonic plates",
  "Books are a uniquely portable form of magic",
  "Sunlight takes about eight minutes to reach Earth",
  "Laughter is the shortest distance between two people",
  "Rivers carved the Grand Canyon over millions of years",
  "Democracy depends on the participation of its citizens",
  "The speed of light is approximately three hundred thousand kilometers per second",
  "Gardens teach patience more effectively than any lecture",
  "Whales sing songs that travel thousands of miles underwater",
  "Architecture is frozen music according to Goethe",
  "Honey never spoils and has been found in ancient tombs",
  "The best time to plant a tree was twenty years ago",
];

// Kick off N jobs in standard mode (1 action per task)
export const runStandard = mutation({
  args: { count: v.number() },
  handler: async (ctx, { count }) => {
    const startedAt = Date.now();
    for (let i = 0; i < count; i++) {
      const sentence = SENTENCES[i % SENTENCES.length];
      const jobId = await ctx.db.insert("jobs", {
        sentence,
        mode: "standard",
        status: "pending",
        startedAt,
      });
      await standard.enqueueAction(
        ctx,
        internal.standardActions.translateToSpanish,
        { sentence },
        {
          onComplete: internal.pipeline.standardAfterSpanish,
          context: { jobId },
        },
      );
    }
    return { started: count, mode: "standard" };
  },
});

// Kick off N jobs in batch mode, chunked to stay under 16K write limit
export const runBatch = mutation({
  args: { count: v.number() },
  handler: async (ctx, { count }) => {
    const startedAt = Date.now();
    const chunk = Math.min(count, CHUNK_SIZE);
    const tasks = [];
    for (let i = 0; i < chunk; i++) {
      const sentence = SENTENCES[i % SENTENCES.length];
      const jobId = await ctx.db.insert("jobs", {
        sentence,
        mode: "batch",
        status: "pending",
        startedAt,
      });
      tasks.push({
        name: "translateToSpanish",
        args: { sentence },
        options: {
          onComplete: internal.pipeline.batchAfterSpanish,
          context: { jobId },
        },
      });
    }
    await batch.enqueueBatch(ctx, tasks);
    const remaining = count - chunk;
    if (remaining > 0) {
      await ctx.scheduler.runAfter(0, internal.test._runBatchChunk, {
        remaining,
        startedAt,
      });
    }
    return { started: count, mode: "batch" };
  },
});

export const _runBatchChunk = internalMutation({
  args: { remaining: v.number(), startedAt: v.number() },
  handler: async (ctx, { remaining, startedAt }) => {
    const chunk = Math.min(remaining, CHUNK_SIZE);
    const tasks = [];
    for (let i = 0; i < chunk; i++) {
      const sentence = SENTENCES[i % SENTENCES.length];
      const jobId = await ctx.db.insert("jobs", {
        sentence,
        mode: "batch",
        status: "pending",
        startedAt,
      });
      tasks.push({
        name: "translateToSpanish",
        args: { sentence },
        options: {
          onComplete: internal.pipeline.batchAfterSpanish,
          context: { jobId },
        },
      });
    }
    await batch.enqueueBatch(ctx, tasks);
    const left = remaining - chunk;
    if (left > 0) {
      await ctx.scheduler.runAfter(0, internal.test._runBatchChunk, {
        remaining: left,
        startedAt,
      });
    }
  },
});

// Check progress for a mode — count non-terminal statuses (small) to avoid 32K limit
export const progress = query({
  args: { mode: v.string(), total: v.optional(v.number()) },
  handler: async (ctx, { mode, total: totalArg }) => {
    const total = totalArg ?? 40000;

    // Count small sets: pending, step1, step2, failed
    const statuses = ["pending", "step1", "step2", "failed"] as const;
    const counts: Record<string, number> = {};
    for (const status of statuses) {
      const docs = await ctx.db
        .query("jobs")
        .withIndex("by_mode_status", (q) =>
          q.eq("mode", mode).eq("status", status),
        )
        .take(10000);
      counts[status] = docs.length;
    }

    const failed = counts.failed;
    const inProgress = counts.pending + counts.step1 + counts.step2;
    const completed = total - inProgress - failed;

    // Duration stats from sample of completed jobs
    const sample = await ctx.db
      .query("jobs")
      .withIndex("by_mode_status", (q) =>
        q.eq("mode", mode).eq("status", "completed"),
      )
      .take(500);
    const durations = sample.map((j) => j.completedAt! - j.startedAt);
    const avgDuration = durations.length
      ? durations.reduce((a, b) => a + b, 0) / durations.length
      : 0;
    const maxDuration = durations.length ? Math.max(...durations) : 0;

    return {
      total,
      completed,
      failed,
      inProgress,
      avgDurationMs: Math.round(avgDuration),
      maxDurationMs: maxDuration,
    };
  },
});

// Get completed results with spot-check data
export const results = query({
  args: { mode: v.string(), limit: v.optional(v.number()) },
  handler: async (ctx, { mode, limit }) => {
    const completed = await ctx.db
      .query("jobs")
      .withIndex("by_mode_status", (q) =>
        q.eq("mode", mode).eq("status", "completed"),
      )
      .take(limit ?? 10);

    return completed.map((j) => ({
      sentence: j.sentence,
      spanish: j.spanish,
      backToEnglish: j.backToEnglish,
      letterCount: j.letterCount,
      actualLetterCount: j.backToEnglish
        ? j.backToEnglish.replace(/\s/g, "").length
        : null,
      letterCountCorrect:
        j.letterCount ===
        (j.backToEnglish ? j.backToEnglish.replace(/\s/g, "").length : null),
      durationMs: j.completedAt ? j.completedAt - j.startedAt : null,
    }));
  },
});

// Concurrent fetch histogram — returns {time, concurrent}[] bucketed by 1s
// Paginated: call with cursor to get next page
export const concurrency = query({
  args: {
    mode: v.string(),
    bucketMs: v.optional(v.number()),
    cursor: v.optional(v.string()),
  },
  handler: async (ctx, { mode, bucketMs: bucketMsArg, cursor }) => {
    const bucketMs = bucketMsArg ?? 1000;
    // Paginate through completed jobs (8000 per page to stay under read limits)
    const PAGE_SIZE = 8000;
    const query = ctx.db
      .query("jobs")
      .withIndex("by_mode_status", (q) =>
        q.eq("mode", mode).eq("status", "completed"),
      );
    const jobs = cursor
      ? await query.take(PAGE_SIZE) // simplified: re-scans from start
      : await query.take(PAGE_SIZE);

    // Collect all fetch intervals
    const events: { time: number; delta: number }[] = [];
    for (const job of jobs) {
      if (job.fetch1Start && job.fetch1End) {
        events.push({ time: job.fetch1Start, delta: 1 });
        events.push({ time: job.fetch1End, delta: -1 });
      }
      if (job.fetch2Start && job.fetch2End) {
        events.push({ time: job.fetch2Start, delta: 1 });
        events.push({ time: job.fetch2End, delta: -1 });
      }
    }
    if (events.length === 0) return { buckets: [], hasMore: false };

    // Sort by time
    events.sort((a, b) => a.time - b.time);

    // Bucket into time windows
    const minTime = events[0].time;
    const maxTime = events[events.length - 1].time;
    const buckets: { time: number; concurrent: number }[] = [];
    let eventIdx = 0;
    let concurrent = 0;
    for (let t = minTime; t <= maxTime; t += bucketMs) {
      // Process all events in this bucket
      while (eventIdx < events.length && events[eventIdx].time < t + bucketMs) {
        concurrent += events[eventIdx].delta;
        eventIdx++;
      }
      buckets.push({ time: t - minTime, concurrent });
    }

    return {
      buckets,
      hasMore: jobs.length === PAGE_SIZE,
      jobsProcessed: jobs.length,
    };
  },
});

// Reset all jobs for a clean test (paginated to avoid read limits)
export const reset = mutation({
  handler: async (ctx) => {
    const jobs = await ctx.db.query("jobs").take(1000);
    for (const job of jobs) {
      await ctx.db.delete(job._id);
    }
    // Also reset the batch component's config and tasks
    if (jobs.length === 0) {
      await ctx.runMutation(components.batchPool.batch.resetConfig);
      const result = await ctx.runMutation(
        components.batchPool.batch.resetTasks,
      );
      if (result.more) return { deleted: result.deleted, more: true };
    }
    return { deleted: jobs.length, more: jobs.length === 1000 };
  },
});
