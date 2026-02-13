import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

export default defineSchema({
  data: defineTable({
    data: v.number(),
  }),

  // Jobs table for the standard vs batch comparison test
  jobs: defineTable({
    sentence: v.string(),
    mode: v.string(), // "standard" | "batch"
    spanish: v.optional(v.string()),
    backToEnglish: v.optional(v.string()),
    letterCount: v.optional(v.number()),
    status: v.string(), // "pending" | "step1" | "step2" | "step3" | "completed" | "failed"
    startedAt: v.number(),
    completedAt: v.optional(v.number()),
    // Per-fetch timing for concurrency graphing
    fetch1Start: v.optional(v.number()),
    fetch1End: v.optional(v.number()),
    fetch2Start: v.optional(v.number()),
    fetch2End: v.optional(v.number()),
  })
    .index("by_mode_status", ["mode", "status"])
    .index("by_mode", ["mode"]),
});
