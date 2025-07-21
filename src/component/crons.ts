import { cronJobs } from "convex/server";
import { internal } from "./_generated/api";
import { internalMutation } from "./_generated/server";
import { getCurrentSegment, MINUTE, toSegment } from "./shared";
import { RECOVERY_PERIOD_SEGMENTS } from "./loop";

const crons = cronJobs();

export const recover = internalMutation({
  args: {},
  handler: async (ctx) => {
    const internalState = await ctx.db.query("internalState").first();
    const runStatus = await ctx.db.query("runStatus").first();
    async function checkPending() {
      const anyPending =
        ((await ctx.db.query("pendingCompletion").first()) && "completion") ||
        ((await ctx.db.query("pendingStart").first()) && "start") ||
        ((await ctx.db.query("pendingCancelation").first()) && "cancelation");
      return anyPending;
    }
    let kick = false;
    if (!runStatus || !internalState) {
      if (await checkPending()) {
        kick = true;
      }
    } else {
      switch (runStatus.state.kind) {
        case "idle":
          if (runStatus.state.generation !== internalState.generation) {
            kick = true;
          } else if (await checkPending()) {
            kick = true;
          }
          break;
        case "running":
          if (
            getCurrentSegment() - internalState.lastRecovery >=
            2n * RECOVERY_PERIOD_SEGMENTS
          ) {
            kick = true;
          }
          break;
        case "scheduled":
          if (
            runStatus.state.segment + toSegment(1 * MINUTE) <
            getCurrentSegment()
          ) {
            kick = true;
          } else {
            const pending = await checkPending();
            if (pending === "completion") {
              kick = true;
            }
          }
          break;
      }
    }
    if (kick) {
      await ctx.scheduler.runAfter(0, internal.kick.forceKick, {});
    }
  },
});

crons.interval("recover", { minutes: 30 }, internal.crons.recover);

export default crons;
