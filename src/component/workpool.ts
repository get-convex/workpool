import { v } from "convex/values";
import { api, internal } from "./_generated/api.js";
import type { Doc, Id } from "./_generated/dataModel.js";
import {
  action,
  internalAction,
  internalMutation,
  internalQuery,
  mutation,
  query,
} from "./_generated/server.js";
import { vLogLevel } from "../client/index.js";
import { createLogger } from "./logging.js";
// start

const ENQUEUE_TIME = 5000;
const segment = v.int64();
type Segment = bigint;

export const loop = internalAction({
  args: {
    generation: v.int64(),
    logLevel: vLogLevel,
  },
  handler: async (ctx, args) => {
    const console = createLogger(args.logLevel);

    const state = await ctx.runMutation(internal.workpool.init, {
      generation: args.generation,
    });
    if (!state.ok) {
      console.debug(`[loop] bailing out: ${state.error}`);
      return;
    }
    let cursors = {
      incoming: 0n,
      completion: 0n,
      cancelation: 0n,
    };
    const start = Date.now();
    const { generation } = state;
    // for each work item, add the promise to ongoing, then when it completes, remove it from ongoing
    // if it hit a generation mismatch, close the channel...
    // check ongoing size to know how many to start
    // TODO: use channel with buffer limit on global limit
    const ongoing = new Set<Promise<void>>();
    // We try to drain after 15 seconds... but what about long-lived actions?
    while (Date.now() < start + ENQUEUE_TIME) {
      // fetch more than we might be able to run
      const work = await ctx.runQuery(internal.workpool.getWork, {
        generation,
        cursors,
      });
      if (!work.ok) {
        break;
      }
      cursors = work.cursors;
      if (
        work.completions.length +
          work.cancelations.length +
          work.ready.length ===
        0
      ) {
        if (ongoing.size === 0) {
          if (work.nextJob < Date.now() + 500) {
            await new Promise((resolve) =>
              setTimeout(resolve, work.nextJob - Date.now() - 100)
            );
          }
          break;
        }
        await Promise.race([
          Promise.all(ongoing.values()),
          new Promise((resolve) => setTimeout(resolve, 500)),
        ]);
        continue;
      }
      for (const completion of work.completions) {
        // complete the work
      }
      for (const cancelation of work.cancelations) {
        // cancel the work (in batches)
      }
      for (const ready of work.ready) {
        /*
        // start the work
        // if it's a query:
          // run query
          // if getWork doesn't return args/handle, run wrapper query
          // run onComplete with the result
          // In the onComplete, delete the pendingStart entry & work entry.
          // So the function never is stored as "running".
          // It reads generation number at start to bail early if we're out
          // of date (presumably next generation will pick this up)
        // if it's a mutation:
          // run wrapper mutation which saves the result (with return value)
            // this wrapper deletes from pendingStart. if it's already gone,
            // return that it was a no-op.
            // optimization: call onComplete in the same transaction, and if
            // it fails, mark that function name as racey and in the future
            // save result in pendingCompletion table & return it to the loop
            // (no durability of it running o.w.)
            // it reads generation number at start to bail early if we're out
            // of date (presumably next generation will pick this up)
            // when it returns, the call is considered complete
              // it return the onComplete to call directly
              // call the onComplete which marks it as complete, deletes work
        // for all actions in a batch:
          // run mutation to mark them as started & delete from pendingStart
          // call ~~schedule~~ wrapper action to run
            // optimization: them all in parallel in one wrapper action
            // run action in try/catch
            // call onComplete wrapper which also deletes from work table,
            // (never writes to pendingCompletion)
          // when the call returns, it's considered complete
        */
      }
      // wait until we have capacity for more work - channel?
    }
    console.debug(`[loop] enqueued for ${Date.now() - start}ms`);
    await ctx.runMutation(internal.workpool.cleanup, {
      generation,
    });
    // close the channel - accept no new work
    // mark the workpool as ready for another loop
    // await all mutations & queries - those aren't tracked as ongoing
    await Promise.all(ongoing);
    // mark the workpool as ready for another loop
    // clean up watchdog? or allow re-use.. maybe use for 5 mins and have scheduled for 15 min out?
    // schedule for the next time it should run, and record that scheduled fn id
    console.debug(`[loop] ran loop for ${Date.now() - start}ms`);
  },
});

export const init = internalMutation({
  args: {
    generation: v.int64(),
  },
  handler: async (ctx, { generation }) => {
    const state = await ctx.db.query("internalState").first();
    if (generation !== state?.generation) {
      return {
        ok: false as const,
        error: "generation mismatch",
      };
    }
    state.generation += 1n;
    await ctx.db.replace(state._id, state);
    // If the generation doesn't match, exit early
    // Ensure there's a recovery cron scheduled. and if it's soon, cancel it and schedule another one in the future
    return {
      ok: true as const,
      ...state,
    };
  },
});

export const cleanup = internalMutation({
  args: {
    generation: v.int64(),
  },
  handler: async (ctx) => {
    // finish up:
    // mark the workpool as ready for another loop
    // clean up watchdog? or allow re-use.. maybe use for 5 mins and have scheduled for 15 min out?
    // schedule for the next time it should run, and record that scheduled fn id
  },
});

// TODO: make a db.query that tracks document size limits
export const getWork = internalQuery({
  args: {
    generation: v.int64(),
    cursors: v.object({
      incoming: segment,
      completion: segment,
      cancelation: segment,
    }),
  },
  handler: async (
    ctx,
    args
  ): Promise<
    | {
        ok: true;
        completions: Doc<"pendingCompletion">[];
        cancelations: Doc<"pendingCancelation">[];
        ready: Doc<"pendingStart">[];
        recover: Doc<"work">[];
        cursors: {
          incoming: Segment;
          completion: Segment;
          cancelation: Segment;
        };
        nextJob: number;
      }
    | {
        ok: false;
        error: "generation mismatch";
      }
  > => {
    return { ok: false, error: "generation mismatch" };
    // TODO: make a db.query that tracks document size limits

    // fetch completions
    //   if it's a saturated queue, mark it so we can try to fetch from it.
    // look up old actions that have now timed out
    // fetch cancelations
    // fetch starts
    // Future: look for future work for batched jobs
    // account for work that's now available b/c of completed saturated work
    // skip saturated work (per-queue)
    //   mark saturated work in separate table
    // if we're at capacity, check status of running scheduled functions?
  },
});

const console = "THIS IS A REMINDER TO USE createLogger";
