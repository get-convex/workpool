import { v } from "convex/values";
import { components } from "../_generated/api";
import { Workpool, enqueue, enqueueBatch } from "@convex-dev/workpool";
import type { ComponentApi } from "@convex-dev/workpool/_generated/component.js";
import {
  Workpool as OldWorkpool,
  enqueue as enqueueOld,
  enqueueBatch as enqueueBatchOld,
} from "@convex-dev/workpool-old";
import { ActionCtx, MutationCtx } from "../_generated/server";

export const POOL_KINDS = ["new", "old"] as const;
export type PoolKind = (typeof POOL_KINDS)[number];
export const vPoolKind = v.union(v.literal("new"), v.literal("old"));

type Ctx = ActionCtx | MutationCtx;

export async function configurePool(
  ctx: Ctx,
  kind: PoolKind,
  maxParallelism: number,
): Promise<void> {
  const component =
    kind === "new" ? components.testWorkpool : components.oldWorkpool;
  await ctx.runMutation(component.config.update, { maxParallelism });
}

/**
 * Returns a Workpool instance for the chosen pool. Both classes share the
 * same `enqueueMutation` / `enqueueAction` / `enqueueBatch` shape, so the
 * union return type works for callers that just need to enqueue.
 */
export function makePool(
  kind: PoolKind,
  opts: { maxParallelism: number },
): Workpool | OldWorkpool {
  if (kind === "new") return new Workpool(components.testWorkpool, opts);
  return new OldWorkpool(components.oldWorkpool, opts);
}

export function getComponent(kind: PoolKind) {
  return kind === "new" ? components.testWorkpool : components.oldWorkpool;
}

export function enqueueFor(kind: PoolKind): {
  component: ComponentApi;
  enqueueOne: typeof enqueue;
  enqueueMany: typeof enqueueBatch;
} {
  if (kind === "new") {
    return {
      component: components.testWorkpool,
      enqueueOne: enqueue,
      enqueueMany: enqueueBatch,
    };
  }
  return {
    component: components.oldWorkpool,
    enqueueOne: enqueueOld,
    enqueueMany: enqueueBatchOld,
  };
}
