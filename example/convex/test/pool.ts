import { v } from "convex/values";
import { components } from "../_generated/api";
import { Workpool, enqueue, enqueueBatch } from "@convex-dev/workpool";
import type { ComponentApi } from "@convex-dev/workpool/_generated/component.js";
import {
  Workpool as WorkpoolV046,
  enqueue as enqueueV046,
  enqueueBatch as enqueueBatchV046,
} from "@convex-dev/workpool-v046";
import {
  Workpool as WorkpoolV042,
  enqueue as enqueueV042,
  enqueueBatch as enqueueBatchV042,
} from "@convex-dev/workpool-v042";
import { ActionCtx, MutationCtx } from "../_generated/server";

export const POOL_KINDS = ["0.4.7", "0.4.6", "0.4.2"] as const;
export type PoolKind = (typeof POOL_KINDS)[number];
export const vPoolKind = v.union(
  v.literal("0.4.7"),
  v.literal("0.4.6"),
  v.literal("0.4.2"),
);

type Ctx = ActionCtx | MutationCtx;

export function getComponent(kind: PoolKind) {
  switch (kind) {
    case "0.4.7":
      return components.v047Pool;
    case "0.4.6":
      return components.v046Pool;
    case "0.4.2":
      return components.v042Pool;
  }
}

export async function configurePool(
  ctx: Ctx,
  kind: PoolKind,
  maxParallelism: number,
): Promise<void> {
  await ctx.runMutation(getComponent(kind).config.update, { maxParallelism });
}

/**
 * Returns a Workpool instance for the chosen pool. All three classes share
 * the same `enqueueMutation` / `enqueueAction` / `enqueueBatch` shape, so
 * the union return type works for callers that just need to enqueue.
 */
export function makePool(
  kind: PoolKind,
  opts: { maxParallelism: number },
): Workpool | WorkpoolV046 | WorkpoolV042 {
  switch (kind) {
    case "0.4.7":
      return new Workpool(components.v047Pool, opts);
    case "0.4.6":
      return new WorkpoolV046(components.v046Pool, opts);
    case "0.4.2":
      return new WorkpoolV042(components.v042Pool, opts);
  }
}

export function enqueueFor(kind: PoolKind): {
  component: ComponentApi;
  enqueueOne: typeof enqueue;
  enqueueMany: typeof enqueueBatch;
} {
  switch (kind) {
    case "0.4.7":
      return {
        component: components.v047Pool,
        enqueueOne: enqueue,
        enqueueMany: enqueueBatch,
      };
    case "0.4.6":
      return {
        component: components.v046Pool,
        enqueueOne: enqueueV046,
        enqueueMany: enqueueBatchV046,
      };
    case "0.4.2":
      return {
        component: components.v042Pool,
        enqueueOne: enqueueV042,
        enqueueMany: enqueueBatchV042,
      };
  }
}
