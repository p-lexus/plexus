/**
 * The meshes this agent is on, kept in step with what the box says.
 *
 * A plugin does not decide its meshes — it receives them. The box issues the
 * grants, publishes the list, and this starts a mesh instance for every root in
 * it and stops the ones that have gone.
 *
 * The list is authoritative once it arrives, INCLUDING an empty one: an agent
 * taken off its last mesh is on nothing, and falling back to a configured root
 * there would put it somewhere the broker refuses a moment later, silently, at
 * QoS 1. The configured root is used only while no box has ever spoken.
 */
import type { Membership } from "../config.js";
import type { Logger } from "../types.js";
import type { MeshInstance, SharedDeps } from "./instance.js";

export interface Supervisor {
  /** Apply a list of roots, starting and stopping to match. */
  apply(roots: string[]): void;
  /** Every instance currently running, in the order they were started. */
  running(): MeshInstance[];
  stopAll(): void;
}

export function createSupervisor(
  shared: SharedDeps,
  logger: Logger,
  build: (root: string) => Membership,
  make: (m: Membership, s: SharedDeps) => MeshInstance,
): Supervisor {
  const live = new Map<string, MeshInstance>();
  const order: string[] = [];

  const start = (root: string) => {
    try {
      live.set(root, make(build(root), shared));
      order.push(root);
      logger.info(`[mesh] joined ${root} — the box says this agent is a member.`);
    } catch (e: any) {
      // One mesh failing must not take the others down. A box that grants
      // access to something this build cannot join is a thing to report, not
      // a reason to leave every other mesh.
      logger.info(`[mesh] could not join ${root}: ${e.message}`);
    }
  };

  const stop = (root: string) => {
    const instance = live.get(root);
    if (!instance) return;
    try { instance.stop(); } catch { /* leaving anyway */ }
    live.delete(root);
    const at = order.indexOf(root);
    if (at >= 0) order.splice(at, 1);
    logger.info(`[mesh] left ${root} — the box no longer lists this agent as a member.`);
  };

  return {
    apply(roots) {
      const wanted = new Set(roots);
      for (const root of [...live.keys()]) {
        if (!wanted.has(root)) stop(root);
      }
      for (const root of roots) {
        if (!live.has(root)) start(root);
      }
    },
    running() {
      return order.map((r) => live.get(r)!).filter(Boolean);
    },
    stopAll() {
      for (const root of [...order].reverse()) stop(root);
    },
  };
}
