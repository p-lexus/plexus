/**
 * What the panel needs from a mesh.
 *
 * Its own file so the dependency runs one way: the panel depends on a mesh, a
 * mesh does not depend on the panel. It lived in http/server.ts, which meant
 * mesh/instance.ts had to reach into the HTTP layer to say what it satisfies —
 * backwards for a module that would still be correct with no panel at all.
 *
 * MeshInstance extends this, so the compiler checks the contract where the
 * object is built rather than where the routes happen to use it.
 */

import type { Said } from "./feedback.js";
import type { ResolvedConfig } from "../config.js";
import type { JobStore } from "./jobs.js";
import type { Dispatcher } from "./dispatch.js";
import type { Registry } from "./registry.js";

export interface MeshView {
  name: string;
  /** This mesh's own configuration, not the panel's. */
  conf: ResolvedConfig;
  jobs: JobStore;
  dispatcher: Dispatcher;
  registry: Registry;
  /** File a verdict on a delegated job. Returns why not, or null. */
  fileVerdict(agent: string, jobId: string, verdict: string, said?: Said): string | null;
  snapshot(): Record<string, unknown>;
  profileWithBroker(): Record<string, unknown>;
  /** The peer registry; the routes call .list(), and snapshot() reads .size. */
  peers: { list(): unknown[]; size: number };
}
