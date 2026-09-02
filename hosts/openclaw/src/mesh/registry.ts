/**
 * The retained registry: the capability profile other agents discover, and the
 * config actions that mutate it.
 *
 * The profile is published RETAINED, so it is the one place a deployment
 * broadcasts what it can do — and, via ownerPolicy, what it actually enforces.
 * Note what is NOT resolved here: `${VAR}` references stay as templates on the
 * wire, because resolving them would broadcast deployment values to every
 * subscriber of the registry topic.
 */

import type { Logger, ServicesFile } from "../types.js";
import { PROTOCOL_VERSION } from "../types.js";
import type { Catalog } from "./catalog.js";

export interface RegistryDeps {
  agentId: string;
  profileTopic: string;
  requireOwner: boolean;
  verifyOwner: boolean;
  /**
   * What this deployment actually enforces about who a requester is, read at
   * publish time rather than fixed at construction: whether the broker refuses
   * an over-broad subscription is not known until it has refused one.
   */
  ownerPolicy(): { required: boolean; topic: string; verified: boolean };
  catalog: Catalog;
  logger: Logger;
  connected(): boolean;
  publish(topic: string, payload: string, opts?: { qos?: 0 | 1 | 2; retain?: boolean }): void;
  onPublished(profile: Record<string, unknown>): void;
}

export interface Registry {
  buildProfile(): Record<string, unknown>;
  publishProfile(): void;
  runConfigAction(msg: any): Record<string, unknown>;
}

export function createRegistry(deps: RegistryDeps): Registry {
  const { catalog, logger, publish, onPublished } = deps;

  function buildProfile(): Record<string, unknown> {
    const svc: ServicesFile = catalog.read();
    return {
      agentId: deps.agentId,
      displayName: svc.displayName ?? deps.agentId,
      status: deps.connected() ? "online" : "offline",
      // The code's, never the catalog's. A capability file says what this
      // agent offers; what it SPEAKS is decided by the implementation in
      // this process, and a file that can answer for it is a file that can
      // lie about it — one deployment advertised 1.2 across three releases
      // because the number had been typed there once.
      protocolVersion: PROTOCOL_VERSION,
      capabilities: svc.capabilities,
      commands: ["invoke", "query", "cancel", "config"],
      executionModel: "transport in framework; logic in agent",
      // Advertised so clients read what this deployment enforces rather than
      // inferring it from the version number.
      ownerPolicy: deps.ownerPolicy(),
      updatedAt: new Date().toISOString(),
    };
  }

  function publishProfile(): void {
    if (!deps.connected()) return;
    catalog.markPublished();
    const profile = buildProfile();
    publish(deps.profileTopic, JSON.stringify(profile), { qos: 1, retain: true });
    logger.info(`profile published (${(profile.capabilities as unknown[]).length} capabilities)`);
    onPublished(profile);
  }

  function runConfigAction(msg: any): Record<string, unknown> {
    const action = msg?.action;
    const svc = catalog.read();
    const wrap = (ok: boolean, rest: Record<string, unknown>) => ({ ok, action, ...rest });

    switch (action) {
      case "list":
        return wrap(true, { capabilities: svc.capabilities });

      case "reload":
        publishProfile();
        return wrap(true, { note: "profile republished" });

      case "add_service": {
        const s = msg.service;
        if (!s?.service) return wrap(false, { error: "service.service required" });
        if (svc.capabilities.some((c) => c.service === s.service)) {
          return wrap(false, { error: "exists (use update_service)", service: s.service });
        }
        svc.capabilities.push(s);
        if (!catalog.write(svc)) return wrap(false, { error: "write failed" });
        publishProfile();
        return wrap(true, { added: s.service });
      }

      case "remove_service": {
        const before = svc.capabilities.length;
        svc.capabilities = svc.capabilities.filter((c) => c.service !== msg.service);
        if (svc.capabilities.length === before) return wrap(false, { error: "not found" });
        if (!catalog.write(svc)) return wrap(false, { error: "write failed" });
        publishProfile();
        return wrap(true, { removed: msg.service });
      }

      case "update_service": {
        const idx = svc.capabilities.findIndex((c) => c.service === msg.service);
        if (idx < 0) return wrap(false, { error: "not found" });
        // The name is preserved unless the patch explicitly renames it —
        // renaming orphans callers, so it must be deliberate.
        svc.capabilities[idx] = {
          ...svc.capabilities[idx],
          ...(msg.patch ?? {}),
          service: msg.patch?.service ?? msg.service,
        };
        if (!catalog.write(svc)) return wrap(false, { error: "write failed" });
        publishProfile();
        return wrap(true, { updated: svc.capabilities[idx].service });
      }

      default:
        return wrap(false, {
          error: "unknown action",
          available: ["list", "reload", "add_service", "remove_service", "update_service"],
        });
    }
  }

  return { buildProfile, publishProfile, runConfigAction };
}
