/**
 * The peer registry: who else is on this mesh, and what they can do.
 *
 * Every agent already publishes a RETAINED capability profile. Until now
 * nobody subscribed to anyone else's, so each agent broadcast a catalog into
 * the void and knew nothing about its neighbours — which makes delegation
 * impossible, since an agent cannot route work to a peer it does not know
 * exists.
 *
 * Because profiles and status are retained, a newly connected agent learns the
 * whole mesh immediately on subscribe rather than waiting for peers to speak.
 */

import type { Logger, Peer } from "../types.js";

export interface PeerRegistry {
  /** All known peers, newest activity first. Excludes ourselves. */
  list(): Peer[];
  get(agentId: string): Peer | undefined;
  /** Peers advertising a given service, online first. */
  providersOf(service: string): Peer[];
  /** Compact directory for an executor prompt: who does what. */
  summary(): string;
  onProfile(agentId: string, profile: any): void;
  onStatus(agentId: string, status: any): void;
  readonly size: number;
}

export function createPeerRegistry(selfAgentId: string, logger: Logger, onChange: () => void): PeerRegistry {
  const peers = new Map<string, Peer>();

  const touch = (agentId: string): Peer => {
    let p = peers.get(agentId);
    if (!p) {
      p = { agentId, online: false, capabilities: [], lastSeen: 0 };
      peers.set(agentId, p);
    }
    p.lastSeen = Date.now();
    return p;
  };

  return {
    get size() { return peers.size; },
    list: () => [...peers.values()].sort((a, b) => b.lastSeen - a.lastSeen),
    get: (agentId) => peers.get(agentId),

    providersOf(service) {
      return [...peers.values()]
        .filter((p) => p.capabilities.some((c) => c.service === service))
        .sort((a, b) => Number(b.online) - Number(a.online));
    },

    onProfile(agentId, profile) {
      // Our own retained profile comes back to us on the wildcard; ignore it.
      if (agentId === selfAgentId) return;

      // An empty retained payload is how MQTT deletes a retained message, so an
      // agent clearing its profile has left the mesh. Drop it rather than
      // keeping a husk with no capabilities that can never be asked for
      // anything.
      if (profile === null || profile === undefined) {
        if (peers.delete(agentId)) {
          logger.info(`peer ${agentId} left the mesh`);
          onChange();
        }
        return;
      }

      const p = touch(agentId);
      const known = p.capabilities.length;
      p.displayName = profile?.displayName ?? p.displayName;
      p.protocolVersion = profile?.protocolVersion ?? p.protocolVersion;
      p.ownerPolicy = profile?.ownerPolicy ?? p.ownerPolicy;
      p.capabilities = Array.isArray(profile?.capabilities)
        ? profile.capabilities.map((c: any) => ({
            service: String(c?.service ?? ""),
            description: c?.description,
            requestSchema: c?.requestSchema,
            avgLatency: c?.avgLatency,
          })).filter((c: any) => c.service)
        : [];
      // A profile is published on connect, so its arrival is also a liveness
      // signal — the status message may have preceded our subscription.
      if (profile?.status === "online") p.online = true;
      if (!known && p.capabilities.length) {
        logger.info(`peer ${agentId} discovered — ${p.capabilities.length} capabilit${p.capabilities.length === 1 ? "y" : "ies"}`);
      }
      onChange();
    },

    onStatus(agentId, status) {
      if (agentId === selfAgentId) return;
      const p = touch(agentId);
      const wasOnline = p.online;
      p.online = status?.status === "online";
      if (wasOnline !== p.online) {
        logger.info(`peer ${agentId} is now ${p.online ? "online" : "offline"}`);
      }
      onChange();
    },

    summary() {
      const online = [...peers.values()].filter((p) => p.online && p.capabilities.length);
      if (!online.length) return "No other agents are currently online.";
      return online
        .map((p) => `- ${p.agentId}: ${p.capabilities.map((c) => c.service).join(", ")}`)
        .join("\n");
    },
  };
}
