/**
 * Which meshes this agent belongs to, as told by the box.
 *
 * The box issues the grants, so the box is the only thing that knows. A plugin
 * is configured with an ORGANIZATION and a credential — never a mesh list —
 * and learns the rest here, on `<org>/members/<agentId>`: retained, so it is
 * told the moment it connects, and republished on every change, so an agent
 * added to a mesh joins it without a restart.
 *
 * Nothing here is required. On a broker with no box nothing is ever published,
 * `onMeshes` is never called, and the caller falls back to its configured root
 * as its one mesh — which is exactly what the plugin always did. A box makes it
 * multi-mesh; the absence of one changes nothing.
 */
import mqtt from "mqtt";
import type { ResolvedConfig } from "../config.js";
import type { Logger } from "../types.js";

export interface RosterWatch {
  stop(): void;
}

/** The payload, checked rather than trusted: it decides what this agent joins. */
export function readMembership(raw: string, agentId: string): string[] | null {
  let parsed: any;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return null;
  // Addressed to somebody else is not a message about us. The topic already
  // carries the name, and checking the body too costs nothing and closes the
  // gap where a box publishes to the wrong topic.
  if (parsed.agent !== undefined && parsed.agent !== agentId) return null;
  if (!Array.isArray(parsed.meshes)) return null;

  const roots: string[] = [];
  for (const m of parsed.meshes) {
    if (typeof m !== "string") continue;
    const root = m.trim();
    // A root with a wildcard in it would widen every filter built from it —
    // the one thing a mesh root may never contain, and the reason this is
    // checked here rather than trusted from a message.
    if (!root || root.includes("+") || root.includes("#")) continue;
    if (!roots.includes(root)) roots.push(root);
  }
  return roots;
}

export function watchRoster(
  cfg: ResolvedConfig,
  org: string,
  logger: Logger,
  onMeshes: (roots: string[]) => void,
): RosterWatch {
  const agentId = cfg.mesh.agentId;
  const topic = `${org}/members/${agentId}`;
  const client = mqtt.connect(cfg.broker.url, {
    // Its own client id: the mesh connections each have one, and two clients
    // sharing an id take turns disconnecting each other forever.
    clientId: `plexus-${agentId}-roster-${Math.random().toString(16).slice(2, 8)}`,
    username: cfg.broker.username,
    password: cfg.broker.password,
    keepalive: cfg.broker.keepalive,
    // Clean, and no will: this connection holds no mesh state and announces
    // nothing. It reads one retained message and stays for the updates.
    clean: true,
    reconnectPeriod: 5_000,
    connectTimeout: 15_000,
    protocolVersion: cfg.broker.protocolVersion,
  });

  client.on("connect", () => {
    client.subscribe(topic, { qos: 1 }, (err, granted) => {
      if (err) {
        logger.info(`[mesh] could not read ${topic}: ${err.message} — using the configured mesh only.`);
        return;
      }
      // A refused subscription is granted with code 128 and no error. Reading
      // the grant is the only way to tell it apart from silence, and silence
      // here means "no box", which is a supported answer.
      const refused = (granted ?? []).some((g: any) => g.qos >= 128);
      if (refused) {
        logger.info(
          `[mesh] not allowed to read ${topic} — this credential predates the box telling agents ` +
            `their meshes. Using the configured mesh only; \`plexus-server rotate-agent ${agentId}\` regenerates the rules.`,
        );
      }
    });
  });

  client.on("message", (t: string, payload: Buffer) => {
    if (t !== topic) return;
    const roots = readMembership(payload.toString(), agentId);
    if (roots === null) {
      logger.info(`[mesh] ${topic} carried something unreadable — ignoring it.`);
      return;
    }
    onMeshes(roots);
  });

  client.on("error", (e: Error) => {
    logger.info(`[mesh] ${topic}: ${e.message}`);
  });

  return {
    stop() {
      try { client.end(true); } catch { /* going away anyway */ }
    },
  };
}
