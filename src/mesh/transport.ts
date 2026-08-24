/**
 * The MQTT transport: one persistent, durable session.
 *
 * Durability rests entirely on the client id being STABLE across restarts.
 * It is derived from hostname + install path, never from the process id —
 * with a changing id every restart is a new MQTT session, so `clean: false`
 * buys nothing: the broker's queued QoS-1 messages stay orphaned with the dead
 * session, and any invoke published while the gateway was down is lost
 * silently. It also leaked one abandoned session per restart, since MQTT 3.1.1
 * has no session expiry.
 *
 * A stable id has one cost: two instances sharing it fight over the session
 * and kick each other. That is detected and resolved rather than left to look
 * like flaky networking.
 */

import mqtt from "mqtt";
import * as os from "os";
import { createHash } from "crypto";
import type { Logger } from "../types.js";
import type { ResolvedConfig } from "../config.js";

export interface SessionInfo {
  clientId: string;
  mqttVersion: number;
  keepalive: number;
  /**
   * False once a collision has forced a disambiguated session — which silently
   * changes whether jobs published during downtime survive, so it is surfaced.
   */
  durable: boolean;
}

export interface TransportStats {
  rx: number;
  tx: number;
  reconnects: number;
  connectedAt: number;
  lastError: string;
}

export interface TransportHandlers {
  onConnect(): void;
  onMessage(topic: string, raw: string, data: any): void;
  onStateChange(): void;
}

export interface Transport {
  readonly stats: TransportStats;
  readonly session: SessionInfo;
  readonly connected: boolean;
  start(handlers: TransportHandlers): void;
  subscribe(topics: Record<string, { qos: 0 | 1 | 2 }>): void;
  publish(topic: string, payload: string, opts?: { qos?: 0 | 1 | 2; retain?: boolean }): void;
  /** Counts toward the published total; used by the agent tool. */
  publishCounted(topic: string, payload: string, opts?: { qos?: 0 | 1 | 2; retain?: boolean }): void;
  end(force?: boolean): void;
  client(): mqtt.MqttClient | null;
}

/**
 * Stable per-deployment identity. Distinct installs on one host, and the same
 * install on different hosts, all get different ids; restarts do not.
 */
export function deriveClientId(pluginDir: string, configured?: string): string {
  if (configured) return configured;
  const suffix = createHash("sha1").update(`${os.hostname()}::${pluginDir}`).digest("hex").slice(0, 10);
  return `openclaw-mqtt-bridge-${suffix}`;
}

export function createTransport(
  cfg: ResolvedConfig,
  pluginDir: string,
  statusTopic: string,
  logger: Logger,
): Transport {
  const baseClientId = deriveClientId(pluginDir, cfg.broker.clientId);
  let idSuffix = "";                       // empty unless a collision forces one
  let client: mqtt.MqttClient | null = null;
  let handlers: TransportHandlers | null = null;
  let subscriptions: Record<string, { qos: 0 | 1 | 2 }> = {};

  const stats: TransportStats = { rx: 0, tx: 0, reconnects: 0, connectedAt: 0, lastError: "" };
  const session: SessionInfo = {
    clientId: baseClientId,
    mqttVersion: cfg.broker.protocolVersion,
    keepalive: cfg.broker.keepalive,
    durable: true,
  };

  // Kick-loop detector: repeated connects in a short window mean another client
  // holds this session and the broker is bouncing us back and forth.
  const recentConnects: number[] = [];

  function connect(): void {
    client = mqtt.connect(cfg.broker.url, {
      username: cfg.broker.username,
      password: cfg.broker.password,
      clientId: `${baseClientId}${idSuffix}`,
      keepalive: cfg.broker.keepalive,
      clean: false,
      reconnectPeriod: 5_000,
      connectTimeout: 15_000,
      protocolVersion: cfg.broker.protocolVersion,
      // MQTT 5 only: bounds how long the broker holds our queued messages, so a
      // decommissioned deployment stops accumulating them forever.
      ...(cfg.broker.protocolVersion === 5
        ? { properties: { sessionExpiryInterval: cfg.broker.sessionExpirySeconds } }
        : {}),
      will: {
        topic: statusTopic,
        payload: Buffer.from(JSON.stringify({
          status: "offline", reason: "unexpected-disconnect", timestamp: new Date().toISOString(),
        })),
        qos: 1,
        retain: true,
      },
    });
    attach();
  }

  function attach(): void {
    if (!client || !handlers) return;
    const h = handlers;

    client.on("connect", () => {
      stats.connectedAt = Date.now();
      const now = Date.now();
      recentConnects.push(now);
      while (recentConnects.length && now - recentConnects[0] > 60_000) recentConnects.shift();

      if (recentConnects.length >= 5 && !idSuffix) {
        // Durability and stability are in direct conflict here, and stability
        // wins: an agent reconnecting every few seconds processes nothing,
        // whereas a non-durable session merely loses jobs published while it
        // was offline.
        idSuffix = `-${createHash("sha1").update(`${process.pid}:${recentConnects[0]}`).digest("hex").slice(0, 4)}`;
        recentConnects.length = 0;
        session.durable = false;
        session.clientId = `${baseClientId}${idSuffix}`;
        logger.alert(
          `clientId collision on "${baseClientId}" — 5+ connects in 60s means another client holds ` +
          `this session and the broker is kicking us back and forth. Switching to "${session.clientId}" ` +
          `to break the loop. DURABILITY IS NOW DEGRADED: this is a fresh session, so invokes published ` +
          `while this agent is offline will NOT be queued. Set a distinct broker.clientId per instance ` +
          `and restart to restore it.`,
        );
        try { client?.end(true); } catch { /* noop */ }
        setTimeout(() => connect(), 1_000);
        return;
      }

      if (Object.keys(subscriptions).length) {
        client!.subscribe(subscriptions, (err) => err && logger.error(`subscribe failed: ${err.message}`));
      }
      h.onConnect();
    });

    client.on("message", (topic: string, payload: Buffer) => {
      stats.rx++;
      const raw = payload.toString();
      let data: any = null;
      try { data = JSON.parse(raw); } catch { /* plaintext is allowed */ }
      h.onMessage(topic, raw, data);
    });

    client.on("reconnect", () => { stats.reconnects++; logger.warn("reconnecting…"); h.onStateChange(); });
    client.on("error", (err) => { stats.lastError = err.message; logger.error(`MQTT error: ${err.message}`); h.onStateChange(); });
    client.on("offline", () => { logger.warn("broker offline"); h.onStateChange(); });
    client.on("close", () => { logger.warn("connection closed"); h.onStateChange(); });
  }

  return {
    stats,
    session,
    get connected() { return Boolean(client?.connected); },
    client: () => client,

    start(h) {
      handlers = h;
      logger.info(`clientId ${baseClientId} (persistent session, clean:false)`);
      connect();
    },

    subscribe(topics) {
      subscriptions = { ...subscriptions, ...topics };
      if (client?.connected) {
        client.subscribe(topics, (err) => err && logger.error(`subscribe failed: ${err.message}`));
      }
    },

    publish(topic, payload, opts) {
      client?.publish(topic, payload, { qos: opts?.qos ?? 1, retain: opts?.retain ?? false });
    },

    publishCounted(topic, payload, opts) {
      client?.publish(topic, payload, { qos: opts?.qos ?? 1, retain: opts?.retain ?? false });
      stats.tx++;
    },

    end(force) {
      try { client?.end(force); } catch { /* noop */ }
      client = null;
    },
  };
}
