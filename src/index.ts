/**
 * Agent Mesh — OpenClaw plugin entry point.
 *
 * Transport for the Agent Mesh Protocol: jobs in over MQTT, executed by an
 * isolated subagent, results back on owner-scoped topics. The bridge is
 * capability-agnostic — it hardcodes no service name anywhere. Capabilities are
 * data in services.json, and job semantics live entirely in prompt templates.
 *
 * Delivery is push end to end. Nothing on a delivery path polls:
 *
 *   broker  → plugin     persistent MQTT session (clean:false, stable clientId), QoS 1
 *   plugin  → executor   subagent.run() at arrival; heartbeat only on older runtimes
 *   plugin  → listeners  QoS 1, results retained
 *   plugin  → panel      Server-Sent Events
 *
 * This file wires the pieces together and owns the lifecycle. Behaviour lives
 * in src/mesh/* and src/http/*.
 */

import { Type } from "typebox";
import * as path from "path";
import { createHash } from "crypto";
import type { Server } from "http";

// @ts-expect-error - openclaw types resolve at runtime from the host
import { definePluginEntry } from "openclaw/plugin-sdk/plugin-entry";

import { PROTOCOL_VERSION } from "./types.js";
import type { PluginConfig } from "./types.js";
import { resolveConfig } from "./config.js";
import { createLogger } from "./logger.js";
import { buildTopics, jobTopicPattern, parseJobTopic, ownerScope } from "./mesh/topics.js";
import { normalizeJobPublish } from "./mesh/payload.js";
import { createCatalog } from "./mesh/catalog.js";
import { createVarStore } from "./mesh/vars.js";
import { createJobStore } from "./mesh/jobs.js";
import { createTransport } from "./mesh/transport.js";
import { createDispatcher } from "./mesh/dispatch.js";
import { createRegistry } from "./mesh/registry.js";
import { createAuth } from "./http/auth.js";
import { createSseHub } from "./http/sse.js";
import { startHttpServer } from "./http/server.js";

// __dirname is unavailable in ESM plugin contexts — resolve from import.meta.
declare const __filename: string | undefined;
const pluginDir: string =
  typeof __filename === "string"
    ? path.dirname(__filename)
    : (() => {
        try { return path.dirname(new URL(import.meta.url).pathname); }
        catch { return process.cwd(); }
      })();

/**
 * Identifies this *module evaluation*.
 *
 * register() runs more than once per process: the gateway registers plugins
 * again for each new agent session, so dispatching a job re-registers this
 * plugin. Those calls share one loaded module and must NOT disturb the
 * transport. A rebuild re-imports the module and produces a new id — the only
 * case where taking over the connection is correct.
 */
const MODULE_INSTANCE = createHash("sha1")
  .update(`${process.pid}:${Date.now()}:${Math.random()}`)
  .digest("hex")
  .slice(0, 12);

const GUARD = Symbol.for("mqtt-bridge.active");
const MODULE_SLOT = Symbol.for("mqtt-bridge.module");
const DISPOSE_SLOT = Symbol.for("mqtt-bridge.dispose");

export default definePluginEntry({
  id: "mqtt-bridge",
  name: "Agent Mesh (MQTT)",
  description: "Agent Mesh Protocol over MQTT, with an HTTP control API and web panel.",

  register(api: any) {
    const cfg = (api.pluginConfig ?? {}) as Partial<PluginConfig>;
    const logger = createLogger(api.logger, "mqtt-bridge");

    // Transport runs ONLY in the gateway. CLI and discovery loads must not
    // connect to the broker or process invokes — a CLI-mode api has no
    // runtime.subagent/system, and dispatching there pollutes the mesh with
    // "inject failed" results (incident 2026-08-24, rev-018/019).
    const mode = api.registrationMode;
    if (mode && mode !== "full") {
      logger.info(`registrationMode=${mode} — transport inactive (gateway-only plugin).`);
      return;
    }

    if (!cfg.broker?.url) {
      logger.warn("no broker.url configured — plugin inactive.");
      return;
    }

    const globalAny = globalThis as Record<symbol, unknown>;

    // ── Reload takeover ────────────────────────────────
    // Another session registering the same loaded module must leave the
    // transport alone; only a rebuilt module should take over. Getting this
    // wrong reconnects the broker on every dispatched job.
    if (globalAny[GUARD]) {
      if (globalAny[MODULE_SLOT] === MODULE_INSTANCE) {
        logger.info("additional registration for the active module — transport untouched");
        return;
      }
      logger.info("new build detected — disposing previous instance and taking over");
      try { (globalAny[DISPOSE_SLOT] as (() => void) | undefined)?.(); }
      catch (e: any) { logger.error(`previous dispose failed: ${e.message}`); }
    }
    globalAny[MODULE_SLOT] = MODULE_INSTANCE;
    globalAny[GUARD] = true;

    // ── Wiring ─────────────────────────────────────────

    const conf = resolveConfig(cfg, pluginDir);
    const topics = buildTopics(conf.mesh.root, conf.mesh.agentId);
    const jobTopicRe = jobTopicPattern(conf.mesh.root);

    const catalog = createCatalog(conf.mesh.servicesFile, logger);
    const vars = createVarStore(conf.mesh.secretsFile, conf.mesh.promptVars, logger);
    const sse = createSseHub();
    const auth = createAuth(conf.web.auth);
    const jobs = createJobStore((rec) => sse.broadcast("job", rec));
    const transport = createTransport(conf, pluginDir, topics.status, logger);

    const snapshot = () => ({
      connected: transport.connected,
      uptimeMs: transport.stats.connectedAt ? Date.now() - transport.stats.connectedAt : 0,
      rx: transport.stats.rx,
      tx: transport.stats.tx,
      reconnects: transport.stats.reconnects,
      lastError: transport.stats.lastError,
      activeJobs: [...jobs.active],
      agentId: conf.mesh.agentId,
      meshRoot: conf.mesh.root,
      protocolVersion: PROTOCOL_VERSION,
      session: { ...transport.session },
      ownerPolicy: { required: conf.mesh.requireOwner, verified: conf.mesh.verifyOwner },
      // Names and SOURCES only. The panel flags unbound ${VAR} references;
      // values are deployment secrets and never reach a browser.
      promptVars: vars.describe().map(({ name, source }) => ({ name, source })),
      secretsAuth: auth.configured,
    });

    const registry = createRegistry({
      agentId: conf.mesh.agentId,
      profileTopic: topics.profile,
      requireOwner: conf.mesh.requireOwner,
      verifyOwner: conf.mesh.verifyOwner,
      catalog,
      logger,
      connected: () => transport.connected,
      publish: transport.publish,
      onPublished: (profile) => sse.broadcast("profile", profile),
    });

    const dispatcher = createDispatcher({
      cfg: conf, logger, catalog, jobs, vars,
      runtime: api.runtime,
      publish: transport.publish,
    });

    const { server } = startHttpServer({
      cfg: conf, logger, auth, sse, jobs, vars, dispatcher, registry,
      snapshot,
      profileWithBroker: () => ({
        ...registry.buildProfile(),
        broker: { connected: transport.connected, stats: transport.stats },
      }),
    });

    // ── Inbound message routing ────────────────────────

    function onMessage(topic: string, raw: string, data: any): void {
      logger.info(`received on ${topic}: ${raw.slice(0, 300)}`);

      // Job traffic: milestones and results, including our executors' own.
      const parsed = parseJobTopic(jobTopicRe, topic);
      if (parsed) {
        const { owner, jobId, kind } = parsed;
        // A cancelled job is terminal — suppress late executor publishes so the
        // client's view matches the cancel_acknowledged contract.
        if (jobs.cancelled.has(jobId)) return;

        if (kind === "events") {
          const type = String(data?.type ?? "message");
          const note = data?.note ?? data?.stage ?? data?.error ?? (data ? undefined : raw.slice(0, 120));
          jobs.record(
            { jobId, lastEvent: type, requestedBy: data?.owner, owner },
            { type, note: note ? String(note).slice(0, 240) : undefined },
          );
          dispatcher.markAgentActivity(jobId);   // any publish proves it is alive
        } else {
          jobs.record(
            { jobId, result: data, state: data?.type === "error" ? "error" : "done", requestedBy: data?.owner, owner },
            { type: String(data?.type ?? "result"), note: data?.error ? String(data.error).slice(0, 240) : undefined },
          );
          jobs.active.delete(jobId);
          dispatcher.forget(jobId);              // terminal — stop watching
        }
        return;
      }

      if (topic === topics.config) {
        transport.publish(`${topics.config}/reply`, JSON.stringify(registry.runConfigAction(data)), { qos: 1 });
        return;
      }

      if (topic === topics.query) {
        const svc = catalog.read();
        const out = data?.jobId
          ? {
              jobId: data.jobId,
              state: jobs.active.has(data.jobId) ? "active"
                : jobs.cancelled.has(data.jobId) ? "cancelled"
                  : "unknown-or-finished",
            }
          : {
              agentId: conf.mesh.agentId,
              protocolVersion: svc.protocolVersion ?? PROTOCOL_VERSION,
              ownerPolicy: { required: conf.mesh.requireOwner, verified: conf.mesh.verifyOwner },
              services: svc.capabilities.map((c) => ({
                service: c.service, description: c.description, requestSchema: c.requestSchema,
              })),
            };
        transport.publish(`${topics.query}/reply`, JSON.stringify(out), { qos: 1 });
        return;
      }

      if (topic === topics.cancel) {
        const jobId = String(data?.jobId ?? "");
        if (jobId && dispatcher.cancel(jobId, data?.requestedBy)) {
          api.runtime.system.enqueueSystemEvent(`🛑 Agent-mesh cancel for job ${jobId}.`, { sessionKey: conf.sessionKey });
        } else {
          dispatcher.publishEvent(jobId || "unknown", { type: "cancel_ignored" }, ownerScope(data?.requestedBy));
        }
        return;
      }

      if (topic === topics.invoke && data) {
        dispatcher.dispatch(
          { jobId: data.jobId, service: data.service, args: data.args, requestedBy: data.requestedBy },
          // Populated by an EMQX rule-engine enrichment when verifyOwner is on.
          { clientUsername: data.client_username ?? data.clientUsername },
        );
      }
    }

    // ── Start ──────────────────────────────────────────

    transport.subscribe({
      [topics.invoke]: { qos: 1 },
      [topics.query]: { qos: 1 },
      [topics.cancel]: { qos: 1 },
      [topics.config]: { qos: 1 },
      [`${conf.mesh.root}/jobs/#`]: { qos: 1 },   // history for the panel
    });

    transport.start({
      onConnect() {
        transport.publish(topics.status,
          JSON.stringify({ status: "online", timestamp: new Date().toISOString() }),
          { qos: 1, retain: true });
        registry.publishProfile();
        logger.info(`connected (MQTT ${conf.broker.protocolVersion === 5 ? "5" : "3.1.1"}) — commands + jobs-history subscribed`);
        sse.broadcast("status", snapshot());
      },
      onMessage,
      onStateChange: () => sse.broadcast("status", snapshot()),
    });

    const stopWatchdog = dispatcher.startWatchdog();
    const stopCatalogWatch = catalog.watch(() => registry.publishProfile());

    // ── Agent tool ─────────────────────────────────────

    api.registerTool({
      name: "mqtt_publish",
      description: "Publish a message to any MQTT topic (job events/results, config, status).",
      parameters: Type.Object({
        payload: Type.String({ description: "Payload (JSON string or text)." }),
        topic: Type.String({ description: "Topic to publish to, e.g. agents/jobs/<owner>/<jobId>/result" }),
        retain: Type.Optional(Type.Boolean({ description: "Retain. Default false; forced true on job result topics." })),
      }),
      async execute(_id: string, params: { payload: string; topic: string; retain?: boolean }) {
        try {
          const { payload, retain } = normalizeJobPublish(jobTopicRe, params.topic, params.payload, params.retain);
          transport.publishCounted(params.topic, payload, { qos: 1, retain });
          return { content: [{ type: "text" as const, text: `Published to ${params.topic}${retain ? " (retained)" : ""}` }] };
        } catch (err: any) {
          return { content: [{ type: "text" as const, text: `Failed: ${err.message}` }], isError: true };
        }
      },
    });

    // ── Shutdown ───────────────────────────────────────

    const shutdown = () => {
      stopWatchdog();
      stopCatalogWatch();
      sse.closeAll();
      try { (server as Server | null)?.close(); } catch { /* noop */ }
      transport.publish(topics.status,
        JSON.stringify({ status: "offline", reason: "shutdown", timestamp: new Date().toISOString() }),
        { qos: 1, retain: true });
      transport.end();
      delete globalAny[GUARD];
      delete globalAny[MODULE_SLOT];
      delete globalAny[DISPOSE_SLOT];
    };

    // Published so the NEXT registration can tear this instance down. Without
    // it a hot reload leaves this client connected while the new module opens
    // its own — two holders of one session, which is what a kick-loop is.
    globalAny[DISPOSE_SLOT] = shutdown;
    process.on("beforeExit", shutdown);
    process.on("SIGTERM", () => { shutdown(); process.exit(0); });
  },
});
