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
import {
  buildTopics, jobTopicPattern, parseJobTopic, ownerScope,
  registryPattern, parseRegistryTopic, registryProfileFilter, registryStatusFilter,
} from "./mesh/topics.js";
import { normalizeJobPublish } from "./mesh/payload.js";
import { createCatalog } from "./mesh/catalog.js";
import { createVarStore } from "./mesh/vars.js";
import { createJobStore } from "./mesh/jobs.js";
import { createTransport } from "./mesh/transport.js";
import { createDispatcher } from "./mesh/dispatch.js";
import { createRegistry } from "./mesh/registry.js";
import { createPeerRegistry } from "./mesh/peers.js";
import { createAskService } from "./mesh/ask.js";
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
    const registryRe = registryPattern(conf.mesh.root);

    const catalog = createCatalog(conf.mesh.servicesFile, logger);
    const vars = createVarStore(conf.mesh.secretsFile, conf.mesh.promptVars, logger);
    const sse = createSseHub();
    const auth = createAuth(conf.web.auth);
    const jobs = createJobStore((rec) => sse.broadcast("job", rec));
    const transport = createTransport(conf, pluginDir, topics.status, logger);
    const peers = createPeerRegistry(conf.mesh.agentId, logger, () => sse.broadcast("peers", peers.list()));

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
      peers: peers.size,
      maxDepth: conf.mesh.maxDepth,
      delegation: conf.mesh.delegation,
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
      peerSummary: () => peers.summary(),
      onCancel: (jobId, requestedBy) => ask.cancelChildren(jobId, requestedBy ?? conf.mesh.agentId),
      // Late-bound: the ask service needs the dispatcher's lineage lookup, so
      // the two are mutually dependent and neither can be built first.
      performAsk: (req) => ask.ask(req),
    });

    const ask = createAskService({
      selfAgentId: conf.mesh.agentId,
      meshRoot: conf.mesh.root,
      maxDepth: conf.mesh.maxDepth,
      timeoutMs: conf.mesh.askTimeoutMs,
      logger,
      publish: transport.publish,
      peer: (id) => peers.get(id),
      lineageOf: (jobId) => dispatcher.lineageOf(jobId),
      onDelegated: (info) => {
        // Recorded locally so a delegated job is visible in our console even
        // though a peer is doing the work.
        jobs.record(
          { jobId: info.jobId, service: info.service, state: "started",
            owner: ownerScope(conf.mesh.agentId), requestedBy: conf.mesh.agentId,
            delegated: true, delegatedTo: info.agent,
            parentJobId: info.parentJobId, rootJobId: info.rootJobId, depth: info.depth },
          { type: "delegated", note: `asked ${info.agent} for ${info.service}` },
        );
      },
    });

    const { server } = startHttpServer({
      cfg: conf, logger, auth, sse, jobs, vars, dispatcher, registry,
      snapshot,
      peers: () => peers.list(),
      profileWithBroker: () => ({
        ...registry.buildProfile(),
        broker: { connected: transport.connected, stats: transport.stats },
      }),
    });

    // ── Inbound message routing ────────────────────────

    function onMessage(topic: string, raw: string, data: any): void {
      logger.info(`received on ${topic}: ${raw.slice(0, 300)}`);

      // Peer registry: who else is on the mesh and what they can do.
      const reg = parseRegistryTopic(registryRe, topic);
      if (reg) {
        if (reg.kind === "profile") peers.onProfile(reg.agentId, data);
        else peers.onStatus(reg.agentId, data);
        return;
      }

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
          // If we asked a peer for this, hand the answer back to the waiting
          // executor. This is the return path that makes delegation possible.
          ask.settle(jobId, data);
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
          { jobId: data.jobId, service: data.service, args: data.args, requestedBy: data.requestedBy,
            parentJobId: data.parentJobId, rootJobId: data.rootJobId, depth: data.depth },
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
      // Retained, so subscribing reveals the whole mesh immediately.
      [registryProfileFilter(conf.mesh.root)]: { qos: 1 },
      [registryStatusFilter(conf.mesh.root)]: { qos: 1 },
    });

    transport.start({
      onConnect() {
        transport.publish(topics.status,
          JSON.stringify({ status: "online", timestamp: new Date().toISOString() }),
          { qos: 1, retain: true });
        registry.publishProfile();
        logger.info(`connected (MQTT ${conf.broker.protocolVersion === 5 ? "5" : "3.1.1"}) — commands, jobs and peer registry subscribed`);
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

    /**
     * Delegation. This is what makes it a mesh rather than a set of agents that
     * happen to share a broker: an executor can hand work to the agent that
     * owns that capability and use the answer in its own reply.
     */
    api.registerTool({
      name: "mesh_ask",
      description:
        "Ask another agent on the mesh to do a job you are not best placed to do, wait for its " +
        "answer, and receive the result. Use mesh_peers first to see who offers what.",
      parameters: Type.Object({
        agent: Type.String({ description: "Agent id of the peer to ask (from mesh_peers)." }),
        service: Type.String({ description: "Capability that peer offers, e.g. schema.review" }),
        args: Type.Optional(Type.Any({ description: "Arguments matching that capability's requestSchema." })),
        parentJobId: Type.Optional(Type.String({
          description: "The job you are currently executing. Pass it so the chain can be traced and cancelled as one request.",
        })),
      }),
      async execute(_id: string, params: { agent: string; service: string; args?: any; parentJobId?: string }) {
        const mode = conf.mesh.delegation;
        if (mode !== "both" && mode !== "dynamic") {
          return {
            content: [{
              type: "text" as const,
              text: mode === "declared"
                ? "Dynamic delegation is disabled here (mesh.delegation is \"declared\"). This agent only " +
                  "delegates what its capabilities declare up front, and those answers are already in your prompt."
                : "Delegation is disabled on this agent (mesh.delegation is \"off\").",
            }],
            isError: true,
          };
        }
        const outcome = await ask.ask({
          agent: params.agent,
          service: params.service,
          args: params.args ?? {},
          parentJobId: params.parentJobId,
        });
        if (!outcome.ok) {
          return {
            content: [{ type: "text" as const, text: `mesh_ask failed: ${outcome.error}` }],
            isError: true,
          };
        }
        return {
          content: [{
            type: "text" as const,
            text: `Answer from ${outcome.agent} (job ${outcome.jobId}):\n` +
                  JSON.stringify(outcome.result, null, 2),
          }],
        };
      },
    });

    api.registerTool({
      name: "mesh_peers",
      description:
        "List the other agents on this mesh and the capabilities each offers. Use this to find " +
        "which agent to ask when a job needs expertise you do not have.",
      parameters: Type.Object({
        service: Type.Optional(Type.String({ description: "Only show agents offering this capability." })),
      }),
      async execute(_id: string, params: { service?: string }) {
        const list = params.service ? peers.providersOf(params.service) : peers.list();
        if (!list.length) {
          return {
            content: [{
              type: "text" as const,
              text: params.service
                ? `No agent on this mesh offers "${params.service}".`
                : "No other agents have published a profile to this mesh.",
            }],
          };
        }
        const text = list.map((p) =>
          `${p.agentId}${p.online ? "" : " (offline)"} — ${p.displayName ?? "no name"}\n` +
          p.capabilities.map((c) => `    ${c.service}${c.description ? `: ${c.description}` : ""}`).join("\n"),
        ).join("\n");
        return { content: [{ type: "text" as const, text }] };
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
