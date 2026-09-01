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
import type { Verdict } from "./types.js";
import type { PluginConfig } from "./types.js";
import { resolveConfig } from "./config.js";
import { createLogger } from "./logger.js";
import {
  buildTopics, jobTopicPattern, parseJobTopic, ownerScope, jobPostmortemTopic,
  registryPattern, parseRegistryTopic, registryProfileFilter, registryStatusFilter,
  invokeFilter, invokeTopicOwner, feedbackFilter, feedbackTopicOwner,
} from "./mesh/topics.js";
import { normalizeJobPublish, publishRefusal } from "./mesh/payload.js";
import { readFeedback, verdictFor } from "./mesh/feedback.js";
import { createLimiter, promptFor, signatureOf, triggerFor } from "./mesh/postmortem.js";
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
/**
 * The live transport-owning instance.
 *
 * Tools must be registered on EVERY registration — the gateway registers
 * plugins per agent session, so a tool registered only once exists only in
 * whichever session happened to be first. But transport must stay a singleton.
 * The tools therefore resolve through this slot at call time rather than
 * closing over one registration's state.
 */
const ACTIVE_SLOT = Symbol.for("mqtt-bridge.instance");

interface ActiveInstance {
  publishCounted(topic: string, payload: string, opts?: { qos?: 0 | 1 | 2; retain?: boolean }): void;
  normalize(topic: string, payload: string, retain?: boolean): { payload: string; retain: boolean };
  /** Why this publish must not go out, or null if it may. */
  refuse(topic: string): string | null;
  ask(req: { agent: string; service: string; args?: any; parentJobId?: string }): Promise<any>;
  peers(): any[];
  providersOf(service: string): any[];
  /** File a verdict on a delegated job. Returns why it was refused, or null. */
  fileVerdict(agent: string, jobId: string, verdict: string, reason?: string): string | null;
  delegationMode: string;
}
const MODULE_SLOT = Symbol.for("mqtt-bridge.module");
const DISPOSE_SLOT = Symbol.for("mqtt-bridge.dispose");

export default definePluginEntry({
  id: "mqtt-bridge",
  name: "Agent Mesh (MQTT)",
  description: "Agent Mesh Protocol over MQTT, with an HTTP control API and web panel.",

  register(api: any) {
    const cfg = (api.pluginConfig ?? {}) as Partial<PluginConfig>;
    const logger = createLogger(api.logger, "mqtt-bridge");

    // Two separate concerns, and conflating them cost us every tool.
    //
    // TRANSPORT runs only in the gateway: a CLI or discovery load has no
    // runtime.subagent/system, and dispatching there pollutes the mesh with
    // "inject failed" results (incident 2026-08-24, rev-018/019).
    //
    // TOOLS must be registered in EVERY mode. The gateway asks the plugin what
    // tools it has via a dedicated "tool-discovery" registration; returning
    // early there tells it we have none, which is why mqtt_publish, mesh_ask
    // and mesh_peers were absent from every agent session.
    const mode = api.registrationMode;
    const transportAllowed = !mode || mode === "full";

    const globalAny = globalThis as Record<symbol, unknown>;
    const active = () => globalAny[ACTIVE_SLOT] as ActiveInstance | undefined;
    const notReady = (what: string) => ({
      content: [{ type: "text" as const, text: `${what}: the mesh bridge is not connected yet.` }],
      isError: true,
    });

    // ── Tools ──────────────────────────────────────────
    // Registered on EVERY registration, before the singleton guard below, so
    // every agent session has them. They resolve the live instance at call
    // time. Registering after the guard is why these tools were previously
    // absent from every session but the first.

    api.registerTool({
      name: "mqtt_publish",
      description: "Publish a message to any MQTT topic (job events/results, config, status).",
      parameters: Type.Object({
        payload: Type.String({ description: "Payload (JSON string or text)." }),
        topic: Type.String({ description: "Topic to publish to, e.g. agents/jobs/<owner>/<jobId>/result" }),
        retain: Type.Optional(Type.Boolean({ description: "Retain. Default false; forced true on job result topics." })),
      }),
      async execute(_id: string, params: { payload: string; topic: string; retain?: boolean }) {
        const inst = active();
        if (!inst) return notReady("mqtt_publish");
        const refusal = inst.refuse(params.topic);
        if (refusal) {
          logger.info(`mqtt_publish refused: ${refusal}`);
          return { content: [{ type: "text" as const, text: `Refused: ${refusal}.` }], isError: true };
        }
        try {
          const { payload, retain } = inst.normalize(params.topic, params.payload, params.retain);
          inst.publishCounted(params.topic, payload, { qos: 1, retain });
          return { content: [{ type: "text" as const, text: `Published to ${params.topic}${retain ? " (retained)" : ""}` }] };
        } catch (err: any) {
          return { content: [{ type: "text" as const, text: `Failed: ${err.message}` }], isError: true };
        }
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
        const inst = active();
        if (!inst) return notReady("mesh_peers");
        const list = params.service ? inst.providersOf(params.service) : inst.peers();
        if (!list.length) {
          return { content: [{ type: "text" as const, text: params.service
            ? `No agent on this mesh offers "${params.service}".`
            : "No other agents have published a profile to this mesh." }] };
        }
        const text = list.map((p: any) =>
          `${p.agentId}${p.online ? "" : " (offline)"} — ${p.displayName ?? "no name"}\n` +
          p.capabilities.map((c: any) => `    ${c.service}${c.description ? `: ${c.description}` : ""}`).join("\n"),
        ).join("\n");
        return { content: [{ type: "text" as const, text }] };
      },
    });

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
        const inst = active();
        if (!inst) return notReady("mesh_ask");
        const mode = inst.delegationMode;
        if (mode !== "both" && mode !== "dynamic") {
          return {
            content: [{ type: "text" as const, text: mode === "declared"
              ? "Dynamic delegation is disabled here (mesh.delegation is \"declared\"). This agent only delegates what its capabilities declare up front, and those answers are already in your prompt."
              : "Delegation is disabled on this agent (mesh.delegation is \"off\")." }],
            isError: true,
          };
        }
        const outcome = await inst.ask({
          agent: params.agent, service: params.service,
          args: params.args ?? {}, parentJobId: params.parentJobId,
        });
        if (!outcome.ok) {
          return { content: [{ type: "text" as const, text: `mesh_ask failed: ${outcome.error}` }], isError: true };
        }
        return { content: [{ type: "text" as const, text:
          `Answer from ${outcome.agent} (job ${outcome.jobId}):\n${JSON.stringify(outcome.result, null, 2)}\n\n` +
          `When you have used this, call mesh_feedback with jobId ${outcome.jobId} to say what it was worth. ` +
          `A capability nobody judges repeats its mistakes.` }] };
      },
    });

    api.registerTool({
      name: "mesh_feedback",
      description:
        "Say what a peer's answer was worth, after you have used it. Call this for work you " +
        "delegated with mesh_ask: \"good\" if the answer did its job, \"bad\" if it was poor, " +
        "\"unusable\" if it did not answer the question at all. Give a specific reason — it is " +
        "read back before that capability runs again, so \"wrong table\" is worth more than " +
        "\"not great\". A delegation that failed outright is already reported without you.",
      parameters: Type.Object({
        jobId: Type.String({ description: "The job id mesh_ask returned." }),
        verdict: Type.String({ description: "good | bad | unusable" }),
        agent: Type.Optional(Type.String({ description: "The peer you asked. Checked against the job." })),
        reason: Type.Optional(Type.String({ description: "Why, specifically. One or two sentences." })),
      }),
      async execute(_id: string, params: { jobId: string; verdict: string; agent?: string; reason?: string }) {
        const inst = active();
        if (!inst) return notReady("mesh_feedback");
        const refused = inst.fileVerdict(params.agent ?? "", params.jobId, params.verdict, params.reason);
        if (refused) {
          return { content: [{ type: "text" as const, text: `mesh_feedback refused: ${refused}` }], isError: true };
        }
        return { content: [{ type: "text" as const, text:
          `Filed "${params.verdict}" on job ${params.jobId}. It reaches the peer if this mesh has a ` +
          `recorder to relay it; on a mesh without one nothing collects it.` }] };
      },
    });

    if (!transportAllowed) {
      logger.info(`registrationMode=${mode} — tools registered, transport inactive (gateway-only).`);
      return;
    }

    if (!cfg.broker?.url) {
      logger.warn("no broker.url configured — plugin inactive.");
      return;
    }

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
    // How much job traffic the broker lets us see. "mesh" is the whole root:
    // the panel's history, and — because a broker echoes a publish back to a
    // subscriber, even the one that sent it — our own executors' results.
    // A generated agent ACL refuses that filter, so `scoped` is the fallback:
    // our own owner scope only, with our executors' publishes observed locally
    // instead of heard back. See onSubscribeDenied.
    let jobFeed: "mesh" | "scoped" = "mesh";
    const refusedFilters: string[] = [];
    const registryRe = registryPattern(conf.mesh.root);

    const catalog = createCatalog(
      conf.mesh.servicesFile, logger, path.join(pluginDir, "services.example.json"),
    );
    const vars = createVarStore(conf.mesh.secretsFile, conf.mesh.promptVars, logger);
    const sse = createSseHub();
    const auth = createAuth(conf.web.auth);
    const jobs = createJobStore((rec) => sse.broadcast("job", rec), {
      file: conf.mesh.historyFile,
      log: (m) => logger.info(m),
    });
    const transport = createTransport(conf, pluginDir, topics.status, logger);
    const peers = createPeerRegistry(conf.mesh.agentId, logger, () => sse.broadcast("peers", peers.list()));

    const snapshot = () => ({
      connected: transport.connected,
      uptimeMs: transport.stats.connectedAt ? Date.now() - transport.stats.connectedAt : 0,
      rx: transport.stats.rx,
      tx: transport.stats.tx,
      reconnects: transport.stats.reconnects,
      reconnectsLastHour: transport.recentReconnects(),
      lastError: transport.stats.lastError,
      lastErrorAt: transport.stats.lastErrorAt || undefined,
      // Whether the link is healthy NOW, rather than whether anything went
      // wrong in the last hour. The panel warns on this, so a recovered link
      // stops warning instead of carrying its worst hour around.
      settled: transport.settled(),
      activeJobs: [...jobs.active],
      agentId: conf.mesh.agentId,
      selfScope: ownerScope(conf.mesh.agentId),
      meshRoot: conf.mesh.root,
      protocolVersion: PROTOCOL_VERSION,
      session: { ...transport.session },
      ownerPolicy: ownerPolicy(),
      // What the broker allows, as opposed to what was asked for. A mesh whose
      // ACLs have narrowed us should say so somewhere an operator looks.
      jobFeed,
      refusedFilters: [...refusedFilters],
      // Names and SOURCES only. The panel flags unbound ${VAR} references;
      // values are deployment secrets and never reach a browser.
      promptVars: vars.describe().map(({ name, source }) => ({ name, source })),
      secretsAuth: auth.configured,
      peers: peers.size,
      maxDepth: conf.mesh.maxDepth,
      delegation: conf.mesh.delegation,
    });

    /**
     * What this deployment enforces about who a requester is — reported, not
     * decided here. The agent serves both invoke forms and refuses neither;
     * whether anyone is stopped is the broker's business.
     *
     * `verified` therefore does not come from anything this agent does. It
     * comes from whoever configured the broker's rules and stated so, which is
     * what `plexus-server add-agent --owner-in-topic` writes into the config it
     * generates. Inferring it here — from a refused subscription, say — would
     * mean advertising a guarantee nobody actually made: a broker can scope job
     * topics without scoping invokes, and the difference is exactly the one
     * this field exists to report.
     */
    const ownerPolicy = () => ({
      required: conf.mesh.requireOwner,
      topic: conf.mesh.ownerInTopic,
      verified: conf.mesh.verifyOwner || conf.mesh.ownerEnforced,
    });

    const registry = createRegistry({
      agentId: conf.mesh.agentId,
      profileTopic: topics.profile,
      requireOwner: conf.mesh.requireOwner,
      verifyOwner: conf.mesh.verifyOwner,
      ownerPolicy,
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

    const explained = createLimiter();

    /**
     * Ask the executor to explain a job that went wrong.
     *
     * Runs outside the watchdog entirely: no job is created and no watch is
     * registered, so nothing here can be re-dispatched or nudged. If the
     * executor never publishes, nobody is left waiting — which is why the
     * bridge does not chase this the way it chases a job.
     */
    function explain(jobId: string): void {
      const job = jobs.find(jobId);
      const trigger = triggerFor(job);
      if (!job || !trigger) return;
      if (!explained.take(signatureOf(job, trigger), Date.now())) {
        logger.info(`[postmortem] ${job.service ?? "unknown"} has already explained this failure recently`);
        return;
      }

      const owner = job.owner ?? ownerScope(job.requestedBy);
      const topic = jobPostmortemTopic(conf.mesh.root, owner, jobId);
      const sub = (api.runtime as any)?.subagent;
      if (typeof sub?.run !== "function") return;

      jobs.record({ jobId }, { type: "postmortem_requested" });
      void sub.run({ sessionKey: `${conf.sessionKey}:postmortem`, message: promptFor(job, trigger, topic) })
        .catch((e: any) => logger.info(`[postmortem] could not start for ${jobId}: ${e?.message ?? e}`));
    }

    /** File a verdict on a job this agent delegated. Returns why not, or null. */
    function fileVerdict(agent: string, jobId: string, verdict: Verdict, reason?: string): string | null {
      const rec = jobs.find(jobId);
      const me = ownerScope(conf.mesh.agentId);

      if (!rec?.delegated) return `job ${jobId} is not one this agent delegated`;
      if (agent && rec.delegatedTo && rec.delegatedTo !== agent) {
        return `job ${jobId} was delegated to ${rec.delegatedTo}, not ${agent}`;
      }
      if (rec.feedback?.some((f) => f.by === me)) {
        return `a verdict on job ${jobId} has already been filed`;
      }

      const out = verdictFor(conf.mesh.root, rec.delegatedTo ?? agent, me, jobId, verdict, reason);
      if (!out) return `"${verdict}" is not a verdict — expected good, bad or unusable`;

      transport.publish(out.topic, JSON.stringify(out.payload), { qos: 1 });
      // Recorded here because the relay returns on a topic this agent cannot read.
      jobs.recordFeedback(jobId, { verdict, ...(reason ? { reason } : {}), by: me, ts: Date.now() });
      return null;
    }

    const ask = createAskService({
      selfAgentId: conf.mesh.agentId,
      meshRoot: conf.mesh.root,
      maxDepth: conf.mesh.maxDepth,
      timeoutMs: conf.mesh.askTimeoutMs,
      logger,
      publish: transport.publish,
      peer: (id) => peers.get(id),
      // Read from the peer's retained profile, so what we publish follows what
      // it says it serves rather than what this deployment happens to prefer.
      peerOwnerTopicMode: (id) => (peers.get(id) as any)?.ownerPolicy?.topic,
      lineageOf: (jobId) => dispatcher.lineageOf(jobId),
      fileVerdict: (agent, jobId, verdict, reason) => {
        const refused = fileVerdict(agent, jobId, verdict, reason);
        if (refused) logger.info(`[feedback] not filed for ${jobId}: ${refused}`);
      },
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
      fileVerdict: (agent, jobId, verdict, reason) =>
        fileVerdict(agent, jobId, verdict as Verdict, reason),
      snapshot,
      peers: () => peers.list(),
      profileWithBroker: () => ({
        ...registry.buildProfile(),
        broker: { connected: transport.connected, stats: transport.stats },
      }),
    });

    // ── Inbound message routing ────────────────────────

    /**
     * Job traffic: milestones and results, including our executors' own.
     *
     * Called for every message that arrives, and — when the broker refuses the
     * mesh-wide filter — for our own publishes too, so that a job's bookkeeping
     * never depends on hearing ourselves come back. Returns whether the topic
     * was job traffic.
     */
    /** When a message says it happened, or now if it does not say. */
    function stamped(data: any): number | undefined {
      const t = Date.parse(String(data?.ts ?? ""));
      return Number.isNaN(t) ? undefined : t;
    }

    function recordJobTraffic(topic: string, raw: string, data: any): boolean {
      const parsed = parseJobTopic(jobTopicRe, topic);
      if (!parsed) return false;
      const { owner, jobId, kind } = parsed;

      // A cancelled job is terminal — suppress late executor publishes so the
      // client's view matches the cancel_acknowledged contract.
      if (jobs.cancelled.has(jobId)) return true;

      if (kind === "postmortem") {
        jobs.record({ jobId, postmortem: { summary: data?.summary, lesson: data?.lesson, ts: stamped(data) ?? Date.now() } },
          { type: "postmortem", note: data?.lesson ?? data?.summary, at: stamped(data) });
        return true;
      }

      if (kind === "events") {
        const type = String(data?.type ?? "message");
        const note = data?.note ?? data?.stage ?? data?.error ?? (data ? undefined : raw.slice(0, 120));
        jobs.record(
          { jobId, lastEvent: type, requestedBy: data?.owner, owner },
          // When it happened, from the payload — not when it arrived. The
          // bridge hears its own publishes, so this is the second copy of an
          // event it already recorded, and only the timestamp tells the store
          // they are the same one.
          { type, note: note ? String(note).slice(0, 240) : undefined, at: stamped(data) },
        );
        // Any publish proves it is alive; a publish that CLAIMS the job is
        // finished starts a clock, because an executor that announces the end
        // and publishes no result is the failure the watchdog cannot see.
        dispatcher.markAgentActivity(jobId, { type, note: note ? String(note) : undefined });
      } else {
        jobs.record(
          { jobId, result: data, state: data?.type === "error" ? "error" : "done", requestedBy: data?.owner, owner },
          // A result is RETAINED, so the broker replays it on every
          // resubscribe — once per gateway restart, forever. Carrying its own
          // timestamp is what stops one finished job collecting seven
          // identical endings spread across days it did not run on.
          { type: String(data?.type ?? "result"),
            note: data?.error ? String(data.error).slice(0, 240) : undefined,
            at: stamped(data) },
        );
        jobs.active.delete(jobId);
        dispatcher.forget(jobId);              // terminal — stop watching
        explain(jobId);
        // If we asked a peer for this, hand the answer back to the waiting
        // executor. This is the return path that makes delegation possible.
        ask.settle(jobId, data);
      }
      return true;
    }

    /**
     * Our own publish, observed locally.
     *
     * Only used when the broker refuses the mesh-wide job filter. Topics inside
     * our own scope are skipped: those we are still subscribed to, so the
     * broker delivers them back and recording here as well would double every
     * entry in the timeline.
     */
    function observeOwnPublish(topic: string, payload: string): void {
      if (jobFeed !== "scoped") return;
      if (topic.startsWith(`${conf.mesh.root}/jobs/${ownerScope(conf.mesh.agentId)}/`)) return;
      let data: any = null;
      try { data = JSON.parse(payload); } catch { /* plaintext is allowed */ }
      recordJobTraffic(topic, payload, data);
    }

    function onMessage(topic: string, raw: string, data: any): void {
      logger.info(`received on ${topic}: ${raw.slice(0, 300)}`);

      // Peer registry: who else is on the mesh and what they can do.
      const reg = parseRegistryTopic(registryRe, topic);
      if (reg) {
        if (reg.kind === "profile") peers.onProfile(reg.agentId, data);
        else peers.onStatus(reg.agentId, data);
        return;
      }

      if (recordJobTraffic(topic, raw, data)) return;

      // v1.4: an invoke whose topic carries the owner. The segment is passed on
      // exactly as it arrived — the dispatcher decides whether it is acceptable,
      // because that decision is the protocol's, not the router's.
      const topicOwner = invokeTopicOwner(conf.mesh.root, conf.mesh.agentId, topic);
      if (topicOwner !== null) {
        dispatcher.dispatch(
          { jobId: data?.jobId, service: data?.service, args: data?.args, requestedBy: data?.requestedBy,
            parentJobId: data?.parentJobId, rootJobId: data?.rootJobId, depth: data?.depth },
          { topicOwner },
        );
        return;
      }

      // v1.5: what the work was worth, from whoever asked for it. Same shape as
      // the invoke topic, so the owner is the one the broker matched and the
      // router hands it on untouched — the decision is the protocol's.
      const judge = feedbackTopicOwner(conf.mesh.root, conf.mesh.agentId, topic);
      if (judge !== null) {
        const jobId = String(data?.jobId ?? "").trim();
        const decision = readFeedback(judge, data, jobId ? jobs.find(jobId) : undefined, Date.now());

        if (!decision.feedback) {
          logger.info(`[feedback] refused from ${judge}: ${decision.reason}`);
          // Told to the sender, in the scope it published from — a verdict
          // that silently vanishes is worse than none, because the requester
          // believes the mesh knows something it does not.
          if (jobId) {
            dispatcher.publishEvent(
              jobId, { type: "feedback_refused", note: decision.reason }, ownerScope(judge));
          }
          return;
        }

        const { verdict, reason } = decision.feedback;
        jobs.recordFeedback(jobId, decision.feedback);
        // On the job's own timeline as well as in the record, so it reaches
        // anyone watching the mesh — the box included — without a new
        // subscription anywhere.
        dispatcher.publishEvent(
          jobId,
          { type: "feedback", verdict, ...(reason ? { note: reason } : {}) },
          ownerScope(judge),
        );
        logger.info(`[feedback] ${judge} judged job ${jobId} ${verdict}${reason ? `: ${reason}` : ""}`);
        explain(jobId);
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
              ownerPolicy: ownerPolicy(),
      // What the broker allows, as opposed to what was asked for. A mesh whose
      // ACLs have narrowed us should say so somewhere an operator looks.
      jobFeed,
      refusedFilters: [...refusedFilters],
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
      // v1.4. Separate from commands/<id>/# so that a broker refusing it is
      // reported as itself rather than taking every command topic with it.
      ...(conf.mesh.ownerInTopic === "off"
        ? {}
        : { [invokeFilter(conf.mesh.root, conf.mesh.agentId)]: { qos: 1 as const } }),
      // v1.5. Written by the mesh's recorder and by nothing else — a broker
      // that grants publish here to anyone but the recorder has given away the
      // guarantee. Separate from the other command topics for the same reason
      // the invoke filter is: a refusal here should be reported as itself.
      [feedbackFilter(conf.mesh.root, conf.mesh.agentId)]: { qos: 1 },
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

      onSubscribeDenied(filters) {
        for (const f of filters) if (!refusedFilters.includes(f)) refusedFilters.push(f);
        // logger.info, deliberately: the gateway keeps info from plugins and
        // drops warn and error, so a warning here would be a warning nobody
        // can read.
        logger.info(
          `[acl] broker refused ${filters.length} subscription(s): ${filters.join(", ")} — ` +
          `this is expected on a broker with per-agent ACLs, and is not a connection fault`,
        );

        const firehose = `${conf.mesh.root}/jobs/#`;
        if (filters.includes(firehose) && jobFeed === "mesh") {
          // An agent ACL grants jobs/<agentId>/# and nothing wider. Take it:
          // it carries the answers to what we delegated, which is the one part
          // of the firehose the mesh cannot work without.
          jobFeed = "scoped";
          transport.subscribe({ [`${conf.mesh.root}/jobs/${ownerScope(conf.mesh.agentId)}/#`]: { qos: 1 } });
          logger.info(
            `[acl] job history is now local: subscribed ${conf.mesh.root}/jobs/` +
            `${ownerScope(conf.mesh.agentId)}/# instead. Jobs this agent serves are recorded as it ` +
            `publishes them, so the panel keeps its own history; other owners' traffic is no longer visible`,
          );
        }
        sse.broadcast("status", snapshot());
      },
    });

    // Publish this registration as the live instance so the tools — registered
    // in every session — operate on the one transport that actually exists.
    globalAny[ACTIVE_SLOT] = {
      publishCounted: (topic: string, payload: string, opts?: { qos?: 0 | 1 | 2; retain?: boolean }) => {
        transport.publishCounted(topic, payload, opts);
        observeOwnPublish(topic, payload);
      },
      normalize: (topic: string, payload: string, retain?: boolean) =>
        normalizeJobPublish(jobTopicRe, topic, payload, retain),
      refuse: (topic: string) => {
        const parsed = parseJobTopic(jobTopicRe, topic);
        if (!parsed) return null;
        return publishRefusal(parsed.kind, {
          cancelled: jobs.cancelled.has(parsed.jobId),
          finished: Boolean(jobs.find(parsed.jobId)?.finishedAt),
        }, parsed.jobId);
      },
      ask: (req: any) => ask.ask(req),
      peers: () => peers.list(),
      providersOf: (service: string) => peers.providersOf(service),
      fileVerdict: (agent: string, jobId: string, verdict: string, reason?: string) =>
        fileVerdict(agent, jobId, verdict as Verdict, reason),
      delegationMode: conf.mesh.delegation,
    };

    const stopWatchdog = dispatcher.startWatchdog();
    const stopCatalogWatch = catalog.watch(() => registry.publishProfile());

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
      delete globalAny[ACTIVE_SLOT];
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
