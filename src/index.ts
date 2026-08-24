/**
 * MQTT Bridge Plugin for OpenClaw — Agent Mesh v1.2
 *
 * Framework (transport only; agent logic lives in services.json prompt templates):
 *   registry/<agentId>/profile   (retained) — capability catalog
 *   registry/<agentId>/status    (retained + LWT)
 *   commands/<agentId>/{invoke,query,cancel,config}
 *   jobs/<owner>/<jobId>/{events,result(retained)}   (owner = requestedBy, sanitized)
 *
 * The bridge is capability-agnostic: it hardcodes no service name anywhere.
 * Capabilities are pure data in services.json, addable and removable at runtime
 * via the config command. Job semantics live entirely in the prompt templates.
 *
 * Delivery model — push end to end, no polling on any delivery path:
 *   broker → plugin    persistent MQTT session (clean:false + STABLE clientId), QoS 1
 *   plugin → executor  subagent.run() at arrival; heartbeat only on older runtimes
 *   plugin → listeners QoS 1, results retained
 *   plugin → web panel Server-Sent Events (the panel no longer polls)
 *
 * Web control panel (front end FOR this plugin — no MQTT in the browser):
 *   GET  /mqtt-bridge/ui/api/profile  — catalog + connection state
 *   GET  /mqtt-bridge/ui/api/status   — broker stats (uptime, rx, reconnects)
 *   GET  /mqtt-bridge/ui/api/jobs     — active jobs + recent history
 *   GET  /mqtt-bridge/ui/api/events   — SSE stream of job/status changes
 *   POST /mqtt-bridge/ui/api/invoke   — {service, args, requestedBy, jobId?}
 *   POST /mqtt-bridge/ui/api/config   — {action, ...} (same as MQTT config)
 *   GET  /mqtt-bridge/ui/...          — static SPA pages
 */

import mqtt from "mqtt";
import { Type } from "typebox";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { createHash } from "crypto";
import type { IncomingMessage, ServerResponse } from "http";
import { createServer } from "http";
// @ts-expect-error - openclaw types resolve at runtime from the host
import { definePluginEntry } from "openclaw/plugin-sdk/plugin-entry";

// __dirname is unavailable in ESM plugin contexts — resolve from import.meta.
declare const __filename: string | undefined;
const pluginDir: string =
  typeof __filename === "string"
    ? path.dirname(__filename)
    : (() => {
        try {
          return path.dirname(new URL(import.meta.url).pathname);
        } catch {
          return process.cwd();
        }
      })();

const PROTOCOL_VERSION = "1.2";

/**
 * Identifies this *module evaluation*.
 *
 * register() runs more than once per process: the gateway registers plugins
 * again for each new agent session, so dispatching a job re-registers this
 * plugin. Those calls share one loaded module and must NOT disturb the
 * transport. A rebuild re-imports the module and produces a new id — that is
 * the only case where taking over the connection is correct.
 */
const MODULE_INSTANCE = createHash("sha1")
  .update(`${process.pid}:${Date.now()}:${Math.random()}`)
  .digest("hex")
  .slice(0, 12);

// ── Types ──────────────────────────────────────────────

interface BrokerConfig {
  url: string;
  username?: string;
  password?: string;
  clientId?: string;
  keepalive?: number;
  /** 4 = MQTT 3.1.1 (default, widest compatibility). 5 = MQTT 5 (adds session expiry). */
  protocolVersion?: 4 | 5;
  /** MQTT 5 only: how long the broker keeps our queued messages while we're down. */
  sessionExpirySeconds?: number;
}

interface MeshConfig {
  root?: string;
  agentId?: string;
  servicesFile?: string;
  /** Reject invokes with no requestedBy (protocol 1.2 requires it). Default true. */
  requireOwner?: boolean;
  /**
   * Verify requestedBy against a broker-injected client_username (EMQX rule engine).
   * Fails closed: with this on, an invoke lacking client_username is rejected.
   * Default false — requires broker-side payload enrichment to be wired first.
   */
  verifyOwner?: boolean;
  /** Hard wall-clock cap per job before the mesh declares it failed. Default 30 min. */
  maxJobDurationMs?: number;
  /**
   * Deployment values substituted into ${VAR} placeholders in capability
   * prompts. Keeps the catalog portable: the same services.json runs in every
   * deployment, and only these differ.
   *
   * Checked before process.env, so a deployment can override an inherited
   * environment without touching it. Put non-secret identifiers here (Slack
   * ids, channel names, repo paths); leave anything genuinely secret in the
   * environment.
   */
  promptVars?: Record<string, string>;
}

interface Capability {
  service: string;
  description?: string;
  requestSchema?: Record<string, unknown>;
  responseSchema?: Record<string, unknown>;
  avgLatency?: string;
  handler?: "session" | "silent";
  prompt?: string;
  [k: string]: unknown;
}

interface ServicesFile {
  agentId?: string;
  displayName?: string;
  protocolVersion?: string;
  capabilities: Capability[];
}

interface PluginConfig {
  broker: BrokerConfig;
  sessionKey?: string;
  mesh?: MeshConfig;
  web?: { enabled?: boolean; path?: string; auth?: string; port?: number };
}

/** One milestone on a job's timeline, as published to the events topic. */
interface JobEvent {
  type: string;
  note?: string;
  ts: number;
}

interface JobRecord {
  jobId: string;
  service?: string;
  state: "accepted" | "started" | "done" | "error" | "duplicate" | "timeout" | "cancelled" | "rejected";
  lastEvent?: string;
  result?: unknown;
  requestedBy?: string;
  owner?: string;
  /**
   * Milestone history. Previously only the most recent event survived, which
   * threw away exactly the information needed to answer "what did this job
   * actually do?" — including requeues, the signal that matters most when a
   * job misbehaves.
   */
  events?: JobEvent[];
  createdAt?: number;
  finishedAt?: number;
  updatedAt: number;
}

const TERMINAL_STATES = new Set(["done", "error", "duplicate", "timeout", "cancelled", "rejected"]);

// ── Plugin Entry ───────────────────────────────────────

export default definePluginEntry({
  id: "mqtt-bridge",
  name: "MQTT Bridge — Agent Mesh",
  description: "Agent mesh over MQTT with an HTTP control API and web panel.",

  register(api: any) {
    const pluginConfig = (api.pluginConfig ?? {}) as Partial<PluginConfig>;
    const logger = api.logger;
    const cfg = pluginConfig;

    // Transport runs ONLY in the gateway ("full" registration). CLI/discovery
    // loads of this plugin must not connect to the broker and must not process
    // job invokes — a CLI-mode api has no runtime.subagent/system and pollutes
    // the mesh with "inject failed" error results (incident 2026-08-24 15:24,
    // rev-018/019).
    if ((api as any).registrationMode && (api as any).registrationMode !== "full") {
      logger.info(`mqtt-bridge: registrationMode=${(api as any).registrationMode} — transport inactive (gateway-only plugin).`);
      return;
    }

    if (!cfg.broker?.url) {
      logger.warn("mqtt-bridge: no broker.url configured — plugin inactive.");
      return;
    }

    const GUARD = Symbol.for("mqtt-bridge.active");
    const globalAny = globalThis as Record<symbol, unknown>;

    /**
     * Operational alert that must reach the operator.
     *
     * Some deployments capture only info-level plugin output, so a warn/error
     * alone can be silently dropped — an alert nobody can see is not an alert.
     * Emitted at error level for correctness and mirrored to info so it is
     * actually visible. Reserved for state changes an operator must act on;
     * ordinary warnings still use logger.warn.
     */
    const alert = (msg: string) => {
      logger.error(msg);
      logger.info(`[ALERT] ${msg}`);
    };

    const meshRoot = cfg.mesh?.root ?? "agents";
    const agentId = cfg.mesh?.agentId ?? "agent";
    const servicesFile = cfg.mesh?.servicesFile ?? path.join(pluginDir, "services.json");
    const sessionKey = cfg.sessionKey ?? "agent:main:main";
    const requireOwner = cfg.mesh?.requireOwner !== false; // default true
    const verifyOwner = cfg.mesh?.verifyOwner === true;    // default false
    const MAX_JOB_DURATION_MS = cfg.mesh?.maxJobDurationMs ?? 30 * 60_000;
    const configVars = (cfg.mesh?.promptVars ?? {}) as Record<string, string>;

    // ── Deployment secrets ─────────────────────────────
    //
    // Panel-managed values live in their own file rather than openclaw.json:
    // that file belongs to the gateway, is JSONC with comments and trailing
    // commas, and rewriting it programmatically would destroy formatting the
    // operator wrote by hand. This one is ours, 0600, and gitignored.
    //
    // Resolution order for ${VAR}: openclaw.json wins (config-as-code is the
    // operator's explicit intent), then this file, then the environment.
    const secretsFile = path.join(pluginDir, "mesh.local.json");
    let localVars: Record<string, string> = {};

    function readLocalVars(): Record<string, string> {
      try {
        const parsed = JSON.parse(fs.readFileSync(secretsFile, "utf8"));
        const vars = parsed?.promptVars;
        return vars && typeof vars === "object" ? vars as Record<string, string> : {};
      } catch { return {}; }   // absent is the normal case, not an error
    }
    function writeLocalVars(vars: Record<string, string>): string | null {
      try {
        // Written 0600 and replaced atomically, so a crash mid-write cannot
        // leave a half-file that silently drops every variable.
        const tmp = `${secretsFile}.tmp`;
        fs.writeFileSync(tmp, JSON.stringify({ promptVars: vars }, null, 2) + "\n", { mode: 0o600 });
        fs.renameSync(tmp, secretsFile);
        try { fs.chmodSync(secretsFile, 0o600); } catch { /* best effort on odd filesystems */ }
        localVars = vars;
        return null;
      } catch (e: any) {
        // Never include values in an error string — this reaches logs.
        logger.error(`mqtt-bridge: writing ${path.basename(secretsFile)} failed: ${e.code ?? e.message}`);
        return `could not write ${path.basename(secretsFile)}`;
      }
    }
    localVars = readLocalVars();
    if (Object.keys(localVars).length) {
      logger.info(`mqtt-bridge: ${Object.keys(localVars).length} local prompt variable(s) loaded`);
    }

    /** Effective value, by precedence. Values never leave this process. */
    const varValue = (k: string): string | undefined =>
      configVars[k] ?? localVars[k] ?? process.env[k];
    /** Where a variable resolves from — safe to send to the panel. */
    const varSource = (k: string): "config" | "local" | "env" | "unset" =>
      k in configVars ? "config" : k in localVars ? "local" : process.env[k] !== undefined ? "env" : "unset";
    const knownVarNames = () => [...new Set([
      ...Object.keys(configVars), ...Object.keys(localVars),
    ])].sort();
    const topics = {
      profile: `${meshRoot}/registry/${agentId}/profile`,
      status: `${meshRoot}/registry/${agentId}/status`,
      invoke: `${meshRoot}/commands/${agentId}/invoke`,
      query: `${meshRoot}/commands/${agentId}/query`,
      cancel: `${meshRoot}/commands/${agentId}/cancel`,
      config: `${meshRoot}/commands/${agentId}/config`,
    };

    const escapeRe = (s: string) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    // Scoped job topics published by anyone on this mesh (including our executors).
    const JOB_TOPIC_RE = new RegExp(`^${escapeRe(meshRoot)}/jobs/([^/]+)/([^/]+)/(events|result)$`);

    // ── Catalog ────────────────────────────────────────

    let lastProfileMtime = 0;

    function readServices(): ServicesFile {
      try {
        return JSON.parse(fs.readFileSync(servicesFile, "utf8")) as ServicesFile;
      } catch (e: any) {
        // The catalog is deployment-local and therefore not tracked in git.
        // On a fresh clone it won't exist yet, so fall back to the shipped
        // example rather than starting with an empty, silently useless agent.
        const example = servicesFile.replace(/\.json$/, ".example.json");
        try {
          const svc = JSON.parse(fs.readFileSync(example, "utf8")) as ServicesFile;
          logger.warn(
            `mqtt-bridge: ${path.basename(servicesFile)} not readable — falling back to ` +
            `${path.basename(example)}. Copy it to ${path.basename(servicesFile)} to customise.`,
          );
          return svc;
        } catch { /* no example either — report the original failure */ }
        logger.error(`mqtt-bridge: read ${servicesFile} failed: ${e.message}`);
        return { capabilities: [] };
      }
    }
    function writeServices(svc: ServicesFile): boolean {
      try {
        fs.writeFileSync(servicesFile, JSON.stringify(svc, null, 2) + "\n");
        return true;
      } catch (e: any) {
        logger.error(`mqtt-bridge: write services failed: ${e.message}`);
        return false;
      }
    }

    // ── Shared state (MQTT + HTTP + SSE all read it) ───

    let client: mqtt.MqttClient | null = null;
    const stats = { rx: 0, tx: 0, reconnects: 0, connectedAt: 0, lastError: "" };
    /**
     * Connection facts the operator needs to trust the mesh. Mutable and
     * populated once the transport is configured, so the HTTP layer can read
     * it without depending on declaration order.
     *
     * `durable` is the one that matters: it goes false when a clientId
     * collision forces a disambiguated session, which silently changes whether
     * jobs published during downtime survive.
     */
    const session = { clientId: "", mqttVersion: 4, keepalive: 30, durable: true };
    const activeJobs = new Set<string>();
    // Jobs cancelled by a client. The mesh guarantees no further job traffic
    // after cancel_acknowledged, so any late executor publish is suppressed.
    const cancelledJobs = new Set<string>();

    // ── Dispatch watchdog ─────────────────────────────
    // enqueueSystemEvent is fire-and-forget: if the receiving session turn is
    // failing (e.g. after a network/DNS outage), the injected job message can
    // be swallowed and the job stays "active" forever (incident 2026-08-24,
    // rev-014). The watchdog verifies execution and re-injects with backoff.
    //
    // Liveness signal: the subagent run's own settlement, NOT "did the executor
    // publish an events message". The old heuristic re-dispatched any job that
    // worked silently for 5 minutes — a large PR review — causing duplicate
    // execution. A run that is still in flight is alive by definition.
    const WATCHDOG_INTERVAL_MS = 60_000;   // supervisory sweep (not a delivery path)
    const REINJECT_AFTER_MS = 5 * 60_000;  // fallback path only: silence before re-inject
    const MAX_REINJECTS = 2;               // then fail loudly with an error result
    interface WatchEntry {
      jobId: string;
      service: string;
      owner: string;
      messageText: string;
      dispatchedAt: number;
      lastAgentEventAt: number; // last milestone published by the executor
      reinjections: number;
      runId?: string;           // set when push dispatch succeeded
      runSettled: boolean;      // true once waitForRun resolved (ok/error/timeout)
      subagentSessionKey: string;
    }
    const watchedJobs = new Map<string, WatchEntry>();
    const jobHistory: JobRecord[] = []; // ring, newest last
    const MAX_HISTORY = 100;

    // ── SSE fan-out (replaces the panel's 2.5s poll) ───

    const sseClients = new Set<ServerResponse>();
    function sseBroadcast(event: string, data: unknown) {
      if (!sseClients.size) return;
      const frame = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
      for (const res of [...sseClients]) {
        try {
          res.write(frame);
        } catch {
          sseClients.delete(res);
        }
      }
    }
    function brokerSnapshot() {
      return {
        connected: !!client?.connected,
        uptimeMs: stats.connectedAt ? Date.now() - stats.connectedAt : 0,
        rx: stats.rx, tx: stats.tx, reconnects: stats.reconnects,
        lastError: stats.lastError,
        activeJobs: [...activeJobs],
        agentId, meshRoot, protocolVersion: PROTOCOL_VERSION,
        session: { ...session },
        ownerPolicy: { required: requireOwner, verified: verifyOwner },
        // Names and SOURCES only — the panel flags unbound ${VAR} references and
        // shows where each resolves from. Values are deployment secrets and
        // never reach a browser.
        promptVars: knownVarNames().map((k) => ({ name: k, source: varSource(k) })),
        secretsAuth: Boolean(webAuth),
      };
    }

    const MAX_JOB_EVENTS = 24;

    function recordJob(
      rec: Partial<JobRecord> & { jobId: string },
      event?: { type: string; note?: string },
    ) {
      const now = Date.now();
      const existing = jobHistory.find((j) => j.jobId === rec.jobId);
      let merged: JobRecord;
      if (existing) {
        Object.assign(existing, rec, { updatedAt: now });
        merged = existing;
      } else {
        merged = { state: "accepted", createdAt: now, events: [], updatedAt: now, ...rec } as JobRecord;
        jobHistory.push(merged);
        if (jobHistory.length > MAX_HISTORY) jobHistory.shift();
      }
      merged.createdAt ??= now;
      if (event) {
        (merged.events ??= []).push({ type: event.type, note: event.note, ts: now });
        if (merged.events.length > MAX_JOB_EVENTS) merged.events.shift();
      }
      // Stamped once, so a late duplicate publish can't restart the clock.
      if (TERMINAL_STATES.has(merged.state)) merged.finishedAt ??= now;
      sseBroadcast("job", merged);
    }

    function buildProfile() {
      const svc = readServices();
      return {
        agentId,
        displayName: svc.displayName ?? agentId,
        status: client?.connected ? "online" : "offline",
        protocolVersion: svc.protocolVersion ?? PROTOCOL_VERSION,
        capabilities: svc.capabilities,
        commands: ["invoke", "query", "cancel", "config"],
        executionModel: "transport in framework; logic in agent",
        // Advertised so clients can tell what this deployment actually enforces
        // rather than inferring it from the version number alone.
        ownerPolicy: { required: requireOwner, verified: verifyOwner },
        updatedAt: new Date().toISOString(),
      };
    }

    function publishProfile() {
      if (!client) return;
      try { lastProfileMtime = fs.statSync(servicesFile).mtimeMs; } catch { /* noop */ }
      client.publish(topics.profile, JSON.stringify(buildProfile()), { qos: 1, retain: true });
      logger.info(`mqtt-bridge: profile published (${readServices().capabilities.length} capabilities)`);
      sseBroadcast("profile", buildProfile());
    }

    // owner scope for job topics — listeners subscribe jobs/<theirId>/# and only get their own traffic
    const ownerScope = (requestedBy?: string): string => {
      const s = String(requestedBy ?? "").toLowerCase().replace(/[^a-z0-9_-]+/g, "-").replace(/^-+|-+$/g, "");
      return s || "public";
    };

    function jobEvent(jobId: string, event: Record<string, unknown>, owner: string) {
      client?.publish(
        `${meshRoot}/jobs/${owner}/${jobId}/events`,
        JSON.stringify({ ...event, jobId, owner, ts: new Date().toISOString() }),
        { qos: 1 },
      );
    }
    function markAgentActivity(jobId: string) {
      const w = watchedJobs.get(jobId);
      if (w) w.lastAgentEventAt = Date.now();
    }
    /** Publish a terminal result: owner-scoped topic, retained, QoS 1. */
    function jobResult(jobId: string, result: Record<string, unknown>, owner: string) {
      const payload = JSON.stringify({ ...result, jobId, owner, ts: new Date().toISOString() });
      client?.publish(`${meshRoot}/jobs/${owner}/${jobId}/result`, payload, { qos: 1, retain: true });
    }

    // ── Wake with retry (older-runtime fallback path only) ──

    async function wakeWithRetry(tag: string, attempt = 0): Promise<void> {
      const MAX = 12;
      try {
        const hb = await api.runtime.system.runHeartbeatOnce({ reason: "mqtt-agent-mesh" });
        const status = (hb as any)?.status;
        if (status === "ran") return;
        if (status === "skipped" && attempt < MAX) {
          setTimeout(() => void wakeWithRetry(tag, attempt + 1), 10_000 + attempt * 2_000);
          return;
        }
        logger.warn(`mqtt-bridge[${tag}]: wake not run (${status}/${(hb as any)?.reason})`);
      } catch (e: any) {
        if (attempt < MAX) {
          setTimeout(() => void wakeWithRetry(tag, attempt + 1), 10_000);
        } else {
          logger.error(`mqtt-bridge[${tag}]: wake failed: ${e.message}`);
        }
      }
    }

    /**
     * Resolve ${ENV_VAR} inside a capability prompt, at dispatch time.
     *
     * Deliberately NOT resolved in readServices(): the capability catalog is
     * published to the RETAINED profile topic, so resolving there would
     * broadcast every deployment secret to anyone subscribing the registry.
     * Templates stay templates on the wire and are only filled in on the way
     * to the executor.
     *
     * Deployment identifiers (Slack ids, internal repo names, channel ids)
     * belong here — the capability definition stays portable and the values
     * live in the environment.
     */
    const resolvePromptEnv = (s: string): string =>
      s.replace(/\$\{([A-Za-z_][A-Za-z0-9_]*)\}/g, (_m, k: string) => {
        // Plugin config first, environment second. Config is versioned and
        // survives service-env regeneration; env stays available for secrets.
        const v = varValue(k);
        if (v === undefined) {
          logger.warn(`mqtt-bridge: prompt references unset variable ${k} — substituted empty ` +
                      `(set it in the panel, in mesh.promptVars, or export ${k})`);
          return "";
        }
        return String(v);
      });

    // ── Job dispatch (shared by MQTT invoke + HTTP API) ─

    function dispatchJob(
      data: { jobId?: string; service: string; args?: Record<string, unknown>; requestedBy?: string },
      opts: { defaultOwner?: string; clientUsername?: string } = {},
    ): { ok: boolean; jobId?: string; error?: string } {
      const jobId = data.jobId || `job-${Date.now().toString(36)}`;
      const { service, args = {} } = data;

      // ── Owner resolution (protocol 1.2: requestedBy is REQUIRED) ──
      let requestedBy = String(data.requestedBy ?? "").trim();
      if (!requestedBy && opts.defaultOwner) requestedBy = opts.defaultOwner;
      if (!requestedBy) {
        if (requireOwner) {
          const err = "requestedBy is required (protocol 1.2); job rejected";
          logger.warn(`mqtt-bridge: rejected job ${jobId} for service "${service}" — ${err}`);
          // Publish where a spec-following client could still see it, and mirror
          // to the flat topic so this is never a silent drop.
          jobResult(jobId, { type: "error", error: err, service }, "public");
          recordJob({ jobId, service, state: "rejected", lastEvent: "missing requestedBy" });
          return { ok: false, error: err, jobId };
        }
        logger.warn(`mqtt-bridge: job ${jobId} has no requestedBy — defaulting owner to "public" (mesh.requireOwner is off)`);
      }

      const owner = ownerScope(requestedBy);

      // ── Optional broker-identity verification (protocol 1.2, opt-in) ──
      if (verifyOwner) {
        const claimed = owner;
        const actual = opts.clientUsername ? ownerScope(opts.clientUsername) : "";
        if (!actual) {
          const err = "owner verification enabled but broker did not supply client_username";
          logger.error(`mqtt-bridge: rejected job ${jobId} — ${err}`);
          jobResult(jobId, { type: "error", error: err, service }, claimed);
          recordJob({ jobId, service, state: "rejected", requestedBy, owner: claimed, lastEvent: "unverifiable owner" });
          return { ok: false, error: err, jobId };
        }
        if (actual !== claimed) {
          const err = `requestedBy "${claimed}" does not match broker identity "${actual}"`;
          logger.error(`mqtt-bridge: rejected job ${jobId} — ${err}`);
          jobResult(jobId, { type: "error", error: err, service }, actual);
          recordJob({ jobId, service, state: "rejected", requestedBy, owner: actual, lastEvent: "owner mismatch" });
          return { ok: false, error: err, jobId };
        }
      }

      const cap = readServices().capabilities.find((c) => c.service === service);
      if (!cap) {
        const err = `unknown service "${service}"`;
        jobResult(jobId, { type: "error", error: err }, owner);
        recordJob({ jobId, service, state: "rejected", requestedBy, owner, lastEvent: err });
        return { ok: false, error: err, jobId };
      }
      if (activeJobs.has(jobId)) {
        jobResult(jobId, { type: "duplicate", note: "jobId already active" }, owner);
        recordJob({ jobId, service, state: "duplicate", requestedBy, owner });
        return { ok: false, error: "duplicate jobId", jobId };
      }

      cancelledJobs.delete(jobId);
      activeJobs.add(jobId);
      jobEvent(jobId, { type: "accepted", service, requestedBy, args }, owner);
      recordJob({ jobId, service, state: "accepted", requestedBy, owner, lastEvent: `args: ${JSON.stringify(args).slice(0, 200)}` });

      // Push dispatch: run the executor as a subagent turn directly from the
      // plugin (same primitive as sessions_spawn) the moment the job arrives —
      // WhatsApp-style push. No enqueueSystemEvent/heartbeat dependency, so a
      // busy or failing main session turn can never swallow the job.
      const subagentSessionKey = `agent:main:subagent:mesh-${jobId}`;
      // NOTE: you are already the isolated executor. This message is delivered
      // by subagent.run() into a dedicated session, so the work must happen
      // HERE. Delegating to a nested subagent would let this run settle while
      // the real work continues elsewhere — which the watchdog correctly reads
      // as "finished without a result" and re-dispatches, duplicating the job.
      const report =
        `EXECUTION: you are the isolated executor for this job — run it here, in this session. ` +
        `Do NOT spawn a nested subagent: this run ending is what signals the job is finished, so ` +
        `delegating would cause the bridge to re-dispatch and duplicate the work.\n` +
        `PROGRESS: publish a milestone to ${meshRoot}/jobs/${owner}/${jobId}/events at least every 2 minutes (type: started, analyzing, result-ready, …).\n` +
        `RESULT: publish the terminal payload to ${meshRoot}/jobs/${owner}/${jobId}/result with a "type" field ` +
        `(review | already_reviewed | error | duplicate). The bridge injects jobId/owner/ts and forces retain, but set "type" yourself.\n` +
        `SCOPE: owner is "${owner}" — publish only to the owner-scoped topics above.`;

      let messageText: string;
      if (cap.prompt) {
        // Env FIRST, then args. Reversing this would let a caller smuggle
        // "${SOME_SECRET}" through an arg value and have the bridge expand it —
        // turning any invoke into an environment read.
        messageText = resolvePromptEnv(String(cap.prompt))
          .replace(/\{\{jobId\}\}/g, jobId)
          .replace(/\{\{requestedBy\}\}/g, String(requestedBy || "unknown"))
          .replace(/\{\{(\w+)\}\}/g, (_m, k: string) => String((args as any)?.[k] ?? ""));
        messageText += `\n\n${report}`;
      } else {
        messageText =
          `Agent-mesh job.\nJobId: ${jobId}\nService: ${service}\n` +
          `Description: ${cap.description ?? ""}\nArgs: ${JSON.stringify(args)}\n` +
          (requestedBy ? `Requested by: ${requestedBy}\n` : "") + `\n${report}`;
      }

      const watch: WatchEntry = {
        jobId, service, owner, messageText,
        dispatchedAt: Date.now(), lastAgentEventAt: Date.now(),
        reinjections: 0, runSettled: false, subagentSessionKey,
      };

      try {
        const sub = (api.runtime as any).subagent;
        if (typeof sub?.run === "function") {
          watchedJobs.set(jobId, watch);
          void sub.run({ sessionKey: subagentSessionKey, message: messageText })
            .then((r: { runId: string }) => {
              watch.runId = r.runId;
              logger.info(`mqtt-bridge: job ${jobId} executor subagent started (runId ${r.runId})`);
              // Await terminal state. Settlement — not silence — is the
              // watchdog's liveness signal.
              return sub.waitForRun({ runId: r.runId, timeoutMs: 0 }).catch(() => null as any);
            })
            .then((w: { status?: string; error?: string } | null) => {
              watch.runSettled = true;
              if (w?.status === "error" && activeJobs.has(jobId) && !cancelledJobs.has(jobId)) {
                logger.error(`mqtt-bridge: executor run for ${jobId} failed: ${w.error ?? "unknown"}`);
                watchedJobs.delete(jobId);
                activeJobs.delete(jobId);
                jobResult(jobId, { type: "error", error: `executor run failed: ${w.error ?? "unknown"}` }, owner);
                recordJob({ jobId, state: "error", lastEvent: "executor run failed" });
              }
            })
            .catch((e: any) => {
              watch.runSettled = true;
              logger.error(`mqtt-bridge: subagent dispatch for ${jobId} failed: ${e.message}`);
              // Watchdog re-dispatches now that the run is marked settled.
            });
          jobEvent(jobId, { type: "started", note: `push-dispatched to executor subagent ${subagentSessionKey}` }, owner);
          recordJob({ jobId, service, state: "started", requestedBy, owner, lastEvent: "push dispatch (subagent)" });
          return { ok: true, jobId };
        }
        // Fallback (older runtime without subagent API): system-event queue + wake pull.
        logger.warn(`mqtt-bridge: subagent runtime unavailable — falling back to system-event dispatch for ${jobId}`);
        api.runtime.system.enqueueSystemEvent(messageText, { sessionKey });
        watchedJobs.set(jobId, watch);
        jobEvent(jobId, { type: "started", note: "dispatched to agent session (fallback)" }, owner);
        recordJob({ jobId, service, state: "started", requestedBy, owner });
        void wakeWithRetry(`job ${jobId}`);
        return { ok: true, jobId };
      } catch (e: any) {
        activeJobs.delete(jobId);
        watchedJobs.delete(jobId);
        jobResult(jobId, { type: "error", error: `inject failed: ${e.message}` }, owner);
        recordJob({ jobId, service, state: "error", owner });
        return { ok: false, error: e.message, jobId };
      }
    }

    // ── Cooperative cancellation ──────────────────────
    // The plugin runtime exposes run/waitForRun/getSession/deleteSession — there
    // is no abort primitive — so mid-run termination cannot be guaranteed. What
    // the mesh *can* guarantee is the contract clients depend on: a terminal
    // result lands immediately and no further traffic appears for that job.
    function cancelJob(jobId: string, requestedBy?: string): boolean {
      const w = watchedJobs.get(jobId);
      const wasActive = activeJobs.delete(jobId);
      if (!wasActive && !w) return false;
      const owner = w?.owner ?? ownerScope(requestedBy);
      cancelledJobs.add(jobId);
      watchedJobs.delete(jobId);
      jobEvent(jobId, { type: "cancel_acknowledged" }, owner);
      // Terminal + retained, so listeners are never left hanging on a cancel.
      jobResult(jobId, { type: "cancelled", note: "cancelled by request", requestedBy }, owner);
      recordJob({ jobId, state: "cancelled", lastEvent: "cancelled", owner, requestedBy });

      // Best effort: drop the executor session so it stops consuming budget.
      const sub = (api.runtime as any).subagent;
      if (w?.subagentSessionKey && typeof sub?.deleteSession === "function") {
        void Promise.resolve(sub.deleteSession({ sessionKey: w.subagentSessionKey, deleteTranscript: false }))
          .then(() => logger.info(`mqtt-bridge: cancelled job ${jobId} — executor session dropped`))
          .catch((e: any) => logger.warn(`mqtt-bridge: cancel ${jobId}: deleteSession failed: ${e.message}`));
      }
      return true;
    }

    // ── Watchdog loop (supervisory; not a delivery path) ──

    const watchdogTimer = setInterval(() => {
      const now = Date.now();
      for (const w of [...watchedJobs.values()]) {
        const age = now - w.dispatchedAt;

        // Hard wall-clock cap regardless of liveness.
        if (age > MAX_JOB_DURATION_MS) {
          logger.error(`mqtt-bridge[watchdog]: job ${w.jobId} exceeded max duration ${Math.round(MAX_JOB_DURATION_MS / 60_000)}min — failing`);
          watchedJobs.delete(w.jobId);
          activeJobs.delete(w.jobId);
          jobEvent(w.jobId, { type: "timeout", note: "exceeded max job duration" }, w.owner);
          jobResult(w.jobId, {
            type: "error",
            error: `job exceeded maximum duration of ${Math.round(MAX_JOB_DURATION_MS / 60_000)} minutes`,
          }, w.owner);
          recordJob({ jobId: w.jobId, state: "timeout", lastEvent: "max duration exceeded" });
          continue;
        }

        if (w.runId && !w.runSettled) continue;        // run in flight → alive by definition

        // Run settled (or we never had a handle). Settlement alone is NOT
        // sufficient grounds to re-dispatch: the terminal publish may still be
        // in flight, and an executor that delegates can outlive the run that
        // started it. Require genuine silence as well, so re-dispatch only
        // happens when nothing has been heard from the executor at all.
        if (now - w.lastAgentEventAt < REINJECT_AFTER_MS) continue;

        if (w.reinjections < MAX_REINJECTS) {
          const why = w.runId ? "run settled without publishing a result" : "no executor activity";
          w.reinjections++;
          w.dispatchedAt = now;
          w.lastAgentEventAt = now;
          w.runSettled = false;
          w.runId = undefined;
          logger.warn(
            `mqtt-bridge[watchdog]: job ${w.jobId} (service ${w.service}) — ${why} — re-injecting (${w.reinjections}/${MAX_REINJECTS})`,
          );
          jobEvent(w.jobId, {
            type: "requeued",
            note: `${why} — re-dispatching executor (attempt ${w.reinjections}/${MAX_REINJECTS})`,
          }, w.owner);
          recordJob({ jobId: w.jobId, state: "started", lastEvent: `watchdog re-inject #${w.reinjections}` });
          try {
            const sub = (api.runtime as any).subagent;
            if (typeof sub?.run === "function") {
              void sub.run({
                sessionKey: w.subagentSessionKey,
                message: `🔁 Watchdog re-dispatch for agent-mesh job ${w.jobId} — the previous run ended without publishing a result.\n\n${w.messageText}`,
              })
                .then((r: { runId: string }) => {
                  w.runId = r.runId;
                  return sub.waitForRun({ runId: r.runId, timeoutMs: 0 }).catch(() => null);
                })
                .then(() => { w.runSettled = true; })
                .catch((e: any) => {
                  w.runSettled = true;
                  logger.error(`mqtt-bridge[watchdog]: re-dispatch failed for ${w.jobId}: ${e.message}`);
                });
            } else {
              api.runtime.system.enqueueSystemEvent(
                `🔁 Watchdog re-inject for agent-mesh job ${w.jobId} (service ${w.service}, owner ${w.owner}) — original dispatch was not confirmed as executed.\n\n${w.messageText}`,
                { sessionKey },
              );
              void wakeWithRetry(`watchdog ${w.jobId}`);
            }
          } catch (e: any) {
            logger.error(`mqtt-bridge[watchdog]: re-inject failed for ${w.jobId}: ${e.message}`);
          }
        } else {
          // Exhausted retries → fail loudly so listeners aren't left hanging.
          logger.error(
            `mqtt-bridge[watchdog]: job ${w.jobId} failed to execute after ${MAX_REINJECTS} re-injections — publishing timeout error`,
          );
          watchedJobs.delete(w.jobId);
          activeJobs.delete(w.jobId);
          jobEvent(w.jobId, { type: "timeout", note: "executor never produced a result" }, w.owner);
          jobResult(w.jobId, {
            type: "error",
            error: `execution not confirmed after ${MAX_REINJECTS} re-injections`,
          }, w.owner);
          recordJob({ jobId: w.jobId, state: "timeout", lastEvent: "watchdog timeout" });
        }
      }
    }, WATCHDOG_INTERVAL_MS);
    watchdogTimer.unref?.();

    // ── Config API (shared by MQTT + HTTP) ─────────────

    function runConfigAction(msg: any): Record<string, unknown> {
      const action = msg?.action;
      const svc = readServices();
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
          if (svc.capabilities.some((c) => c.service === s.service))
            return wrap(false, { error: `exists (use update_service)`, service: s.service });
          svc.capabilities.push(s);
          if (!writeServices(svc)) return wrap(false, { error: "write failed" });
          publishProfile();
          return wrap(true, { added: s.service });
        }
        case "remove_service": {
          const before = svc.capabilities.length;
          svc.capabilities = svc.capabilities.filter((c) => c.service !== msg.service);
          if (svc.capabilities.length === before) return wrap(false, { error: "not found" });
          if (!writeServices(svc)) return wrap(false, { error: "write failed" });
          publishProfile();
          return wrap(true, { removed: msg.service });
        }
        case "update_service": {
          const idx = svc.capabilities.findIndex((c) => c.service === msg.service);
          if (idx < 0) return wrap(false, { error: "not found" });
          svc.capabilities[idx] = { ...svc.capabilities[idx], ...(msg.patch ?? {}), service: msg.patch?.service ?? msg.service };
          if (!writeServices(svc)) return wrap(false, { error: "write failed" });
          publishProfile();
          return wrap(true, { updated: svc.capabilities[idx].service });
        }
        default:
          return wrap(false, { error: `unknown action`, available: ["list", "reload", "add_service", "remove_service", "update_service"] });
      }
    }

    // ══ HTTP API + static UI ═══════════════════════════

    const webDir = path.join(pluginDir, "web");
    const uiCfg = (cfg.web ?? {}) as NonNullable<PluginConfig["web"]>;
    const webEnabled = uiCfg.enabled !== false; // default on
    const webAuth = typeof uiCfg.auth === "string" && uiCfg.auth.trim() ? uiCfg.auth.trim() : "";
    const base = uiCfg.path ?? "/mqtt-bridge/ui";

    function sendJson(res: ServerResponse, code: number, obj: unknown) {
      res.writeHead(code, { "Content-Type": "application/json" });
      res.end(JSON.stringify(obj));
    }
    async function readBody(req: IncomingMessage): Promise<any> {
      const chunks: Buffer[] = [];
      for await (const c of req) chunks.push(c as Buffer);
      try { return JSON.parse(Buffer.concat(chunks).toString() || "{}"); } catch { return {}; }
    }
    /** web.auth was previously declared but never enforced — a silent no-op. */
    function authorized(req: IncomingMessage, url: URL): boolean {
      if (!webAuth) return true;
      return hasToken(req, url);
    }
    function hasToken(req: IncomingMessage, url: URL): boolean {
      if (!webAuth) return false;
      const header = String(req.headers.authorization ?? "");
      if (header === `Bearer ${webAuth}` || header === webAuth) return true;
      // EventSource cannot set headers, so allow a query token for the SSE stream.
      return url.searchParams.get("token") === webAuth;
    }
    /**
     * Secret operations demand a configured token, always — unlike the rest of
     * the panel, which may run open on loopback. Reading or writing deployment
     * secrets over an unauthenticated endpoint is not a trade-off worth making
     * for convenience, so this fails closed when web.auth is unset.
     */
    const elevated = (req: IncomingMessage, url: URL) => Boolean(webAuth) && hasToken(req, url);

    /**
     * Cross-site request forgery guard for state-changing routes.
     *
     * Binding to 127.0.0.1 keeps other machines out, but any page in the
     * operator's browser can POST to localhost. A custom header cannot be set
     * cross-origin without a preflight this server never approves, and a
     * cross-site Origin is rejected outright, so a hostile page cannot dispatch
     * jobs, edit the catalog or touch secrets.
     */
    function sameOrigin(req: IncomingMessage): boolean {
      if (String(req.headers["x-mesh-panel"] ?? "") !== "1") return false;
      const origin = req.headers.origin;
      if (!origin) return true;                    // non-browser client (curl, scripts)
      try { return new URL(String(origin)).host === String(req.headers.host ?? ""); }
      catch { return false; }
    }

    const handleUiRequest = async (req: IncomingMessage, res: ServerResponse): Promise<boolean> => {
        const url = new URL(req.url ?? "/", "http://local");
        let p = url.pathname;
        // On the standalone port, accept both /api/* and the gateway-style path
        if (!p.startsWith(base) && p.startsWith("/api/")) p = base + p;

        if (!authorized(req, url)) {
          res.writeHead(401, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ ok: false, error: "unauthorized" }));
          return true;
        }

        // ── SSE stream: push, replaces the panel's polling loop ──
        if (p === `${base}/api/events`) {
          res.writeHead(200, {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache, no-transform",
            Connection: "keep-alive",
            "X-Accel-Buffering": "no",
          });
          res.write(`retry: 3000\n\n`);
          res.write(`event: status\ndata: ${JSON.stringify(brokerSnapshot())}\n\n`);
          res.write(`event: profile\ndata: ${JSON.stringify(buildProfile())}\n\n`);
          res.write(`event: snapshot\ndata: ${JSON.stringify({ active: [...activeJobs], history: jobHistory.slice().reverse() })}\n\n`);
          sseClients.add(res);
          const ka = setInterval(() => { try { res.write(`: ka\n\n`); } catch { /* noop */ } }, 25_000);
          ka.unref?.();
          const drop = () => { clearInterval(ka); sseClients.delete(res); };
          req.on("close", drop);
          req.on("error", drop);
          return true;
        }

        // ── API ──
        if (p === `${base}/api/profile`) {
          const profile = buildProfile();
          sendJson(res, 200, { ...profile, broker: { connected: !!client?.connected, stats } });
          return true;
        }
        if (p === `${base}/api/status`) {
          sendJson(res, 200, brokerSnapshot());
          return true;
        }
        if (p === `${base}/api/jobs`) {
          sendJson(res, 200, {
            active: [...activeJobs],
            history: jobHistory.slice().reverse(), // newest first
          });
          return true;
        }
        // ── Deployment variables ──
        // GET returns names, sources and a masked hint — never a value. There
        // is deliberately no way to read a secret back out of this API: the
        // panel only ever needs to know what is set, not what it is.
        if (p === `${base}/api/secrets`) {
          if (!elevated(req, url)) {
            sendJson(res, 403, {
              ok: false,
              authRequired: true,
              error: webAuth
                ? "A valid token is required to manage deployment variables."
                : "Set web.auth in the plugin config to manage deployment variables from the panel.",
            });
            return true;
          }
          if (req.method === "GET") {
            sendJson(res, 200, {
              ok: true,
              file: path.basename(secretsFile),
              vars: knownVarNames().map((k) => {
                const v = varValue(k) ?? "";
                return {
                  name: k,
                  source: varSource(k),
                  // Enough to recognise a value, never enough to reconstruct it.
                  hint: v.length > 8 ? `••••${v.slice(-4)}` : v ? "••••" : "",
                  editable: varSource(k) !== "config",
                };
              }),
            });
            return true;
          }
          if (req.method === "POST") {
            if (!sameOrigin(req)) { sendJson(res, 403, { ok: false, error: "cross-origin request refused" }); return true; }
            const body = await readBody(req);
            const name = String(body.name ?? "").trim();
            if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
              sendJson(res, 400, { ok: false, error: "Name must look like AN_ENV_VAR: letters, digits and underscores, not starting with a digit." });
              return true;
            }
            if (name in configVars) {
              sendJson(res, 409, { ok: false, error: `${name} is pinned in openclaw.json (mesh.promptVars) and takes precedence. Remove it there to manage it here.` });
              return true;
            }
            const next = { ...localVars };
            if (body.delete === true) delete next[name];
            else {
              const value = String(body.value ?? "");
              if (!value) { sendJson(res, 400, { ok: false, error: "Value cannot be empty. Use delete to remove it." }); return true; }
              next[name] = value;
            }
            const err = writeLocalVars(next);
            if (err) { sendJson(res, 500, { ok: false, error: err }); return true; }
            // Logged by NAME only. The value must never reach a log line.
            logger.info(`mqtt-bridge: prompt variable ${name} ${body.delete === true ? "removed" : "set"} via panel`);
            sseBroadcast("status", brokerSnapshot());
            sendJson(res, 200, { ok: true, name, removed: body.delete === true });
            return true;
          }
        }

        if (p === `${base}/api/invoke` && req.method === "POST") {
          if (!sameOrigin(req)) { sendJson(res, 403, { ok: false, error: "cross-origin request refused" }); return true; }
          const body = await readBody(req);
          const r = dispatchJob(
            { jobId: body.jobId, service: body.service, args: body.args, requestedBy: body.requestedBy },
            // The panel is an authenticated local operator surface; it supplies
            // its own identity rather than relying on the required-owner check.
            { defaultOwner: "web-ui", clientUsername: verifyOwner ? "web-ui" : undefined },
          );
          sendJson(res, r.ok ? 200 : 400, r);
          return true;
        }
        if (p === `${base}/api/cancel` && req.method === "POST") {
          if (!sameOrigin(req)) { sendJson(res, 403, { ok: false, error: "cross-origin request refused" }); return true; }
          const body = await readBody(req);
          const ok = cancelJob(String(body.jobId ?? ""), body.requestedBy ?? "web-ui");
          sendJson(res, ok ? 200 : 404, { ok, jobId: body.jobId });
          return true;
        }
        if (p === `${base}/api/config` && req.method === "POST") {
          if (!sameOrigin(req)) { sendJson(res, 403, { ok: false, error: "cross-origin request refused" }); return true; }
          const body = await readBody(req);
          const r = runConfigAction(body);
          sendJson(res, r.ok ? 200 : 400, r);
          return true;
        }

        // ── Static (SPA) ──
        let rel = p.slice(base.length) || "/";
        if (!/\.[a-zA-Z0-9]+$/.test(rel)) rel = "/index.html";
        const file = path.normalize(path.join(webDir, rel));
        if (!file.startsWith(webDir)) { res.writeHead(403); res.end("forbidden"); return true; }
        const types: Record<string, string> = {
          ".html": "text/html; charset=utf-8", ".js": "text/javascript",
          ".css": "text/css", ".json": "application/json", ".png": "image/png", ".svg": "image/svg+xml",
        };
        try {
          const data = fs.readFileSync(file);
          res.writeHead(200, { "Content-Type": types[path.extname(file)] ?? "application/octet-stream" });
          res.end(data);
        } catch {
          res.writeHead(404); res.end("not found");
        }
        return true;
    };

    // ── Reload takeover ────────────────────────────────
    //
    // This used to bail out when GUARD was already set, which meant a
    // hot-reloaded plugin never took over: the gateway kept serving the OLD
    // module and code updates silently required a full restart. Worse, the
    // previous module's MQTT client stayed connected — so with a stable
    // clientId two clients could briefly hold the same session and kick each
    // other in a loop.
    //
    // Instead: tear the previous instance down, then take over.
    const CLIENT_SLOT = Symbol.for("mqtt-bridge.client");
    const DISPOSE_SLOT = Symbol.for("mqtt-bridge.dispose");
    const MODULE_SLOT = Symbol.for("mqtt-bridge.module");
    if (globalAny[GUARD]) {
      if (globalAny[MODULE_SLOT] === MODULE_INSTANCE) {
        // Same loaded module, additional session. The transport is already up
        // and belongs to this module — leave it completely alone. Tearing it
        // down here would reconnect the broker on every dispatched job, since
        // spawning an executor subagent re-registers the plugin.
        logger.info("mqtt-bridge: additional registration for the active module — transport untouched");
        return;
      }
      // Different module id => the plugin was rebuilt and re-imported. This is
      // the genuine hot-reload case, where taking over is what we want.
      logger.info("mqtt-bridge: new build detected — disposing previous instance and taking over");
      try { (globalAny[DISPOSE_SLOT] as (() => void) | undefined)?.(); }
      catch (e: any) { logger.error(`mqtt-bridge: previous dispose failed: ${e.message}`); }
    }
    globalAny[MODULE_SLOT] = MODULE_INSTANCE;
    // Belt and braces: end any client the previous module left behind, even if
    // its dispose hook was missing or threw. force=true so we do not wait on a
    // graceful DISCONNECT that may never complete.
    const staleClient = globalAny[CLIENT_SLOT] as mqtt.MqttClient | undefined;
    if (staleClient) {
      try { staleClient.end(true); } catch { /* already gone */ }
      globalAny[CLIENT_SLOT] = undefined;
    }
    globalAny[GUARD] = true;

    // Standalone HTTP server on its own port — independent of the gateway
    // lifecycle for browsing; the plugin process hosts it either way.
    let standaloneServer: ReturnType<typeof createServer> | null = null;
    if (webEnabled) {
      const standalonePort = uiCfg.port ?? 8765;
      standaloneServer = createServer((req, res) => { void handleUiRequest(req, res); });
      standaloneServer.on("error", (e: any) => logger.warn(`mqtt-bridge: standalone UI port ${standalonePort} failed: ${e.message}`));
      standaloneServer.listen(standalonePort, "127.0.0.1", () => {
        logger.info(`mqtt-bridge: standalone UI listening on http://127.0.0.1:${standalonePort}${webAuth ? " (auth required)" : ""}`);
      });
    } else {
      logger.info("mqtt-bridge: web panel disabled (web.enabled=false)");
    }


    // ── MQTT connect ───────────────────────────────────

    const resolveEnv = (val?: string): string | undefined => {
      if (!val) return undefined;
      const m = val.match(/^\$\{(.+)\}$/);
      return m ? process.env[m[1]] : val;
    };

    logger.info(`mqtt-bridge: connecting to ${cfg.broker.url}`);
    const username = resolveEnv(cfg.broker.username);
    const password = resolveEnv(cfg.broker.password);

    /**
     * clientId MUST be stable across restarts.
     *
     * It used to be suffixed with process.pid, which gave every restart a new
     * MQTT identity — so `clean:false` bought nothing: the broker's queued QoS-1
     * messages stayed with the dead session and any invoke published while the
     * gateway was down was lost, silently. It also leaked one orphaned session
     * per restart (MQTT 3.1.1 has no session-expiry interval).
     *
     * Derived instead from host + install path, so it is stable for this
     * deployment and distinct from any other. If you genuinely run two gateways
     * from the SAME directory, set broker.clientId explicitly per instance —
     * durability and multi-instance safety are in tension and the operator has
     * to pick. A collision shows up as a kick-loop, which is detected below.
     */
    const stableSuffix = createHash("sha1")
      .update(`${os.hostname()}::${pluginDir}`)
      .digest("hex")
      .slice(0, 10);
    const baseClientId = cfg.broker.clientId ?? `openclaw-mqtt-bridge-${stableSuffix}`;
    // Disambiguator appended only after a collision is detected (see below).
    // Empty in the normal case, so the session stays durable.
    let idSuffix = "";
    const currentClientId = () => `${baseClientId}${idSuffix}`;
    logger.info(`mqtt-bridge: clientId ${baseClientId} (persistent session, clean:false)`);

    const protocolVersion = cfg.broker.protocolVersion ?? 4;
    const sessionExpirySeconds = cfg.broker.sessionExpirySeconds ?? 86_400;
    session.clientId = baseClientId;
    session.mqttVersion = protocolVersion;
    session.keepalive = cfg.broker.keepalive ?? 30;
    // Kick-loop detector: a stable clientId is what makes the session durable,
    // but two instances sharing one id will fight over it. Surface that clearly
    // instead of letting it look like flaky networking.
    const recentConnects: number[] = [];

    // Wrapped in a function so the collision fallback below can rebuild the
    // connection under a different clientId without restarting the plugin.
    function connect() {
      client = mqtt.connect(cfg.broker!.url, {
        username,
        password,
        clientId: currentClientId(),
        keepalive: cfg.broker!.keepalive ?? 30,
        clean: false,
        reconnectPeriod: 5_000,
        connectTimeout: 15_000,
        protocolVersion: protocolVersion as 4 | 5,
        // MQTT 5 only: bounds how long the broker holds our queued messages,
        // so a decommissioned deployment stops accumulating them forever.
        ...(protocolVersion === 5
          ? { properties: { sessionExpiryInterval: sessionExpirySeconds } }
          : {}),
        will: {
          topic: topics.status,
          payload: Buffer.from(JSON.stringify({ status: "offline", reason: "unexpected-disconnect", timestamp: new Date().toISOString() })),
          qos: 1, retain: true,
        },
      });
      globalAny[CLIENT_SLOT] = client;
      attachHandlers();
    }

    // ── services.json change detection: push (fs.watch), not a poll ──
    // Watch the directory rather than the file so editor rename-on-save is caught.
    let profileDebounce: NodeJS.Timeout | null = null;
    const scheduleProfileRepublish = () => {
      if (profileDebounce) clearTimeout(profileDebounce);
      profileDebounce = setTimeout(() => {
        try {
          const m = fs.statSync(servicesFile).mtimeMs;
          if (m !== lastProfileMtime) publishProfile();
        } catch { /* noop */ }
      }, 250);
      profileDebounce.unref?.();
    };
    let servicesWatcher: fs.FSWatcher | null = null;
    try {
      const dir = path.dirname(servicesFile);
      const baseName = path.basename(servicesFile);
      servicesWatcher = fs.watch(dir, { persistent: false }, (_ev, fname) => {
        if (!fname || fname === baseName) scheduleProfileRepublish();
      });
      logger.info("mqtt-bridge: services.json watched via fs.watch (push)");
    } catch (e: any) {
      logger.warn(`mqtt-bridge: fs.watch unavailable (${e.message}) — falling back to mtime polling`);
    }
    // Slow reconciler: cheap safety net for filesystems where watch is unreliable
    // (network mounts, some containers). 30s when watch failed, 5min when it works.
    const mtimeTimer = setInterval(() => {
      try {
        if (fs.statSync(servicesFile).mtimeMs !== lastProfileMtime) publishProfile();
      } catch { /* noop */ }
    }, servicesWatcher ? 300_000 : 30_000);
    mtimeTimer.unref?.();


    function attachHandlers() {
    client.on("connect", () => {
      stats.connectedAt = Date.now();
      const now = Date.now();
      recentConnects.push(now);
      while (recentConnects.length && now - recentConnects[0] > 60_000) recentConnects.shift();
      if (recentConnects.length >= 5 && !idSuffix) {
        // Two clients are fighting over one session and kicking each other.
        // Durability and stability are in direct conflict here, and stability
        // wins: an agent that reconnects every few seconds processes nothing,
        // whereas a non-durable session merely loses jobs published while it
        // was down. Take a distinct id and say so plainly.
        idSuffix = `-${createHash("sha1")
          .update(`${process.pid}:${recentConnects[0]}`)
          .digest("hex")
          .slice(0, 4)}`;
        recentConnects.length = 0;
        session.durable = false;
        session.clientId = `${baseClientId}${idSuffix}`;
        alert(
          `mqtt-bridge: clientId collision on "${baseClientId}" — 5+ connects in 60s means another ` +
          `client holds this session and the broker is kicking us back and forth. Switching to ` +
          `"${currentClientId()}" to break the loop. DURABILITY IS NOW DEGRADED: this is a fresh ` +
          `session, so invokes published while this agent is offline will NOT be queued. Fix the ` +
          `collision (set a distinct broker.clientId per instance) and restart to restore it.`,
        );
        try { client?.end(true); } catch { /* noop */ }
        setTimeout(() => connect(), 1_000);
        return;
      }

      client!.subscribe({
        [topics.invoke]: { qos: 1 }, [topics.query]: { qos: 1 },
        [topics.cancel]: { qos: 1 }, [topics.config]: { qos: 1 },
        [`${meshRoot}/jobs/#`]: { qos: 1 }, // history for the web panel
      }, (err) => err && logger.error(`mqtt-bridge: subscribe failed: ${err.message}`));

      client!.publish(topics.status, JSON.stringify({ status: "online", timestamp: new Date().toISOString() }), { qos: 1, retain: true });
      publishProfile();
      logger.info(`mqtt-bridge: connected (MQTT ${protocolVersion === 5 ? "5" : "3.1.1"}) — commands + jobs-history subscribed`);
      sseBroadcast("status", brokerSnapshot());
    });
    client.on("reconnect", () => { stats.reconnects++; logger.warn("mqtt-bridge: reconnecting…"); sseBroadcast("status", brokerSnapshot()); });
    client.on("error", (err) => { stats.lastError = err.message; logger.error(`mqtt-bridge: MQTT error: ${err.message}`); sseBroadcast("status", brokerSnapshot()); });
    client.on("offline", () => { logger.warn("mqtt-bridge: broker offline"); sseBroadcast("status", brokerSnapshot()); });
    client.on("close", () => { logger.warn("mqtt-bridge: connection closed"); sseBroadcast("status", brokerSnapshot()); });

    client.on("message", (topic: string, payload: Buffer) => {
      stats.rx++;
      const raw = payload.toString();
      logger.info(`mqtt-bridge: received on ${topic}: ${raw.slice(0, 300)}`);
      let data: any = null;
      try { data = JSON.parse(raw); } catch { /* plaintext */ }

      // job history — tracks milestones and results the executor publishes
      const jm = topic.match(JOB_TOPIC_RE);
      if (jm) {
        const owner = decodeURIComponent(jm[1]);
        const jobId = decodeURIComponent(jm[2]);
        const kind = jm[3];

        // A cancelled job is terminal — suppress any late executor publish so
        // the client's view matches the cancel_acknowledged contract.
        if (cancelledJobs.has(jobId)) return;

        if (kind === "events") {
          const type = String(data?.type ?? "message");
          // note carries whatever the executor chose to say; fall back to a
          // trimmed payload so a non-conforming publish still shows something.
          const note = data?.note ?? data?.stage ?? data?.error ??
            (data ? undefined : raw.slice(0, 120));
          recordJob(
            { jobId, lastEvent: type, requestedBy: data?.owner, owner },
            { type, note: note ? String(note).slice(0, 240) : undefined },
          );
          markAgentActivity(jobId); // any events publish = executor is alive
        } else {
          recordJob(
            { jobId, result: data, state: data?.type === "error" ? "error" : "done", requestedBy: data?.owner, owner },
            { type: String(data?.type ?? "result"), note: data?.error ? String(data.error).slice(0, 240) : undefined },
          );
          activeJobs.delete(jobId);
          watchedJobs.delete(jobId); // terminal state — stop watching
        }
        return;
      }

      if (topic === topics.config) {
        const r = runConfigAction(data);
        client!.publish(`${topics.config}/reply`, JSON.stringify(r), { qos: 1 });
        return;
      }
      if (topic === topics.query) {
        const svc = readServices();
        const out = data?.jobId
          ? { jobId: data.jobId, state: activeJobs.has(data.jobId) ? "active" : cancelledJobs.has(data.jobId) ? "cancelled" : "unknown-or-finished" }
          : {
              agentId,
              protocolVersion: svc.protocolVersion ?? PROTOCOL_VERSION,
              ownerPolicy: { required: requireOwner, verified: verifyOwner },
              services: svc.capabilities.map((c) => ({ service: c.service, description: c.description, requestSchema: c.requestSchema })),
            };
        client!.publish(`${topics.query}/reply`, JSON.stringify(out), { qos: 1 });
        return;
      }
      if (topic === topics.cancel) {
        const jobId = String(data?.jobId ?? "");
        if (jobId && cancelJob(jobId, data?.requestedBy)) {
          api.runtime.system.enqueueSystemEvent(`🛑 Agent-mesh cancel for job ${jobId}.`, { sessionKey });
          void wakeWithRetry(`cancel ${jobId}`);
        } else {
          jobEvent(jobId || "unknown", { type: "cancel_ignored" }, ownerScope(data?.requestedBy));
        }
        return;
      }
      if (topic === topics.invoke && data) {
        dispatchJob(
          { jobId: data.jobId, service: data.service, args: data.args, requestedBy: data.requestedBy },
          // Populated by an EMQX rule-engine payload enrichment when verifyOwner
          // is on; absent otherwise.
          { clientUsername: data.client_username ?? data.clientUsername },
        );
        return;
      }
    });
    } // end attachHandlers

    connect();

    // ── Agent tool ─────────────────────────────────────

    /**
     * Normalise anything an executor publishes to a job topic.
     *
     * PROTOCOL.md requires every result to carry jobId/owner/ts/type and to be
     * RETAINED. Previously that depended entirely on the executor LLM following
     * a sentence of prose; a forgotten `retain: true` meant late subscribers got
     * nothing. The bridge now guarantees it at the transport boundary.
     */
    function normalizeJobPublish(topic: string, payload: string, retainReq?: boolean) {
      const m = JOB_TOPIC_RE.exec(topic);
      if (!m) return { payload, retain: retainReq ?? false };
      const [, ownerRaw, jobIdRaw, kind] = m;
      let obj: any;
      try { obj = JSON.parse(payload); } catch { obj = { note: payload }; }
      if (obj === null || typeof obj !== "object" || Array.isArray(obj)) obj = { value: obj };
      const normalized = {
        ...obj,
        jobId: obj.jobId ?? decodeURIComponent(jobIdRaw),
        owner: obj.owner ?? decodeURIComponent(ownerRaw),
        ts: obj.ts ?? new Date().toISOString(),
        type: obj.type ?? (kind === "result" ? "result" : "progress"),
      };
      // Results are retained by protocol; events deliberately are not.
      return { payload: JSON.stringify(normalized), retain: kind === "result" ? true : (retainReq ?? false) };
    }

    api.registerTool({
      name: "mqtt_publish",
      description: "Publish a message to any MQTT topic (job events/results, config, status).",
      parameters: Type.Object({
        payload: Type.String({ description: "Payload (JSON string or text)." }),
        topic: Type.String({ description: "Topic to publish to, e.g. agents/jobs/<owner>/<jobId>/result" }),
        retain: Type.Optional(Type.Boolean({ description: "Retain. Default false; forced true on job result topics." })),
      }),
      async execute(_id: string, params: { payload: string; topic: string; retain?: boolean }) {
        const t = params.topic;
        try {
          const { payload, retain } = normalizeJobPublish(t, params.payload, params.retain);
          client?.publish(t, payload, { qos: 1, retain });
          stats.tx++;
          return { content: [{ type: "text" as const, text: `Published to ${t}${retain ? " (retained)" : ""}` }] };
        } catch (err: any) {
          return { content: [{ type: "text" as const, text: `Failed: ${err.message}` }], isError: true };
        }
      },
    });

    // ── Shutdown ───────────────────────────────────────

    const shutdown = () => {
      clearInterval(mtimeTimer);
      clearInterval(watchdogTimer);
      try { servicesWatcher?.close(); } catch { /* noop */ }
      for (const res of [...sseClients]) { try { res.end(); } catch { /* noop */ } }
      sseClients.clear();
      try { standaloneServer?.close(); } catch { /* noop */ }
      const off = JSON.stringify({ status: "offline", reason: "shutdown", timestamp: new Date().toISOString() });
      client?.publish(topics.status, off, { qos: 1, retain: true });
      client?.end();
      globalAny[CLIENT_SLOT] = undefined;
      delete globalAny[GUARD];
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
