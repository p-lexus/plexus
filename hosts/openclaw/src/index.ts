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
 * An agent may be on more than one mesh, and each membership is its own
 * connection — see mesh/instance.ts, which is one of them. This file owns what
 * is above them: the tools, the singleton guard, the catalog and variables they
 * share, the panel in front of all of them, and the lifecycle. Behaviour lives
 * in src/mesh/* and src/http/*.
 */

import { Type } from "typebox";
import * as path from "path";
import { createHash } from "crypto";
import type { Server } from "http";

// @ts-expect-error - openclaw types resolve at runtime from the host
import { definePluginEntry } from "openclaw/plugin-sdk/plugin-entry";

import type { PluginConfig } from "./types.js";
import { resolveConfig, resolveMeshes } from "./config.js";
import { startMeshes } from "./mesh/instance.js";
import type { MeshInstance } from "./mesh/instance.js";
import { createLogger } from "./logger.js";
import { createCatalog } from "./mesh/catalog.js";
import { createVarStore } from "./mesh/vars.js";
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

/**
 * Every mesh this agent is on, and how a tool finds the right one.
 *
 * The tools are registered in every agent session and resolve through this at
 * call time. With one mesh every lookup below answers it and nothing about a
 * tool call has changed; with several, which mesh a call is for is derived —
 * from the topic, or from the job the executor is running — rather than left to
 * the model, because a model that picked wrong would be delegating a job across
 * the boundary a broker's rules exist to enforce.
 */
interface ActiveMeshes {
  /** The mesh a topic belongs to. A topic names its root, so nothing is guessed. */
  forTopic(topic: string): MeshInstance | undefined;
  /** A mesh by name, or the only one when there is only one. */
  byName(name?: string): MeshInstance | undefined;
  /** The mesh running a job, when this agent has a record of it. */
  forJob(jobId: string): MeshInstance | undefined;
  names(): string[];
  instances(): MeshInstance[];
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
    const meshes = () => globalAny[ACTIVE_SLOT] as ActiveMeshes | undefined;
    const notReady = (what: string) => ({
      content: [{ type: "text" as const, text: `${what}: the mesh bridge is not connected yet.` }],
      isError: true,
    });

    /**
     * Which mesh a tool call is for.
     *
     * Derived, never taken on trust. A job that arrived on one mesh may only
     * delegate on that mesh — a root is the boundary a broker's rules enforce,
     * and two meshes can each have a `dba`, so picking the wrong one is not a
     * refused call but a job quietly done by the wrong agent.
     *
     * So the job decides, on the same rule v1.4 applies to an invoke's owner:
     * where the executor names the job it is running, that job's mesh wins, and
     * a `mesh` argument disagreeing with it is refused rather than reconciled.
     * The argument is only consulted where there is nothing better, and where
     * there is one mesh there is no question to answer.
     */
    function meshFor(
      what: string,
      params: { mesh?: string; parentJobId?: string },
    ): { mesh: MeshInstance } | { error: string } {
      const all = meshes();
      if (!all) return { error: `${what}: the mesh bridge is not connected yet.` };
      const names = all.names();

      const byJob = params.parentJobId ? all.forJob(params.parentJobId) : undefined;
      const named = params.mesh ? all.byName(params.mesh) : undefined;

      if (params.mesh && !named) {
        return { error: `${what}: there is no mesh called "${params.mesh}" here — this agent is on ${names.join(", ")}.` };
      }
      if (byJob && named && byJob !== named) {
        return { error:
          `${what}: job ${params.parentJobId} is running on ${byJob.name}, and this asks for ${named.name}. ` +
          `Work does not move between meshes — refused rather than sent to the wrong one.` };
      }
      const picked = byJob ?? named ?? all.byName();
      if (!picked) {
        return { error:
          `${what}: this agent is on ${names.join(", ")}, so name one — pass mesh, or pass parentJobId ` +
          `and the job's own mesh is used.` };
      }
      return { mesh: picked };
    }

    /** A tool's `mesh` argument, described the same way wherever it appears. */
    const meshParam = Type.Optional(Type.String({
      description: "Which mesh, when this agent is on more than one. Omitted, the job you name in " +
        "parentJobId decides, and a single-mesh agent needs neither.",
    }));

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
        const all = meshes();
        if (!all) return notReady("mqtt_publish");
        // A topic begins with its mesh root, so which connection carries it is
        // a fact about the string rather than a decision anybody makes.
        const inst = all.forTopic(params.topic);
        if (!inst) {
          return { content: [{ type: "text" as const, text:
            `Refused: ${params.topic} is not on any mesh this agent is on (${all.names().join(", ")}). ` +
            `A topic starts with its mesh root.` }], isError: true };
        }
        const refusal = inst.active.refuse(params.topic);
        if (refusal) {
          logger.info(`mqtt_publish refused: ${refusal}`);
          return { content: [{ type: "text" as const, text: `Refused: ${refusal}.` }], isError: true };
        }
        try {
          const { payload, retain } = inst.active.normalize(params.topic, params.payload, params.retain);
          inst.active.publishCounted(params.topic, payload, { qos: 1, retain });
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
        parentJobId: Type.Optional(Type.String({
          description: "The job you are currently executing. Its mesh is the one you can reach.",
        })),
        mesh: meshParam,
      }),
      async execute(_id: string, params: { service?: string; mesh?: string; parentJobId?: string }) {
        const picked = meshFor("mesh_peers", params);
        if ("error" in picked) return { content: [{ type: "text" as const, text: picked.error }], isError: true };
        const inst = picked.mesh;
        const list = params.service ? inst.active.providersOf(params.service) : inst.active.peers();
        if (!list.length) {
          return { content: [{ type: "text" as const, text: params.service
            ? `No agent on ${inst.name} offers "${params.service}".`
            : `No other agents have published a profile to ${inst.name}.` }] };
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
          description: "The job you are currently executing. Pass it so the chain can be traced and " +
            "cancelled as one request — and so the peer is looked for on that job's own mesh.",
        })),
        mesh: meshParam,
      }),
      async execute(_id: string, params: {
        agent: string; service: string; args?: any; parentJobId?: string; mesh?: string;
      }) {
        const picked = meshFor("mesh_ask", params);
        if ("error" in picked) return { content: [{ type: "text" as const, text: picked.error }], isError: true };
        const inst = picked.mesh;
        const mode = inst.active.delegationMode;
        if (mode !== "both" && mode !== "dynamic") {
          return {
            content: [{ type: "text" as const, text: mode === "declared"
              ? "Dynamic delegation is disabled here (mesh.delegation is \"declared\"). This agent only delegates what its capabilities declare up front, and those answers are already in your prompt."
              : "Delegation is disabled on this agent (mesh.delegation is \"off\")." }],
            isError: true,
          };
        }
        const outcome = await inst.active.ask({
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
        mesh: meshParam,
          reason: Type.String({ description: "Why, specifically. One or two sentences." }),
          details: Type.Optional(Type.String({
            description: "What actually happened — what you asked for, what came back, what you " +
              "did with it. The evidence behind the verdict." })),
          lesson: Type.String({
            description: "What a later run of this capability should do. Write one for good work " +
              "too: \"keep checking the rate limit\" is a lesson, \"nice one\" is not." }),
      }),
        async execute(_id: string, params: {
          jobId: string; verdict: string; agent?: string; mesh?: string;
          reason: string; details?: string; lesson: string;
        }) {
        // The job being judged is the one that names the mesh: a verdict belongs
        // to the mesh the work was done on, and there is nowhere else to file it.
        const picked = meshFor("mesh_feedback", { mesh: params.mesh, parentJobId: params.jobId });
        if ("error" in picked) return { content: [{ type: "text" as const, text: picked.error }], isError: true };
        const refused = picked.mesh.active.fileVerdict(params.agent ?? "", params.jobId, params.verdict,
          { reason: params.reason, details: params.details, lesson: params.lesson });
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

    // What every mesh shares. An agent has one catalog, one set of deployment
    // variables and one panel however many meshes it is on: the capabilities
    // are the agent's, and `offer` decides which of them each mesh is told
    // about rather than giving each mesh a catalog of its own to drift.
    const shared0 = resolveConfig(cfg, pluginDir);
    const catalog = createCatalog(
      shared0.mesh.servicesFile, logger, path.join(pluginDir, "services.example.json"),
    );
    const vars = createVarStore(shared0.mesh.secretsFile, shared0.mesh.promptVars, logger);
    const sse = createSseHub();
    const auth = createAuth(shared0.web.auth);

    let memberships;
    try {
      memberships = resolveMeshes(cfg, pluginDir);
    } catch (e: any) {
      // A mesh list that cannot work is worth refusing here rather than at the
      // third reconnect, and refusing loudly: the gateway keeps info from
      // plugins and drops warn, so this is the level an operator can read.
      logger.info(`[mesh] ${e.message} — plugin inactive.`);
      delete globalAny[GUARD];
      delete globalAny[MODULE_SLOT];
      return;
    }

    // Every mesh, or none: a membership is live as soon as it is built, and the
    // shutdown that would stop it is registered further down.
    let instances;
    try {
      instances = startMeshes(memberships, {
        logger, runtime: api.runtime, pluginDir, catalog, vars, sse, auth,
      });
    } catch (e: any) {
      logger.info(`[mesh] could not join every mesh: ${e.message} — plugin inactive.`);
      sse.closeAll();
      delete globalAny[GUARD];
      delete globalAny[MODULE_SLOT];
      return;
    }
    const byMesh = new Map(instances.map((i) => [i.name, i]));

    // The tools reach whichever mesh the work is on through this. Registered in
    // every session, so they resolve at call time rather than closing over one
    // registration's state.
    globalAny[ACTIVE_SLOT] = {
      /** Which mesh a topic belongs to. A topic names its root, so nothing has to be guessed. */
      forTopic(topic: string) {
        return instances.find((i) => topic === i.conf.mesh.root || topic.startsWith(`${i.conf.mesh.root}/`));
      },
      byName(name?: string) {
        if (name) return byMesh.get(name);
        return instances.length === 1 ? instances[0] : undefined;
      },
      names: () => instances.map((i) => i.name),
      /** The mesh a job is running on, when it is one this agent has a record of. */
      forJob(jobId: string) {
        return instances.find((i) => i.jobs.find(jobId));
      },
      instances: () => instances,
    };

    const { server } = startHttpServer({
      cfg: shared0, logger, auth, sse, vars,
      meshes: {
        names: () => instances.map((i) => i.name),
        pick: (name) => {
          if (name) return instances.find((i) => i.name === name);
          // One mesh needs no naming, and that is not a convenience: every
          // panel and script written against a single-mesh agent keeps working
          // exactly as it did.
          return instances.length === 1 ? instances[0] : undefined;
        },
      },
    });

    // ── Shutdown ───────────────────────────────────────

    const shutdown = () => {
      for (const instance of instances) {
        try { instance.stop(); } catch (e: any) { logger.error(`stopping ${instance.name} failed: ${e.message}`); }
      }
      sse.closeAll();
      try { (server as Server | null)?.close(); } catch { /* noop */ }
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
