/**
 * One mesh this agent belongs to: its connection, its peers, its jobs.
 *
 * All of this used to sit inside register(), where being the only mesh was an
 * assumption rather than a decision. It is a unit because a mesh is one — the
 * transport, the peer directory, the job store, the dispatcher, the recorder it
 * heard from and the lessons it read back all belong to a single root, and
 * sharing any of them between two would be the bug this file exists to prevent.
 *
 * The connection is the part that cannot be shared. MQTT carries one Last Will
 * per connection and presence is a will, so one connection spanning two roots
 * could announce its death on only one of them: the other would keep a retained
 * profile saying `online` and go on being sent work.
 *
 * What IS shared sits above and is passed in — the catalog an operator edits,
 * the deployment's variables, the panel's event stream and its token. An agent
 * has one of each of those no matter how many meshes it is on.
 */

import { PROTOCOL_VERSION } from "../types.js";
import type { Said } from "./feedback.js";
import type { Logger, Verdict } from "../types.js";
import type { Membership, ResolvedConfig } from "../config.js";
import {
  buildTopics, jobTopicPattern, parseJobTopic, ownerScope, jobPostmortemTopic,
  memoryAskTopic, memoryReplyFilter, memoryReplyService, boxTopic,
  registryPattern, parseRegistryTopic, registryProfileFilter, registryStatusFilter,
  invokeFilter, invokeTopicOwner, feedbackFilter, feedbackTopicOwner,
} from "./topics.js";
import { normalizeJobPublish, publishRefusal } from "./payload.js";
import { readFeedback, verdictFor } from "./feedback.js";
import { createLimiter, promptFor, signatureOf, triggerFor } from "./postmortem.js";
import { renderLessons } from "./lessons.js";
import { createRecall } from "./recall.js";
import { reviewPromptFor, UNJUDGED } from "./review.js";
import type { Catalog } from "./catalog.js";
import type { VarStore } from "./vars.js";
import { createJobStore } from "./jobs.js";
import { createTransport } from "./transport.js";
import { createDispatcher } from "./dispatch.js";
import { createRegistry } from "./registry.js";
import { createPeerRegistry } from "./peers.js";
import { createAskService } from "./ask.js";
import type { SseHub } from "../http/sse.js";
import type { MeshView } from "./view.js";
import type { Auth } from "../http/auth.js";

/**
 * What every mesh on this agent shares. One catalog, one set of deployment
 * variables, one panel — however many meshes.
 */
export interface SharedDeps {
  logger: Logger;
  runtime: any;
  pluginDir: string;
  catalog: Catalog;
  vars: VarStore;
  sse: SseHub;
  /** The panel's token. One panel, so one of these however many meshes. */
  auth: Auth;
}

/**
 * One mesh, wired and running.
 *
 * It **extends MeshView** — what the panel needs from a mesh — and that
 * is load-bearing rather than tidy: register() hands these objects straight to
 * the panel, so the two shapes have to agree. Declared this way the compiler
 * checks it here, where the object is built.
 *
 * It did not, and that is how a panel shipped calling two members no instance
 * had. The check was not missing — it was erased at the call site, where one
 * untyped `let` under strict:false made the whole list `any`, and `any` is
 * assignable to anything. A contract that only holds where it is consumed is a
 * contract one careless declaration can switch off.
 */
export interface MeshInstance extends MeshView {
  /** What the mesh tools operate through — see ActiveMeshes in index.ts. */
  active: ActiveInstance;
  /** The live peer directory. The panel reads .list(); snapshot() reads .size. */
  peers: ReturnType<typeof createPeerRegistry>;
  transport: ReturnType<typeof createTransport>;
  stop(): void;
}

export interface ActiveInstance {
  publishCounted(topic: string, payload: string, opts?: { qos?: 0 | 1 | 2; retain?: boolean }): void;
  normalize(topic: string, payload: string, retain?: boolean): { payload: string; retain: boolean };
  /** Why this publish must not go out, or null if it may. */
  refuse(topic: string): string | null;
  ask(req: { agent: string; service: string; args?: any; parentJobId?: string }): Promise<any>;
  peers(): any[];
  providersOf(service: string): any[];
  /** File a verdict on a delegated job. Returns why it was refused, or null. */
  fileVerdict(agent: string, jobId: string, verdict: string, said?: Said): string | null;
  delegationMode: string;
}

/**
 * A view of the catalog holding only what this mesh was offered.
 *
 * Both the registry and the dispatcher read the catalog, so filtering here is
 * what makes `offer` mean "not served on this mesh" rather than merely "not
 * advertised there": a capability left out is not found when an invoke for it
 * arrives, and is refused as an unknown service.
 *
 * Writes are not filtered. There is one catalog and the panel edits it; a mesh
 * gets a narrower view of it, not a copy of its own to drift.
 */
export function offering(
  catalog: Catalog,
  offer: string[] | null,
  onMissing: (services: string[]) => void = () => {},
): Catalog {
  if (!offer) return catalog;
  const allowed = new Set(offer);
  let reported = "";
  return {
    ...catalog,
    read: () => {
      const svc = catalog.read();
      const capabilities = (svc.capabilities ?? []).filter((c) => allowed.has(c.service));

      // An offer naming a capability the catalog does not have advertises
      // nothing and says nothing about why — a typo reads exactly like a mesh
      // meant to be quiet. connectAll refuses this outright; here the catalog
      // is a file an operator edits while the agent runs, so a name that is
      // absent now may arrive in a minute. Reported rather than refused, and
      // only when the answer changes, because this is read on every publish.
      const missing = offer.filter((s) => !(svc.capabilities ?? []).some((c) => c.service === s));
      const seen = missing.join(",");
      if (seen !== reported) {
        reported = seen;
        if (missing.length) onMissing(missing);
      }
      return { ...svc, capabilities };
    },
  };
}

/**
 * Build every membership, or none of them.
 *
 * A membership is live the moment it is built: it has a transport dialling out,
 * a watchdog sweeping and a catalog watch registered. So a throw partway
 * through the list used to leave the earlier ones running while the plugin
 * reported itself inactive — nothing held a reference to stop them, because the
 * shutdown that would have is registered after this returns.
 *
 * Rolled back in reverse, for the same reason the plugin host tears plugins
 * down in reverse: the last one built is the least likely to be depended upon.
 *
 * `make` is injected so this can be tested without a broker.
 */
export function startMeshes(
  memberships: Membership[],
  shared: SharedDeps,
  make: (m: Membership, s: SharedDeps) => MeshInstance = createMeshInstance,
): MeshInstance[] {
  const started: MeshInstance[] = [];
  try {
    for (const m of memberships) started.push(make(m, shared));
    return started;
  } catch (err) {
    for (const instance of [...started].reverse()) {
      try { instance.stop(); } catch { /* already failing; the first error is the one that matters */ }
    }
    throw err;
  }
}

/**
 * Anything but a list.
 *
 * The mesh tag is added by spreading, and an array IS an object — so spreading
 * one produces {"0":…,"1":…} and quietly stops being an array. This makes that
 * a compile error rather than something the next reader has to remember.
 */
type NotAList<T> = T extends readonly unknown[] ? never : T;

export function createMeshInstance(membership: Membership, shared: SharedDeps): MeshInstance {
  const { conf, name: meshName } = membership;
  const { logger, pluginDir, vars, sse, auth, runtime } = shared;
  const catalog = offering(shared.catalog, membership.offer, (missing) =>
    logger.info(
      `[mesh] ${meshName} offers ${missing.join(", ")}, which the catalog does not have — ` +
      `nothing is advertised for ${missing.length > 1 ? "them" : "it"} on this mesh`));

  // Every broadcast says which mesh it is about. The panel shows several, and
  // an event that did not say would be attributed to whichever one the reader
  // happened to be looking at.
  //
  // Typed as an object rather than `any`, because the first version took `any`
  // and spread it — and an array IS an object, so a list of peers went out as
  // {"0":…,"1":…,"mesh":…} and stopped being an array. Nothing caught it: the
  // panel does not read that event yet. A payload that is a list is given a
  // name here instead, so the tag can never change what it is tagging.
  const broadcast = <T extends object>(kind: string, payload: NotAList<T>) =>
    sse.broadcast(kind, { ...(payload as object), mesh: meshName });

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

  const jobs = createJobStore((rec) => broadcast("job", rec), {
    file: conf.mesh.historyFile,
    log: (m) => logger.info(m),
  });
  const transport = createTransport(conf, pluginDir, topics.status, logger);
  const peers = createPeerRegistry(conf.mesh.agentId, logger, () => broadcast("peers", { peers: peers.list() }));

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
    // How this mesh is addressed — the segment ?mesh= takes. Not meshRoot:
    // the two differ exactly when two meshes share a root, which is the case
    // a name exists for.
    mesh: meshName,
    meshRoot: conf.mesh.root,
    protocolVersion: PROTOCOL_VERSION,
      // Whether anything on this mesh records. The panel offers no verdict
      // where nothing keeps one, and an operator asking why the feedback
      // cycle is quiet reads the answer here.
      recorder: recall.heard,
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
    onPublished: (profile) => broadcast("profile", profile),
  });

  const dispatcher = createDispatcher({
    cfg: conf, logger, catalog, jobs, vars,
    runtime,
    publish: transport.publish,
    peerSummary: () => peers.summary(),
    lessonsFor: (service) => recall.of(service),
    onCancel: (jobId, requestedBy) => ask.cancelChildren(jobId, requestedBy ?? conf.mesh.agentId),
    // Late-bound: the ask service needs the dispatcher's lineage lookup, so
    // the two are mutually dependent and neither can be built first.
    performAsk: (req) => ask.ask(req),
  });

  // Asked when a command arrives, rather than held. See mesh/recall.ts.
  const recall = createRecall({
    meshRoot: conf.mesh.root,
    agentId: conf.mesh.agentId,
    timeoutMs: conf.mesh.recallTimeoutMs,
    logger,
    publish: transport.publish,
    askTopic: (service) => memoryAskTopic(conf.mesh.root, conf.mesh.agentId, service),
  });
  const explained = createLimiter();

  /**
   * Get a verdict out of this agent for work it asked another to do.
   *
   * Enforced rather than offered: an executor that has moved on will never
   * come back to judge, and a capability nobody judges repeats its mistakes.
   * The floor verdict is the guarantee — a delegation cannot end in silence,
   * and what it records is "delivered, unexamined" rather than praise nobody
   * gave.
   */
  function review(agent: string, jobId: string): void {
    if (!recall.heard) return;
    const rec = jobs.find(jobId);
    if (!rec) return;

    const me = ownerScope(conf.mesh.agentId);
    const judged = () => !!jobs.find(jobId)?.feedback?.some((f) => f.by === me);
    if (judged()) return;

    const floor = () => {
      if (judged()) return;
      const refused = fileVerdict(agent, jobId, UNJUDGED.verdict, {
        reason: UNJUDGED.reason,
        details: `Asked ${agent} for ${rec.service ?? "a capability"} as job ${jobId}. ` +
          `It answered and nothing here judged the answer within ${conf.mesh.reviewGraceMs}ms.`,
      });
      if (refused) logger.info(`[feedback] no verdict for ${jobId}: ${refused}`);
    };

    const sub = (runtime as any)?.subagent;
    if (typeof sub?.run !== "function") { floor(); return; }

    void sub.run({ sessionKey: `${conf.sessionKey}:review`, message: reviewPromptFor(rec, agent) })
      .catch((e: any) => logger.info(`[feedback] could not review ${jobId}: ${e?.message ?? e}`));

    const grace = setTimeout(floor, conf.mesh.reviewGraceMs);
    grace.unref?.();
  }

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
    // A postmortem costs an executor run to write and exists for a recorder
    // to keep. Where nothing answers there is no recorder: it would be paid
    // for, published to a topic the broker most likely refuses without
    // saying so, and read by nobody.
    if (!recall.heard) return;
    if (!explained.take(signatureOf(job, trigger), Date.now())) {
      logger.info(`[postmortem] ${job.service ?? "unknown"} has already explained this failure recently`);
      return;
    }

    const owner = job.owner ?? ownerScope(job.requestedBy);
    const topic = jobPostmortemTopic(conf.mesh.root, owner, jobId);
    const sub = (runtime as any)?.subagent;
    if (typeof sub?.run !== "function") return;

    jobs.record({ jobId }, { type: "postmortem_requested" });
    void sub.run({ sessionKey: `${conf.sessionKey}:postmortem`, message: promptFor(job, trigger, topic) })
      .catch((e: any) => logger.info(`[postmortem] could not start for ${jobId}: ${e?.message ?? e}`));
  }

  /** File a verdict on a job this agent delegated. Returns why not, or null. */
  function fileVerdict(agent: string, jobId: string, verdict: Verdict, said?: Said): string | null {
    const rec = jobs.find(jobId);
    const me = ownerScope(conf.mesh.agentId);

    if (!rec?.delegated) return `job ${jobId} is not one this agent delegated`;
    if (agent && rec.delegatedTo && rec.delegatedTo !== agent) {
      return `job ${jobId} was delegated to ${rec.delegatedTo}, not ${agent}`;
    }
    if (rec.feedback?.some((f) => f.by === me)) {
      return `a verdict on job ${jobId} has already been filed`;
    }
    // A verdict reaches the agent it judges only by way of a recorder: it is
    // filed on one topic and delivered on another, and no agent may publish
    // the delivering one. Without a box it goes nowhere.
    if (!recall.heard) {
      return "nothing records verdicts on this mesh — the feedback cycle needs a Plexus box";
    }

    const out = verdictFor(conf.mesh.root, rec.delegatedTo ?? agent, me, jobId, verdict, said);
    if (!out) return `"${verdict}" is not a verdict — expected good, bad or unusable`;

    transport.publish(out.topic, JSON.stringify(out.payload), { qos: 1 });
    // Recorded here because the relay returns on a topic this agent cannot read.
    // The payload is the authority on what was said: it is what the recorder
    // will keep, already trimmed to the caps.
    const { jobId: _id, verdict: _v, ts: _ts, ...said_ } = out.payload as any;
    jobs.recordFeedback(jobId, { verdict, ...said_, by: me, ts: Date.now() });
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
    onAnswered: (agent, jobId) => review(agent, jobId),
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

    const answered = memoryReplyService(conf.mesh.root, conf.mesh.agentId, topic);
    if (answered !== null) {
      recall.settle(answered, renderLessons(Array.isArray(data?.lessons) ? data.lessons : [], answered));
      return;
    }

    // Peer registry: who else is on the mesh and what they can do.
    const reg = parseRegistryTopic(registryRe, topic);
    if (reg) {
      if (reg.kind === "profile") peers.onProfile(reg.agentId, data);
      else peers.onStatus(reg.agentId, data);
      return;
    }

    // v1.6. A box saying it is here, or its will saying it is gone. This is
    // the only thing that turns the feedback cycle on, and an empty payload
    // is how a retained announcement is withdrawn — so it reads as absence.
    if (topic === boxTopic(conf.mesh.root)) {
      const there = String(raw ?? "").trim() !== "";
      recall.present(there);
      logger.info(there
        ? "[memory] a box records this mesh — verdicts, postmortems and lessons are on"
        : "[memory] no box on this mesh — verdicts, postmortems and lessons are off");
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
            protocolVersion: PROTOCOL_VERSION,
            ownerPolicy: ownerPolicy(),
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
        runtime.system.enqueueSystemEvent(`🛑 Agent-mesh cancel for job ${jobId}.`, { sessionKey: conf.sessionKey });
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
    // v1.5. Where answers about a capability's lessons come back. Under this
    // agent's own commands subtree, so an ACL already grants it and no reply
    // for another agent can arrive here.
    [memoryReplyFilter(conf.mesh.root, conf.mesh.agentId)]: { qos: 1 },
    // v1.6. Whether this mesh has a box. Retained, so the answer is here
    // before the first job is — and absent on a bare broker, which is how
    // the agent knows to publish none of the cycle.
    [boxTopic(conf.mesh.root)]: { qos: 1 },
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
      broadcast("status", snapshot());
    },
    onMessage,
    onStateChange: () => broadcast("status", snapshot()),

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
      broadcast("status", snapshot());
    },
  });

  // Publish this registration as the live instance so the tools — registered
  // in every session — operate on the one transport that actually exists.

  const active: ActiveInstance = {
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
    fileVerdict: (agent: string, jobId: string, verdict: string, said?: Said) =>
      fileVerdict(agent, jobId, verdict as Verdict, said),
    delegationMode: conf.mesh.delegation,
  };

  const stopWatchdog = dispatcher.startWatchdog();
  const stopCatalogWatch = shared.catalog.watch(() => registry.publishProfile());

  return {
    name: meshName,
    conf,
    active,
    snapshot,
    jobs,
    dispatcher,
    registry,
    peers,
    transport,
    fileVerdict,
    // The panel's profile view, with the connection stats beside it — moved
    // here from index.ts when the wiring became per-mesh, and missed then:
    // /api/profile called it on the instance and the instance did not have it,
    // which took the gateway down on every panel request.
    profileWithBroker: () => ({
      ...registry.buildProfile(),
      broker: { connected: transport.connected, stats: transport.stats },
    }),
    stop() {
      stopWatchdog();
      stopCatalogWatch();
      transport.publish(topics.status,
        JSON.stringify({ status: "offline", reason: "shutdown", timestamp: new Date().toISOString() }),
        { qos: 1, retain: true });
      transport.end();
    },
  };
}
