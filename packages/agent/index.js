/**
 * plexus-agent — join an agent mesh in about fifteen lines.
 *
 * A small, dependency-light client for the Agent Mesh Protocol. It speaks the
 * same wire format as the OpenClaw bridge, so an agent written with this is
 * indistinguishable on the broker from one running inside a gateway.
 *
 *   import { connect } from "plexus-agent";
 *
 *   const agent = await connect({
 *     broker: "mqtt://localhost:1883",
 *     agentId: "dba",
 *     capabilities: [{ service: "schema.review", description: "Reviews a migration." }],
 *   });
 *
 *   agent.serve("schema.review", async (job, ctx) => {
 *     ctx.progress("reading the migration");
 *     return { risk: "high" };
 *   });
 *
 * What it handles for you, because each of these is a way meshes break:
 * durable sessions with a stable client id, owner-scoped result routing,
 * lineage on delegated work, hop limits, duplicate rejection, cancel
 * propagation to children, and a terminal result on every path — including the
 * ones where your handler throws.
 *
 * @module plexus-agent
 */

import mqtt from "mqtt";
import os from "node:os";
import { createHash, randomBytes } from "node:crypto";

/** The protocol revision this client implements. */
export const PROTOCOL_VERSION = "1.4";

const DEFAULTS = {
  root: "agents",
  maxDepth: 4,
  askTimeoutMs: 300_000,
  keepalive: 30,
  reconnectPeriod: 5_000,
  requireOwner: true,
  // v1.4: whether to serve commands/<agentId>/invoke/<owner>. "accept" (both
  // forms) or "off" (the v1.3 form only). There is no mode that refuses the
  // old form: refusing is enforcement, and enforcement is the broker's.
  ownerInTopic: "accept",
  // Whether the broker enforces who a requester may claim to be. Stated by
  // whoever applied the rules — this library cannot find it out, and guessing
  // would advertise a guarantee nobody made.
  ownerEnforced: false,
};

/** Terminal result types. Anything unrecognised is also terminal — treat it so. */
const TERMINAL = new Set(["result", "error", "cancelled", "duplicate", "rejected", "timeout"]);

// ── topics ──────────────────────────────────────────────────────────────────
// Kept pure and exported: the topic layout is the protocol's only real API
// surface, and being able to test it without a broker matters.

const escapeRe = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

/**
 * Owner scope for job topics. Lowercased, `[a-z0-9_-]` only, edges trimmed.
 * Empty becomes `public` — which is why a client that omits `requestedBy`
 * never sees its own results on its own filter.
 */
export const ownerScope = (requestedBy) => {
  const s = String(requestedBy ?? "").toLowerCase().replace(/[^a-z0-9_-]+/g, "-").replace(/^-+|-+$/g, "");
  return s || "public";
};

export const topics = {
  profile: (root, id) => `${root}/registry/${id}/profile`,
  status: (root, id) => `${root}/registry/${id}/status`,
  invoke: (root, id) => `${root}/commands/${id}/invoke`,
  /**
   * v1.4: the owner in the topic, so a broker ACL can enforce who a requester
   * claims to be. The owner must ALREADY be scoped — this does not scope it,
   * because a topic that normalises quietly would make one identity two
   * spellings, and an ACL matches only one of them.
   */
  invokeAs: (root, id, owner) => `${root}/commands/${id}/invoke/${owner}`,
  invokeFilter: (root, id) => `${root}/commands/${id}/invoke/+`,
  cancel: (root, id) => `${root}/commands/${id}/cancel`,
  events: (root, owner, jobId) => `${root}/jobs/${owner}/${jobId}/events`,
  result: (root, owner, jobId) => `${root}/jobs/${owner}/${jobId}/result`,
  jobPattern: (root) => new RegExp(`^${escapeRe(root)}/jobs/([^/]+)/([^/]+)/(events|result)$`),
  registryPattern: (root) => new RegExp(`^${escapeRe(root)}/registry/([^/]+)/(profile|status)$`),
};

/**
 * Stable per-deployment identity — the single thing durability rests on.
 *
 * Never derive this from a pid or a timestamp. With a changing id every restart
 * is a *new* MQTT session, so `clean: false` buys nothing: the broker's queued
 * QoS-1 messages stay orphaned with the dead session, and any invoke published
 * while you were down is lost silently rather than delivered on reconnect.
 */
export function deriveClientId(agentId, root) {
  const suffix = createHash("sha1").update(`${os.hostname()}::${root}::${agentId}`).digest("hex").slice(0, 10);
  return `plexus-${agentId}-${suffix}`;
}

const newJobId = (prefix = "job") => `${prefix}-${randomBytes(6).toString("hex")}`;
const nowIso = () => new Date().toISOString();

/**
 * Connect to the mesh and advertise this agent.
 *
 * Resolves once the broker connection is established and the retained profile
 * has been published, so the returned agent is immediately discoverable.
 *
 * @param {object} options
 * @param {string} options.broker            Broker URL, e.g. `mqtt://host:1883`.
 * @param {string} options.agentId           This agent's stable id on the mesh.
 * @param {string} [options.displayName]     Human-facing name. Defaults to `agentId`.
 * @param {Array}  [options.capabilities]    `[{ service, description, requestSchema? }]`.
 * @param {string} [options.root="agents"]   Topic root. Isolates one mesh from another.
 * @param {string} [options.username]        Broker credentials.
 * @param {string} [options.password]
 * @param {string} [options.clientId]        Override the derived id. Set this if two
 *                                           processes share an agentId, otherwise they
 *                                           fight over one session and kick each other.
 * @param {boolean}[options.durable=true]    `clean: false`. Turn off only for short-lived
 *                                           clients that must not accumulate a session.
 * @param {number} [options.maxDepth=4]      Delegation hop limit.
 * @param {number} [options.askTimeoutMs]    How long a delegated ask waits.
 * @param {boolean}[options.requireOwner=true] Reject invokes with no `requestedBy`.
 * @param {(msg: string, meta?: object) => void} [options.log]
 * @returns {Promise<Agent>}
 */
export async function connect(options = {}) {
  const cfg = { ...DEFAULTS, ...options };
  if (!cfg.broker) throw new Error("plexus-agent: `broker` is required, e.g. mqtt://localhost:1883");
  if (!cfg.agentId) throw new Error("plexus-agent: `agentId` is required");

  const { root, agentId } = cfg;
  const log = cfg.log ?? (() => {});
  const capabilities = [...(cfg.capabilities ?? [])];
  const handlers = new Map();          // service  -> handler
  const peers = new Map();             // agentId  -> profile
  const pending = new Map();           // jobId    -> { resolve, reject, timer }
  const active = new Map();            // jobId    -> { controller, children[], owner, settled }
  const watchers = new Set();
  const commandObservers = new Set();
  const peerWaiters = new Set();
  const selfScope = ownerScope(agentId);

  const jobRe = topics.jobPattern(root);
  const regRe = topics.registryPattern(root);
  const cmdRe = new RegExp(`^${escapeRe(root)}/commands/([^/]+)/invoke$`);
  // The v1.4 form. Captured separately so the owner segment reaches onInvoke as
  // it arrived — it is the string the broker authorised.
  const cmdOwnerRe = new RegExp(`^${escapeRe(root)}/commands/([^/]+)/invoke/([^/]+)$`);

  const client = mqtt.connect(cfg.broker, {
    clientId: cfg.clientId ?? deriveClientId(agentId, root),
    username: cfg.username,
    password: cfg.password,
    clean: cfg.durable === false,
    keepalive: cfg.keepalive,
    reconnectPeriod: cfg.reconnectPeriod,
    protocolVersion: cfg.protocolVersion ?? 4,
    // Presence, published by the broker if this process dies without saying goodbye.
    will: {
      topic: topics.status(root, agentId),
      payload: JSON.stringify({ status: "offline", reason: "unexpected-disconnect", ts: nowIso() }),
      qos: 1,
      retain: true,
    },
  });

  const pub = (topic, payload, retain = false) =>
    client.publish(topic, typeof payload === "string" ? payload : JSON.stringify(payload), { qos: 1, retain });

  /** Republish the retained profile. Called on connect and whenever capabilities change. */
  function advertise() {
    pub(topics.profile(root, agentId), {
      agentId,
      displayName: cfg.displayName ?? agentId,
      status: "online",
      protocolVersion: PROTOCOL_VERSION,
      capabilities,
      // What this deployment does with the two invoke forms, so a publisher
      // reads it rather than guessing — and what the BROKER does about owners,
      // which this library only reports.
      ownerPolicy: {
        required: cfg.requireOwner,
        topic: cfg.ownerInTopic,
        verified: cfg.ownerEnforced === true,
      },
      ts: nowIso(),
    }, true);
    pub(topics.status(root, agentId), { status: "online", ts: nowIso() }, true);
  }

  // ── inbound ───────────────────────────────────────────────────────────────

  client.on("message", (topic, buf) => {
    const raw = buf.toString();

    const reg = regRe.exec(topic);
    if (reg) return onRegistry(reg[1], reg[2], raw);

    const job = jobRe.exec(topic);
    if (job) return onJobTopic(decodeURIComponent(job[1]), decodeURIComponent(job[2]), job[3], raw);

    const cmd = cmdRe.exec(topic);
    if (cmd) {
      // Observers see every invoke on the mesh, including ours. Matching on the
      // topic rather than the subscription is what makes the wide filter safe:
      // our own invoke is handled once, no matter which filter delivered it.
      if (commandObservers.size) {
        let parsed = null;
        try { parsed = JSON.parse(raw); } catch { /* ignore unparseable */ }
        if (parsed) {
          for (const o of commandObservers) {
            try { o(decodeURIComponent(cmd[1]), parsed); } catch (err) { log(`observer threw: ${err.message}`); }
          }
        }
      }
      if (decodeURIComponent(cmd[1]) === agentId) return onInvoke(raw);
      return;
    }

    const cmdOwner = cmdOwnerRe.exec(topic);
    if (cmdOwner) {
      // Observers first, and for every agent's invoke — not just ours. The
      // owner is in the topic now, so it is supplied to the handler rather than
      // left to be dug out of a payload that need not carry it.
      if (commandObservers.size) {
        let parsed = null;
        try { parsed = JSON.parse(raw); } catch { /* ignore unparseable */ }
        if (parsed) {
          const owner = decodeURIComponent(cmdOwner[2]);
          for (const o of commandObservers) {
            try { o(decodeURIComponent(cmdOwner[1]), { requestedBy: owner, ...parsed }); }
            catch (err) { log(`observer threw: ${err.message}`); }
          }
        }
      }
      if (decodeURIComponent(cmdOwner[1]) !== agentId) return;
      if (cfg.ownerInTopic === "off") return log("invoke/<owner> received but ownerInTopic is off");
      return onInvoke(raw, decodeURIComponent(cmdOwner[2]));
    }

    if (topic === topics.cancel(root, agentId)) return onCancel(raw);
  });

  function onRegistry(peerId, kind, raw) {
    if (peerId === agentId) return;
    // An empty retained payload, or an offline status, both mean the peer left.
    // Deleting on either is what stops husks accumulating in the registry.
    if (!raw) { peers.delete(peerId); return; }
    let data;
    try { data = JSON.parse(raw); } catch { return; }
    if (kind === "status") {
      if (data.status === "offline") peers.delete(peerId);
      return;
    }
    if (data.status === "offline") { peers.delete(peerId); return; }
    peers.set(peerId, data);
    for (const w of [...peerWaiters]) w(peerId, data);
  }

  function onJobTopic(owner, jobId, kind, raw) {
    if (!raw) return;
    let data;
    try { data = JSON.parse(raw); } catch { return; }

    for (const w of watchers) {
      try { w({ owner, jobId, kind, ...data }); } catch (err) { log(`watcher threw: ${err.message}`); }
    }

    if (kind !== "result") return;
    const waiter = pending.get(jobId);
    if (!waiter) return;
    if (!TERMINAL.has(data.type) && data.type !== undefined) {
      // An unrecognised type is still terminal per the spec; only skip if the
      // publisher explicitly marked it as non-final.
      if (data.final === false) return;
    }
    clearTimeout(waiter.timer);
    pending.delete(jobId);
    waiter.resolve(data);
  }

  // ── serving work ──────────────────────────────────────────────────────────

  async function onInvoke(raw, topicOwner) {
    let msg;
    try { msg = JSON.parse(raw); } catch { return log("invoke: unparseable payload, dropped"); }

    const jobId = msg.jobId || newJobId();
    // v1.4: the topic wins, because it is the part a broker can check. The
    // payload's claim is not preferred over it and not quietly ignored either —
    // they have to agree.
    const owner = topicOwner ?? ownerScope(msg.requestedBy);
    const reject = (type, error) => {
      pub(topics.result(root, owner, jobId), { jobId, owner, type, error, ts: nowIso() }, true);
      log(`rejected ${jobId}: ${error}`);
    };

    // Order matters: each of these refuses *before* any work starts, which is
    // the only point at which refusing is cheap.
    if (topicOwner !== undefined) {
      if (ownerScope(topicOwner) !== topicOwner) {
        return reject("rejected",
          `invoke topic owner "${topicOwner}" is not owner-scoped — use "${ownerScope(topicOwner)}"`);
      }
      if (msg.requestedBy && ownerScope(msg.requestedBy) !== topicOwner) {
        return reject("rejected",
          `requestedBy "${ownerScope(msg.requestedBy)}" disagrees with the invoke topic's owner "${topicOwner}"`);
      }
    } else if (cfg.requireOwner && !msg.requestedBy) {
      return reject("rejected", "requestedBy is required");
    }
    if (active.has(jobId)) {
      return reject("duplicate", `jobId ${jobId} is already active`);
    }
    const depth = Number(msg.depth ?? 0);
    if (depth > cfg.maxDepth) {
      // A cycle allowed to start is a cycle that runs until something else stops it.
      return reject("rejected", `depth ${depth} exceeds maxDepth ${cfg.maxDepth}`);
    }
    const handler = handlers.get(msg.service);
    if (!handler) {
      return reject("error", `unknown service: ${msg.service}`);
    }

    const controller = new AbortController();
    const entry = { controller, children: [], owner, settled: false, service: msg.service };
    active.set(jobId, entry);

    const emit = (payload) => {
      if (entry.settled) return;                       // suppress late output after cancel
      pub(topics.events(root, owner, jobId), { jobId, owner, ts: nowIso(), ...payload });
    };
    const settle = (payload) => {
      if (entry.settled) return;
      entry.settled = true;
      active.delete(jobId);
      pub(topics.result(root, owner, jobId), { jobId, owner, ts: nowIso(), ...payload }, true);
    };

    emit({ type: "accepted", service: msg.service });

    /** @type {JobContext} */
    const ctx = {
      agentId, root, depth, signal: controller.signal,
      progress: (message, extra) => emit({ type: "progress", message, ...extra }),
      emit,
      peers: () => [...peers.values()],
      find: (service) => findPeer(service),
      ask: (peerId, service, args, opts) =>
        askPeer(peerId, service, args, {
          ...opts, parentJobId: jobId, rootJobId: msg.rootJobId ?? jobId,
          childDepth: depth + 1, track: entry, idPrefix: "ask",
        }),
      askAny: (service, args, opts) => {
        const peerId = findPeer(service);
        if (!peerId) return Promise.reject(new Error(`no peer on the mesh offers ${service}`));
        return ctx.ask(peerId, service, args, opts);
      },
    };

    try {
      const out = await handler(msg, ctx);
      if (controller.signal.aborted) return;
      const payload = out && typeof out === "object" && !Array.isArray(out) ? out : { value: out };
      settle({ type: payload.type ?? "result", ...payload });
    } catch (err) {
      if (controller.signal.aborted) return;
      settle({ type: "error", error: err?.message ?? String(err) });
      log(`job ${jobId} failed: ${err?.message ?? err}`);
    }
  }

  function onCancel(raw) {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    const entry = active.get(msg.jobId);
    if (!entry) return;

    pub(topics.events(root, entry.owner, msg.jobId), { jobId: msg.jobId, type: "cancel_acknowledged", ts: nowIso() });
    entry.controller.abort();
    entry.settled = true;
    active.delete(msg.jobId);
    pub(topics.result(root, entry.owner, msg.jobId),
      { jobId: msg.jobId, owner: entry.owner, type: "cancelled", requestedBy: msg.requestedBy, ts: nowIso() }, true);

    // Unwind the chain. Each agent cancels only its own children; those agents
    // cancel theirs in turn, so one cancel stops a chain nobody has a map of.
    for (const child of entry.children) {
      pub(topics.cancel(root, child.peerId), { jobId: child.jobId, requestedBy: agentId });
      const waiter = pending.get(child.jobId);
      if (waiter) {
        clearTimeout(waiter.timer);
        pending.delete(child.jobId);
        waiter.reject(new Error("cancelled by parent"));
      }
    }
  }

  // ── delegating ────────────────────────────────────────────────────────────

  function findPeer(service) {
    for (const [id, profile] of peers) {
      if ((profile.capabilities ?? []).some((c) => c.service === service)) return id;
    }
    return null;
  }

  /**
   * An ask is an ordinary invoke with `requestedBy` set to *this* agent, so the
   * peer's result lands in our own owner scope — which we already subscribe to.
   * That is the whole return path; there is no callback channel.
   */
  function askPeer(peerId, service, args, opts = {}) {
    // `childDepth` is the depth of the job being created, not of its parent.
    // A request entering the mesh from a client is depth 0; a delegated ask is
    // its parent's depth plus one.
    const childDepth = Number(opts.childDepth ?? 0);
    if (childDepth > cfg.maxDepth) {
      // Refuse on the asking side too, so nothing reaches the wire.
      return Promise.reject(new Error(`ask would exceed maxDepth ${cfg.maxDepth} (depth ${childDepth})`));
    }
    const jobId = opts.jobId ?? newJobId(opts.idPrefix ?? "job");
    const timeoutMs = opts.timeoutMs ?? cfg.askTimeoutMs;

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        pending.delete(jobId);
        reject(new Error(`ask ${service} -> ${peerId} timed out after ${timeoutMs}ms`));
      }, timeoutMs);
      if (timer.unref) timer.unref();
      pending.set(jobId, { resolve, reject, timer });
      opts.track?.children.push({ peerId, jobId });

      // v1.4: publish where a broker can check who we claim to be — but only to
      // a peer whose profile says it serves that form. A peer we have not heard
      // from gets the v1.3 form, which every version understands.
      const peerTopic = peers.get(peerId)?.ownerPolicy?.topic;
      const invokeTopic = peerTopic === "accept" || peerTopic === "require"
        ? topics.invokeAs(root, peerId, ownerScope(agentId))
        : topics.invoke(root, peerId);

      pub(invokeTopic, {
        service, args: args ?? {},
        requestedBy: agentId,
        jobId,
        parentJobId: opts.parentJobId,
        rootJobId: opts.rootJobId ?? opts.parentJobId ?? jobId,
        depth: childDepth,
        ts: nowIso(),
      });
    });
  }

  // ── lifecycle ─────────────────────────────────────────────────────────────

  await new Promise((resolve, reject) => {
    const fail = (err) => reject(new Error(`plexus-agent: cannot reach ${cfg.broker} — ${err.message}`));
    client.once("error", fail);
    client.once("connect", () => {
      client.removeListener("error", fail);
      client.subscribe({
        [topics.invoke(root, agentId)]: { qos: 1 },
        // v1.4. Subscribed separately from the v1.3 form, so a broker that
        // refuses one does not take the other with it.
        ...(cfg.ownerInTopic === "off"
          ? {}
          : { [topics.invokeFilter(root, agentId)]: { qos: 1 } }),
        [topics.cancel(root, agentId)]: { qos: 1 },
        [`${root}/registry/+/profile`]: { qos: 1 },
        [`${root}/registry/+/status`]: { qos: 1 },
        [`${root}/jobs/${selfScope}/#`]: { qos: 1 },
      }, (err) => (err ? reject(err) : resolve()));
      advertise();
    });
  });

  client.on("connect", advertise);       // re-advertise after any reconnect
  client.on("error", (err) => log(`mqtt error: ${err.message}`));

  /** @typedef {object} JobContext */

  /** @typedef {object} Agent */
  const agent = {
    agentId, root, client, PROTOCOL_VERSION,

    /** Offer a capability. Registering a service also advertises it. */
    serve(service, handler, meta) {
      handlers.set(service, handler);
      if (!capabilities.some((c) => c.service === service)) {
        capabilities.push({ service, description: meta?.description ?? "", ...meta });
      }
      advertise();
      return agent;
    },

    /** Ask a specific peer and wait for its terminal result. */
    invoke(peerId, service, args, opts) {
      return askPeer(peerId, service, args, opts);
    },

    /** Find a peer offering `service` and ask it. */
    async ask(service, args, opts) {
      const peerId = findPeer(service) ?? (opts?.waitMs ? await agent.waitForPeer(service, opts.waitMs) : null);
      if (!peerId) throw new Error(`no peer on the mesh offers ${service}`);
      return askPeer(peerId, service, args, opts);
    },

    /** What this agent currently offers. */
    capabilities: () => capabilities.map((c) => ({ ...c })),

    /** Everyone currently in the retained registry, this agent excluded. */
    peers: () => [...peers.values()],

    /** The id of a peer offering `service`, or null. */
    find: (service) => findPeer(service),

    /** Resolve once some peer offers `service`. Useful at startup, before discovery settles. */
    waitForPeer(service, timeoutMs = 10_000) {
      const existing = findPeer(service);
      if (existing) return Promise.resolve(existing);
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => { peerWaiters.delete(check); reject(new Error(`no peer offering ${service} after ${timeoutMs}ms`)); }, timeoutMs);
        if (timer.unref) timer.unref();
        const check = () => {
          const id = findPeer(service);
          if (!id) return;
          clearTimeout(timer);
          peerWaiters.delete(check);
          resolve(id);
        };
        peerWaiters.add(check);
      });
    },

    /**
     * Observe job traffic across the whole mesh, not just your own scope.
     *
     * This deliberately subscribes the firehose that ordinary clients are told
     * to avoid. It is right for an observer — a notifier, a dashboard, an audit
     * log — and wrong for a participant, which should stay in its owner scope.
     */
    async watch(handler, filter = "jobs") {
      watchers.add(handler);
      const firehose = `${root}/${filter}/#`;
      const ownScope = `${root}/jobs/${selfScope}/#`;

      // A broker delivers a message once per *matching subscription*, and MQTT
      // 3.1.1 gives the client no way to tell which subscription caused a
      // delivery — so an overlapping narrow filter silently doubles every
      // callback. The fix is to not overlap: take the firehose, then drop the
      // narrow filter it already covers. Both are awaited, so by the time this
      // resolves the swap is complete and no message can arrive twice.
      await new Promise((r) => client.subscribe({ [firehose]: { qos: 1 } }, r));
      const overlaps = firehose === `${root}/jobs/#`;
      if (overlaps) await new Promise((r) => client.unsubscribe(ownScope, r));

      return () => {
        watchers.delete(handler);
        if (watchers.size) return;
        client.unsubscribe(firehose);
        if (overlaps) client.subscribe({ [ownScope]: { qos: 1 } });
      };
    },

    /**
     * Observe every `invoke` on the mesh — who is asking whom, for what, with
     * which arguments.
     *
     * Results are deliberately narrow: they carry the answer, not the question.
     * Anything that needs the *request* (a notifier rendering "changes on
     * acme/web#42", a dashboard drawing the call graph) has to read the invoke
     * itself. This is the supported way to do that.
     *
     * Like `watch`, it swaps the narrow filter for the wide one to avoid
     * double-delivery, and must be awaited.
     */
    async observeCommands(handler) {
      commandObservers.add(handler);
      // Both forms. An observer that watched only `+/invoke` would go quiet the
      // day a mesh moved to v1.4 — and quietly, since there is nothing to see
      // when nobody publishes the old form any more.
      const wide = `${root}/commands/+/invoke`;
      const wideOwned = `${root}/commands/+/invoke/+`;
      const own = topics.invoke(root, agentId);
      const ownOwned = topics.invokeFilter(root, agentId);
      await new Promise((r) => client.subscribe({ [wide]: { qos: 1 }, [wideOwned]: { qos: 1 } }, r));
      await new Promise((r) => client.unsubscribe(own, r));
      await new Promise((r) => client.unsubscribe(ownOwned, r));
      return () => {
        commandObservers.delete(handler);
        if (commandObservers.size) return;
        client.unsubscribe(wide);
        client.unsubscribe(wideOwned);
        client.subscribe({ [own]: { qos: 1 } });
        if (cfg.ownerInTopic !== "off") client.subscribe({ [ownOwned]: { qos: 1 } });
      };
    },

    /** Ask a peer to stop a job. Cooperative: it stops traffic, not necessarily compute. */
    cancel(peerId, jobId) {
      pub(topics.cancel(root, peerId), { jobId, requestedBy: agentId, ts: nowIso() });
    },

    /** Publish an arbitrary message. Escape hatch for protocol extensions. */
    publish: pub,

    /** Withdraw from the registry and disconnect cleanly. */
    async close() {
      for (const [, w] of pending) { clearTimeout(w.timer); }
      pending.clear();
      // Empty retained payload = "this agent has left", rather than a husk that
      // outlives the process and gets asked for work nobody is there to do.
      pub(topics.profile(root, agentId), "", true);
      pub(topics.status(root, agentId), "", true);
      await new Promise((r) => setTimeout(r, 120));
      await new Promise((r) => client.end(false, {}, r));
    },
  };

  return agent;
}

export default { connect, ownerScope, topics, deriveClientId, PROTOCOL_VERSION };
