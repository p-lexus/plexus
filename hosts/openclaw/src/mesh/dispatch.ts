/**
 * Job dispatch, cancellation, and the supervisory watchdog.
 *
 * Dispatch is push: the executor runs as a subagent turn started the moment
 * the job arrives, so a busy or failing main session can never swallow it.
 * Older runtimes without a subagent API fall back to a system-event queue plus
 * a heartbeat wake — the only pull-shaped path left in the bridge.
 */

import type {
  Capability, CapabilityDelegate, DispatchOptions, DispatchRequest,
  DispatchResult, Logger, WatchEntry,
} from "../types.js";
import { TERMINAL_STATES } from "../types.js";
import type { ResolvedConfig } from "../config.js";
import type { Catalog } from "./catalog.js";
import type { JobStore } from "./jobs.js";
import type { VarStore } from "./vars.js";
import { jobEventsTopic, jobResultTopic, ownerScope } from "./topics.js";
import { renderPrompt, unresolvedPlaceholders, missingRequiredArgs } from "./payload.js";

const WATCHDOG_INTERVAL_MS = 60_000;   // supervisory sweep, not a delivery path
const REINJECT_AFTER_MS = 5 * 60_000;  // silence required before re-dispatch
const MAX_REINJECTS = 2;               // then fail loudly rather than retry forever
const NUDGE_AFTER_MS = 60_000;         // grace between "I am done" and the result
const MAX_NUDGES = 2;                  // then let the re-dispatch path have it

export interface DispatcherDeps {
  cfg: ResolvedConfig;
  /** Called when a job is cancelled, so anything it delegated stops too. */
  onCancel?(jobId: string, requestedBy?: string): void;
  /** What past runs of a capability reported, already rendered as quoted data. */
  lessonsFor?(service: string): string;
  /** A directory of peers, injected into the executor's briefing. */
  peerSummary?(): string;
  /**
   * Performs a declared delegation. Late-bound because the ask service needs
   * the dispatcher's lineage lookup, so the two are mutually dependent.
   */
  performAsk?(req: { agent: string; service: string; args?: Record<string, unknown>; parentJobId?: string }):
    Promise<{ ok: boolean; jobId: string; agent: string; result?: any; error?: string }>;
  logger: Logger;
  catalog: Catalog;
  jobs: JobStore;
  vars: VarStore;
  runtime: any;
  publish(topic: string, payload: string, opts?: { qos?: 0 | 1 | 2; retain?: boolean }): void;
}

export interface Dispatcher {
  dispatch(data: DispatchRequest, opts?: DispatchOptions): DispatchResult;
  cancel(jobId: string, requestedBy?: string): boolean;
  /** Any executor publish proves the executor is alive; the event says how alive. */
  markAgentActivity(jobId: string, event?: { type?: string; note?: string }): void;
  publishEvent(jobId: string, event: Record<string, unknown>, owner: string): number;
  /** Lineage of a known job, for continuing a delegation chain. */
  lineageOf(jobId?: string): { rootJobId?: string; depth: number };
  publishResult(jobId: string, result: Record<string, unknown>, owner: string): void;
  startWatchdog(): () => void;
  isWatched(jobId: string): boolean;
  forget(jobId: string): void;
}

export function createDispatcher(deps: DispatcherDeps): Dispatcher {
  const { cfg, logger, catalog, jobs, vars, runtime, publish } = deps;
  const root = cfg.mesh.root;
  const watched = new Map<string, WatchEntry>();

  /**
   * Publishes a milestone and answers when it happened.
   *
   * The caller records the same event locally, and the bridge also hears its
   * own publish come back — so the same milestone reaches the job store twice.
   * Returning the timestamp lets both copies carry it, which is what makes
   * them one event rather than two identical rows a millisecond apart.
   */
  const publishEvent = (jobId: string, event: Record<string, unknown>, owner: string): number => {
    const at = Date.now();
    publish(
      jobEventsTopic(root, owner, jobId),
      JSON.stringify({ ...event, jobId, owner, ts: new Date(at).toISOString() }),
      { qos: 1 },
    );
    return at;
  };

  /** Terminal result: owner-scoped, retained, QoS 1. */
  const publishResult = (jobId: string, result: Record<string, unknown>, owner: string) =>
    publish(
      jobResultTopic(root, owner, jobId),
      JSON.stringify({ ...result, jobId, owner, ts: new Date().toISOString() }),
      { qos: 1, retain: true },
    );

  /**
   * Whether this job already published a terminal result.
   *
   * Results are RETAINED, so the last publish wins forever: anything written
   * to a finished job's result topic does not sit beside the answer, it
   * replaces it. The tool-side guard in payload.ts already refuses an executor
   * that tries; this is the same fact, exposed to the two callers inside the
   * bridge that were never asked — the dispatcher, and the watchdog.
   */
  const hasTerminalResult = (jobId: string): boolean => {
    const rec = jobs.find(jobId);
    return !!rec && TERMINAL_STATES.has(rec.state);
  };

  /**
   * An executor milestone that claims the work is over.
   *
   * Deliberately generous: a false positive costs one nudge into a session
   * that is about to publish anyway, while a false negative costs the wait
   * that this whole mechanism exists to remove.
   */
  const announcesCompletion = (event?: { type?: string; note?: string }): boolean => {
    if (!event) return false;
    const said = `${event.type ?? ""} ${event.note ?? ""}`;
    return /result[-_ ]?ready/i.test(said) || /\b(complete|completed|finished)\b/i.test(said);
  };

  const markAgentActivity = (jobId: string, event?: { type?: string; note?: string }) => {
    const w = watched.get(jobId);
    if (!w) return;
    w.lastAgentEventAt = Date.now();
    // Only the first claim starts the clock. Re-arming on every later milestone
    // would let a chatty executor postpone the nudge indefinitely.
    if (w.completionAnnouncedAt === undefined && announcesCompletion(event)) {
      w.completionAnnouncedAt = Date.now();
    }
  };

  /** Resolve ${VAR}, warning by NAME when a reference is unbound. */
  const varOrWarn = (k: string): string => {
    const v = vars.value(k);
    if (v === undefined) {
      logger.warn(
        `prompt references unset variable ${k} — substituted empty ` +
        `(set it in the panel, in mesh.promptVars, or export ${k})`,
      );
      return "";
    }
    return String(v);
  };

  // ── Older-runtime fallback: nudge the session to pick up a queued event ──
  async function wakeWithRetry(tag: string, attempt = 0): Promise<void> {
    const MAX = 12;
    try {
      const hb = await runtime.system.runHeartbeatOnce({ reason: "mqtt-agent-mesh" });
      const status = (hb as any)?.status;
      if (status === "ran") return;
      if (status === "skipped" && attempt < MAX) {
        setTimeout(() => void wakeWithRetry(tag, attempt + 1), 10_000 + attempt * 2_000);
        return;
      }
      logger.warn(`[${tag}] wake not run (${status}/${(hb as any)?.reason})`);
    } catch (e: any) {
      if (attempt < MAX) setTimeout(() => void wakeWithRetry(tag, attempt + 1), 10_000);
      else logger.error(`[${tag}] wake failed: ${e.message}`);
    }
  }

  function executorBriefing(owner: string, jobId: string, depth: number): string {
    const dynamicAllowed = cfg.mesh.delegation === "both" || cfg.mesh.delegation === "dynamic";
    const peers = dynamicAllowed ? (deps.peerSummary?.() ?? "") : "";
    const remaining = cfg.mesh.maxDepth - depth;
    const delegation = peers && remaining > 0
      ? `\nDELEGATION: other agents on this mesh have capabilities you do not. If part of this job ` +
        `is better handled by one of them, call mesh_ask with parentJobId "${jobId}" — it dispatches ` +
        `the work, waits, and returns their answer for you to use. Do not guess at work another agent ` +
        `owns.\nAvailable now:\n${peers}\n` +
        `You may delegate ${remaining} more hop(s) before the mesh refuses.\n`
      : "";
    return delegation + rawBriefing(owner, jobId);
  }

  function rawBriefing(owner: string, jobId: string): string {
    // The bridge already delivered this into a dedicated subagent session, so
    // the executor IS the isolated run. Delegating to a nested subagent would
    // let this run settle while the real work continued elsewhere, which the
    // watchdog correctly reads as "finished without a result" — and re-runs.
    return (
      `EXECUTION: you are the isolated executor for this job — run it here, in this session. ` +
      `Do NOT spawn a nested subagent: this run ending is what signals the job is finished, so ` +
      `delegating would cause the bridge to re-dispatch and duplicate the work.\n` +
      `PROGRESS: publish a milestone to ${jobEventsTopic(root, owner, jobId)} at least every 2 minutes ` +
      `(type: started, analyzing, result-ready, …).\n` +
      `RESULT: publish the terminal payload to ${jobResultTopic(root, owner, jobId)} with a "type" field ` +
      `(review | already_reviewed | error | duplicate). The bridge injects jobId/owner/ts and forces retain, ` +
      `but set "type" yourself. This is the LAST thing you do, and you do it even if you already told ` +
      `someone the answer: a Slack message, an email or a PR comment is a courtesy, not delivery. ` +
      `The requester is waiting on that topic and can see nothing else.\n` +
      `SCOPE: owner is "${owner}" — publish only to the owner-scoped topics above.`
    );
  }

  function dispatch(data: DispatchRequest, opts: DispatchOptions = {}): DispatchResult {
    const jobId = data.jobId || `job-${Date.now().toString(36)}`;
    const { service, args = {} } = data;
    const depth = Number.isFinite(data.depth) ? Number(data.depth) : 0;
    const rootJobId = data.rootJobId ?? jobId;

    // Refuse before doing any work: a chain that is allowed to start is a chain
    // that keeps consuming agents until something else stops it.
    if (depth > cfg.mesh.maxDepth) {
      const err = `delegation depth ${depth} exceeds the limit of ${cfg.mesh.maxDepth}`;
      logger.warn(`rejected job ${jobId} — ${err}`);
      publishResult(jobId, { type: "error", error: err, service }, ownerScope(data.requestedBy));
      jobs.record(
        { jobId, service, state: "rejected", owner: ownerScope(data.requestedBy),
          parentJobId: data.parentJobId, rootJobId, depth, lastEvent: "depth limit" },
        { type: "rejected", note: err },
      );
      return { ok: false, error: err, jobId };
    }

    // ── Owner in the topic (protocol 1.4) ──
    // The topic is what a broker ACL can enforce, so it outranks the payload.
    // Neither is trusted more than the other by default — they simply have to
    // agree, and a disagreement is refused rather than resolved.
    const topicOwner = opts.topicOwner;
    if (topicOwner !== undefined) {
      if (cfg.mesh.ownerInTopic === "off") {
        const err = "this agent does not serve owner-scoped invoke topics (mesh.ownerInTopic is off)";
        logger.info(`rejected job ${jobId} — ${err}`);
        publishResult(jobId, { type: "error", error: err, service }, ownerScope(topicOwner));
        jobs.record({ jobId, service, state: "rejected", lastEvent: "owner-in-topic disabled" },
          { type: "rejected", note: err });
        return { ok: false, error: err, jobId };
      }
      // Already-scoped, or refused. Normalising here would make `Mohanad.Q!`
      // and `mohanad-q` two spellings of one identity, and a broker ACL matches
      // only one of them — so the agent would accept what the broker did not.
      if (ownerScope(topicOwner) !== topicOwner) {
        const err = `invoke topic owner "${topicOwner}" is not owner-scoped — ` +
          `use "${ownerScope(topicOwner)}", which is what an ACL will match`;
        logger.info(`rejected job ${jobId} — ${err}`);
        publishResult(jobId, { type: "error", error: err, service }, ownerScope(topicOwner));
        jobs.record({ jobId, service, state: "rejected", lastEvent: "unscoped topic owner" },
          { type: "rejected", note: err });
        return { ok: false, error: err, jobId };
      }
      const claimed = String(data.requestedBy ?? "").trim();
      if (claimed && ownerScope(claimed) !== topicOwner) {
        const err = `requestedBy "${ownerScope(claimed)}" disagrees with the invoke topic's ` +
          `owner "${topicOwner}"`;
        logger.info(`rejected job ${jobId} — ${err}`);
        // To the TOPIC's owner: that is the identity the broker authorised, and
        // the one whose scope a reply can safely be published to.
        publishResult(jobId, { type: "error", error: err, service }, topicOwner);
        jobs.record({ jobId, service, state: "rejected", owner: topicOwner, lastEvent: "owner mismatch" },
          { type: "rejected", note: err });
        return { ok: false, error: err, jobId };
      }
    }
    // Deliberately no `else`: an invoke that arrives in the v1.3 form is
    // served. Refusing it would be this agent enforcing a policy, and on a
    // broker with rules the old form cannot be published at all — an ACL that
    // grants commands/+/invoke/<owner> does not grant commands/+/invoke.

    // ── Owner resolution (protocol 1.2: requestedBy is REQUIRED) ──
    let requestedBy = topicOwner ?? String(data.requestedBy ?? "").trim();
    if (!requestedBy && opts.defaultOwner) requestedBy = opts.defaultOwner;
    if (!requestedBy && cfg.mesh.requireOwner) {
      const err = "requestedBy is required (protocol 1.2); job rejected";
      logger.warn(`rejected job ${jobId} for service "${service}" — ${err}`);
      // Published where a spec-following client could still find it, so a
      // rejection is never a silent drop.
      publishResult(jobId, { type: "error", error: err, service }, "public");
      jobs.record({ jobId, service, state: "rejected", lastEvent: "missing requestedBy" },
        { type: "rejected", note: err });
      return { ok: false, error: err, jobId };
    }
    if (!requestedBy) {
      logger.warn(`job ${jobId} has no requestedBy — defaulting owner to "public" (mesh.requireOwner is off)`);
    }

    const owner = ownerScope(requestedBy);

    // ── Optional broker-identity verification (opt-in, fails closed) ──
    if (cfg.mesh.verifyOwner) {
      const actual = opts.clientUsername ? ownerScope(opts.clientUsername) : "";
      if (!actual) {
        const err = "owner verification enabled but broker did not supply client_username";
        logger.error(`rejected job ${jobId} — ${err}`);
        publishResult(jobId, { type: "error", error: err, service }, owner);
        jobs.record({ jobId, service, state: "rejected", requestedBy, owner, lastEvent: "unverifiable owner" },
          { type: "rejected", note: err });
        return { ok: false, error: err, jobId };
      }
      if (actual !== owner) {
        const err = `requestedBy "${owner}" does not match broker identity "${actual}"`;
        logger.error(`rejected job ${jobId} — ${err}`);
        publishResult(jobId, { type: "error", error: err, service }, actual);
        jobs.record({ jobId, service, state: "rejected", requestedBy, owner: actual, lastEvent: "owner mismatch" },
          { type: "rejected", note: err });
        return { ok: false, error: err, jobId };
      }
    }

    const cap = catalog.read().capabilities.find((c) => c.service === service);
    if (!cap) {
      const err = `unknown service "${service}"`;
      publishResult(jobId, { type: "error", error: err }, owner);
      jobs.record({ jobId, service, state: "rejected", requestedBy, owner, lastEvent: err },
        { type: "rejected", note: err });
      return { ok: false, error: err, jobId };
    }
    if (jobs.active.has(jobId)) {
      publishResult(jobId, { type: "duplicate", note: "jobId already active" }, owner);
      jobs.record({ jobId, service, state: "duplicate", requestedBy, owner }, { type: "duplicate" });
      return { ok: false, error: "duplicate jobId", jobId };
    }
    // A jobId that already finished is not merely a duplicate: its answer is
    // retained on the broker, and running it again can only end by overwriting
    // that answer with a second one. Refused as an EVENT — publishing the
    // refusal to the result topic would itself destroy what it is protecting.
    if (hasTerminalResult(jobId)) {
      const err =
        `job ${jobId} already finished — its result is retained on the broker. ` +
        `Use a new jobId, or clear the retained result first`;
      publishEvent(jobId, { type: "duplicate", note: err }, owner);
      logger.warn(`refused re-dispatch of ${jobId} (${service}): ${err}`);
      return { ok: false, error: err, jobId };
    }

    // ── Required arguments (declared by the capability, not by us) ──
    // Rejected here rather than warned about later: a job missing one produces
    // a prompt with a hole in it, and the executor spends a real run failing at
    // it. Terminal and immediate is both cheaper and clearer.
    const missing = cap.prompt
      ? missingRequiredArgs(String(cap.prompt), args, (cap.requestSchema ?? {}) as Record<string, unknown>)
      : [];
    if (missing.length) {
      const err =
        `missing required argument${missing.length === 1 ? "" : "s"}: ` +
        `${missing.join(", ")} — declared in ${service}'s requestSchema and not supplied`;
      logger.info(`rejected job ${jobId} — ${err}`);
      publishResult(jobId, { type: "error", error: err, service }, owner);
      jobs.record({ jobId, service, state: "rejected", requestedBy, owner, lastEvent: "missing required args" },
        { type: "rejected", note: err });
      return { ok: false, error: err, jobId };
    }

    jobs.cancelled.delete(jobId);
    jobs.active.add(jobId);
    const at = publishEvent(jobId, { type: "accepted", service, requestedBy, args }, owner);
    jobs.record(
      { jobId, service, state: "accepted", requestedBy, owner,
        parentJobId: data.parentJobId, rootJobId, depth,
        lastEvent: `args: ${JSON.stringify(args).slice(0, 200)}` },
      { type: "accepted", note: service, at },
    );

    const subagentSessionKey = `agent:main:subagent:mesh-${jobId}`;
    const briefing = executorBriefing(owner, jobId, depth);

    // Before the instructions, so what the model reads last is what it is
    // being asked to do rather than what somebody else once wrote.
    const recalled = deps.lessonsFor?.(service) ?? "";

    const instructions = cap.prompt
      ? `${renderPrompt(String(cap.prompt), varOrWarn, jobId, requestedBy, args)}\n\n${briefing}`
      : `Agent-mesh job.\nJobId: ${jobId}\nService: ${service}\n` +
        `Description: ${cap.description ?? ""}\nArgs: ${JSON.stringify(args)}\n` +
        (requestedBy ? `Requested by: ${requestedBy}\n` : "") + `\n${briefing}`;
    const messageText = recalled ? `${recalled}\n\n${instructions}` : instructions;

    // A prompt that rendered with holes in it still runs, and still returns
    // something that reads like an answer. Say so on the job itself: the
    // gateway log keeps only info-level output from plugins, so a warn here
    // would be a warning nobody can ever read.
    if (cap.prompt) {
      const holes = unresolvedPlaceholders(
        String(cap.prompt),
        args,
        (k) => vars.value(k) !== undefined,
        (cap.requestSchema ?? {}) as Record<string, unknown>,
      );
      if (holes.length) {
        const note =
          `rendered empty: ${holes.join(", ")} — the executor was given a prompt ` +
          `with holes in it`;
        logger.info(`job ${jobId}: ${note}`);
        publishEvent(jobId, { type: "prompt_incomplete", note, placeholders: holes }, owner);
      }
    }

    // Declared delegation: gather what the capability says it depends on,
    // BEFORE the executor starts, then hand it the answers. This path needs no
    // tool in the executor's session and does not rely on the model choosing to
    // delegate — the cost is that it cannot adapt to what the job turns out to
    // need.
    const declared = declaredDelegates(cap);
    if (declared.length && deps.performAsk) {
      publishEvent(jobId, {
        type: "delegating",
        note: `gathering ${declared.length} declared dependenc${declared.length === 1 ? "y" : "ies"} before starting`,
      }, owner);
      jobs.record({ jobId, lastEvent: "delegating" },
        { type: "delegating", note: declared.map((d) => `${d.agent}/${d.service}`).join(", ") });

      void resolveDeclared(declared, jobId, owner, args)
        .then((outcome) => {
          if (outcome.ok === false) {
            jobs.active.delete(jobId);
            publishResult(jobId, { type: "error", error: outcome.error }, owner);
            jobs.record({ jobId, state: "error", lastEvent: "required delegation failed" },
              { type: "error", note: outcome.error });
            return;
          }
          launch(`${messageText}\n\n${outcome.context}`);
        })
        .catch((e: any) => {
          jobs.active.delete(jobId);
          publishResult(jobId, { type: "error", error: `delegation failed: ${e.message}` }, owner);
          jobs.record({ jobId, state: "error", lastEvent: "delegation threw" }, { type: "error", note: e.message });
        });
      return { ok: true, jobId };
    }

    return launch(messageText);

    function launch(finalMessage: string): DispatchResult {
    const messageText = finalMessage;
    const watch: WatchEntry = {
      jobId, service, owner, messageText,
      dispatchedAt: Date.now(), lastAgentEventAt: Date.now(),
      reinjections: 0, nudges: 0, runSettled: false, subagentSessionKey,
    };

    try {
      const sub = runtime.subagent;
      if (typeof sub?.run === "function") {
        watched.set(jobId, watch);
        void sub.run({ sessionKey: subagentSessionKey, message: messageText })
          .then((r: { runId: string }) => {
            watch.runId = r.runId;
            logger.info(`job ${jobId} executor subagent started (runId ${r.runId})`);
            // Settlement — not silence — is the watchdog's liveness signal.
            return sub.waitForRun({ runId: r.runId, timeoutMs: 0 }).catch(() => null);
          })
          .then((w: { status?: string; error?: string } | null) => {
            watch.runSettled = true;
            if (w?.status === "error" && jobs.active.has(jobId) && !jobs.cancelled.has(jobId)) {
              logger.error(`executor run for ${jobId} failed: ${w.error ?? "unknown"}`);
              watched.delete(jobId);
              jobs.active.delete(jobId);
              publishResult(jobId, { type: "error", error: `executor run failed: ${w.error ?? "unknown"}` }, owner);
              jobs.record({ jobId, state: "error", lastEvent: "executor run failed" },
                { type: "error", note: w.error ?? "unknown" });
            }
          })
          .catch((e: any) => {
            watch.runSettled = true;
            logger.error(`subagent dispatch for ${jobId} failed: ${e.message}`);
          });
        publishEvent(jobId, { type: "started", note: `push-dispatched to executor subagent ${subagentSessionKey}` }, owner);
        jobs.record({ jobId, service, state: "started", requestedBy, owner, lastEvent: "push dispatch (subagent)" },
          { type: "started", note: "push dispatch" });
        return { ok: true, jobId };
      }

      // Older runtime without a subagent API.
      logger.warn(`subagent runtime unavailable — falling back to system-event dispatch for ${jobId}`);
      runtime.system.enqueueSystemEvent(messageText, { sessionKey: cfg.sessionKey });
      watched.set(jobId, watch);
      publishEvent(jobId, { type: "started", note: "dispatched to agent session (fallback)" }, owner);
      jobs.record({ jobId, service, state: "started", requestedBy, owner },
        { type: "started", note: "fallback dispatch" });
      void wakeWithRetry(`job ${jobId}`);
      return { ok: true, jobId };
    } catch (e: any) {
      jobs.active.delete(jobId);
      watched.delete(jobId);
      publishResult(jobId, { type: "error", error: `inject failed: ${e.message}` }, owner);
      jobs.record({ jobId, service, state: "error", owner }, { type: "error", note: e.message });
      return { ok: false, error: e.message, jobId };
    }
    }
  }

  /** Declared dependencies, honoured only when the mode permits them. */
  function declaredDelegates(cap: Capability): CapabilityDelegate[] {
    const mode = cfg.mesh.delegation;
    if (mode !== "both" && mode !== "declared") return [];
    const list = Array.isArray(cap.delegates) ? cap.delegates : [];
    return list.filter((d) => d && d.agent && d.service && d.as);
  }

  /** Fill {{arg}} placeholders in a delegate's arguments from the parent job. */
  function fillArgs(spec: Record<string, unknown> | undefined, args: Record<string, unknown>) {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(spec ?? {})) {
      out[k] = typeof v === "string"
        ? v.replace(/\{\{(\w+)\}\}/g, (_m, key: string) => String(args?.[key] ?? ""))
        : v;
    }
    return out;
  }

  async function resolveDeclared(
    declared: CapabilityDelegate[],
    jobId: string,
    owner: string,
    args: Record<string, unknown>,
  ): Promise<{ ok: true; context: string } | { ok: false; error: string }> {
    // Run them concurrently: they are independent by construction, and serial
    // would multiply the wait by the number of dependencies.
    const results = await Promise.all(declared.map(async (d) => {
      const outcome = await deps.performAsk!({
        agent: d.agent, service: d.service,
        args: fillArgs(d.args, args), parentJobId: jobId,
      });
      publishEvent(jobId, {
        type: outcome.ok ? "delegated" : "delegation_failed",
        note: `${d.agent}/${d.service}${outcome.ok ? "" : ` — ${outcome.error}`}`,
      }, owner);
      jobs.record({ jobId, lastEvent: outcome.ok ? "delegated" : "delegation_failed" },
        { type: outcome.ok ? "delegated" : "delegation_failed", note: `${d.agent}/${d.service}` });
      return { d, outcome };
    }));

    const fatal = results.find((r) => r.d.required && !r.outcome.ok);
    if (fatal) {
      return { ok: false, error: `required delegation to ${fatal.d.agent}/${fatal.d.service} failed: ${fatal.outcome.error}` };
    }

    // Named blocks, so the prompt can refer to each answer by its `as` name and
    // the executor can tell which agent said what.
    const blocks = results.map(({ d, outcome }) =>
      outcome.ok
        ? `### ${d.as} — answered by ${d.agent} (${d.service}), job ${outcome.jobId}\n${JSON.stringify(outcome.result, null, 2)}`
        : `### ${d.as} — ${d.agent} could not answer\n${outcome.error}`,
    );
    const answered = results.filter((r) => r.outcome.ok).map((r) => r.outcome.jobId);
    return {
      ok: true,
      context:
        `CONTEXT FROM OTHER AGENTS\nThese answers were gathered for you before you started. ` +
        `Use them; do not ask for them again.\n\n${blocks.join("\n\n")}` +
        (answered.length
          ? `\n\nWhen you have used them, say what each was worth: mesh_feedback with jobId ` +
            `${answered.join(" and ")}. A capability nobody judges repeats its mistakes.`
          : ""),
    };
  }

  /**
   * Cooperative cancellation.
   *
   * The runtime exposes run/waitForRun/getSession/deleteSession — there is no
   * abort primitive — so mid-run termination cannot be guaranteed. What the
   * mesh CAN guarantee is the contract clients depend on: a terminal result
   * lands immediately, and no further traffic appears for that job.
   */
  function cancel(jobId: string, requestedBy?: string): boolean {
    const w = watched.get(jobId);
    const wasActive = jobs.active.delete(jobId);
    if (!wasActive && !w) return false;

    const owner = w?.owner ?? ownerScope(requestedBy);
    jobs.cancelled.add(jobId);
    watched.delete(jobId);
    publishEvent(jobId, { type: "cancel_acknowledged" }, owner);
    publishResult(jobId, { type: "cancelled", note: "cancelled by request", requestedBy }, owner);
    jobs.record({ jobId, state: "cancelled", lastEvent: "cancelled", owner, requestedBy },
      { type: "cancelled", note: requestedBy ? `by ${requestedBy}` : undefined });

    // Propagate downward. Each peer cancels ITS children in turn, so one cancel
    // unwinds the whole chain without us needing to know its shape.
    deps.onCancel?.(jobId, requestedBy);

    // Best effort: drop the executor session so it stops consuming budget.
    const sub = runtime.subagent;
    if (w?.subagentSessionKey && typeof sub?.deleteSession === "function") {
      void Promise.resolve(sub.deleteSession({ sessionKey: w.subagentSessionKey, deleteTranscript: false }))
        .then(() => logger.info(`cancelled job ${jobId} — executor session dropped`))
        .catch((e: any) => logger.warn(`cancel ${jobId}: deleteSession failed: ${e.message}`));
    }
    return true;
  }

  /**
   * The watchdog's own guard: stop, rather than publish, when the job it is
   * about to fail has already answered.
   *
   * This is the case that cost a real review. A jobId was re-dispatched over a
   * finished job; the second executor's publish was refused by the guard in
   * payload.ts — correctly — and that refusal made it look silent. The watchdog
   * re-injected twice, then wrote "execution not confirmed" to the result
   * topic, and because results are retained, the review that had been sitting
   * there was gone. A watchdog exists to notice an answer that never arrived;
   * it must never be the thing that removes one.
   */
  function settledElsewhere(w: WatchEntry, why: string): boolean {
    if (!hasTerminalResult(w.jobId)) return false;
    logger.warn(
      `[watchdog] ${why} for job ${w.jobId}, but it already published a terminal result — ` +
      `leaving the answer alone and giving up supervision`,
    );
    publishEvent(w.jobId, {
      type: "watchdog_stood_down",
      note: `${why}, but a terminal result is already retained — not overwriting it`,
    }, w.owner);
    return true;
  }

  function startWatchdog(): () => void {
    const timer = setInterval(() => {
      const now = Date.now();
      for (const w of [...watched.values()]) {
        // Hard wall-clock cap, regardless of liveness.
        if (now - w.dispatchedAt > cfg.mesh.maxJobDurationMs) {
          const mins = Math.round(cfg.mesh.maxJobDurationMs / 60_000);
          watched.delete(w.jobId);
          jobs.active.delete(w.jobId);
          if (settledElsewhere(w, "exceeded max job duration")) continue;
          logger.error(`[watchdog] job ${w.jobId} exceeded max duration ${mins}min — failing`);
          publishEvent(w.jobId, { type: "timeout", note: "exceeded max job duration" }, w.owner);
          publishResult(w.jobId, { type: "error", error: `job exceeded maximum duration of ${mins} minutes` }, w.owner);
          jobs.record({ jobId: w.jobId, state: "timeout", lastEvent: "max duration exceeded" },
            { type: "timeout", note: "max duration" });
          continue;
        }

        // ── The executor said it was done and then published nothing ──
        //
        // This is not silence and it is not a crash, so neither gate below
        // catches it: the run is still open, and the milestone that announced
        // completion counts as activity. It is the commonest way a job goes
        // wrong — the executor delivers to a human, on Slack or a PR comment,
        // and treats that as the job. The requester is left watching a topic
        // nothing will ever arrive on.
        //
        // Ask the SAME session to publish what it already produced. That is
        // not a re-run: no work is repeated, and payload.ts still refuses an
        // overwrite if the result did land while we were asking.
        if (w.completionAnnouncedAt !== undefined
            && !hasTerminalResult(w.jobId)
            && now - w.completionAnnouncedAt >= NUDGE_AFTER_MS
            && w.nudges < MAX_NUDGES) {
          w.nudges++;
          w.completionAnnouncedAt = now;   // next chase is a grace period later
          logger.warn(`[watchdog] job ${w.jobId} announced completion but published no result — asking it to (${w.nudges}/${MAX_NUDGES})`);
          publishEvent(w.jobId, {
            type: "result_pending",
            note: `completion announced with no result published — asking the executor to publish (${w.nudges}/${MAX_NUDGES})`,
          }, w.owner);
          const sub = runtime.subagent;
          if (typeof sub?.run === "function") {
            void Promise.resolve(sub.run({
              sessionKey: w.subagentSessionKey,
              message:
                `⏳ Job ${w.jobId}: you reported this finished, but nothing has been published to ` +
                `${jobResultTopic(cfg.mesh.root, w.owner, w.jobId)} and the requester is still waiting there. ` +
                `Publish the terminal payload now, with a "type" field. Do NOT redo the work — publish what ` +
                `you already produced. Telling someone on Slack does not deliver the job.`,
            })).catch((e: any) => logger.error(`[watchdog] nudge for ${w.jobId} failed: ${e.message}`));
          }
          continue;
        }

        if (w.runId && !w.runSettled) continue;   // run in flight → alive by definition

        // Settlement is NOT evidence that the executor is finished, in either
        // direction. It has arrived seventeen minutes after the last thing the
        // executor did, and it has arrived nineteen seconds into a job that
        // went on to publish a result eighty seconds later. A run ending is
        // one turn ending, not the work ending.
        //
        // So silence, and only silence, decides. It is the one signal that
        // means what it says: nothing has come from this executor for long
        // enough that something is actually wrong. Anything shorter re-runs
        // jobs that were working, and re-running twice inside two minutes
        // exhausts the retries and publishes "execution not confirmed" over a
        // job that was about to answer.
        if (now - w.lastAgentEventAt < REINJECT_AFTER_MS) continue;

        if (w.reinjections >= MAX_REINJECTS) {
          watched.delete(w.jobId);
          jobs.active.delete(w.jobId);
          if (settledElsewhere(w, `no result after ${MAX_REINJECTS} re-injections`)) continue;
          logger.error(`[watchdog] job ${w.jobId} failed after ${MAX_REINJECTS} re-injections — publishing timeout`);
          publishEvent(w.jobId, { type: "timeout", note: "executor never produced a result" }, w.owner);
          publishResult(w.jobId, { type: "error", error: `execution not confirmed after ${MAX_REINJECTS} re-injections` }, w.owner);
          jobs.record({ jobId: w.jobId, state: "timeout", lastEvent: "watchdog timeout" },
            { type: "timeout", note: "no result after retries" });
          continue;
        }

        const why = w.runId ? "run settled without publishing a result" : "no executor activity";
        w.reinjections++;
        w.dispatchedAt = now;
        w.lastAgentEventAt = now;
        w.runSettled = false;
        w.runId = undefined;
        w.completionAnnouncedAt = undefined;   // a fresh run makes its own claims
        logger.warn(`[watchdog] job ${w.jobId} (service ${w.service}) — ${why} — re-injecting (${w.reinjections}/${MAX_REINJECTS})`);
        publishEvent(w.jobId, {
          type: "requeued",
          note: `${why} — re-dispatching executor (attempt ${w.reinjections}/${MAX_REINJECTS})`,
        }, w.owner);
        jobs.record({ jobId: w.jobId, state: "started", lastEvent: `watchdog re-inject #${w.reinjections}` },
          { type: "requeued", note: why });

        try {
          const sub = runtime.subagent;
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
              .catch((e: any) => { w.runSettled = true; logger.error(`[watchdog] re-dispatch failed for ${w.jobId}: ${e.message}`); });
          } else {
            runtime.system.enqueueSystemEvent(
              `🔁 Watchdog re-inject for agent-mesh job ${w.jobId} (service ${w.service}, owner ${w.owner}) — original dispatch was not confirmed as executed.\n\n${w.messageText}`,
              { sessionKey: cfg.sessionKey },
            );
            void wakeWithRetry(`watchdog ${w.jobId}`);
          }
        } catch (e: any) {
          logger.error(`[watchdog] re-inject failed for ${w.jobId}: ${e.message}`);
        }
      }
    }, WATCHDOG_INTERVAL_MS);
    timer.unref?.();
    return () => clearInterval(timer);
  }

  const lineageOf = (jobId?: string): { rootJobId?: string; depth: number } => {
    if (!jobId) return { depth: 0 };
    const rec = jobs.find(jobId);
    return { rootJobId: rec?.rootJobId ?? jobId, depth: rec?.depth ?? 0 };
  };

  return {
    dispatch, cancel, markAgentActivity, publishEvent, publishResult, startWatchdog, lineageOf,
    isWatched: (jobId) => watched.has(jobId),
    forget: (jobId) => { watched.delete(jobId); },
  };
}
