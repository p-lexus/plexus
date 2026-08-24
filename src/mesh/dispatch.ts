/**
 * Job dispatch, cancellation, and the supervisory watchdog.
 *
 * Dispatch is push: the executor runs as a subagent turn started the moment
 * the job arrives, so a busy or failing main session can never swallow it.
 * Older runtimes without a subagent API fall back to a system-event queue plus
 * a heartbeat wake — the only pull-shaped path left in the bridge.
 */

import type {
  DispatchOptions, DispatchRequest, DispatchResult, Logger, WatchEntry,
} from "../types.js";
import type { ResolvedConfig } from "../config.js";
import type { Catalog } from "./catalog.js";
import type { JobStore } from "./jobs.js";
import type { VarStore } from "./vars.js";
import { jobEventsTopic, jobResultTopic, ownerScope } from "./topics.js";
import { renderPrompt } from "./payload.js";

const WATCHDOG_INTERVAL_MS = 60_000;   // supervisory sweep, not a delivery path
const REINJECT_AFTER_MS = 5 * 60_000;  // silence required before re-dispatch
const MAX_REINJECTS = 2;               // then fail loudly rather than retry forever

export interface DispatcherDeps {
  cfg: ResolvedConfig;
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
  /** Any executor publish proves the executor is alive. */
  markAgentActivity(jobId: string): void;
  publishEvent(jobId: string, event: Record<string, unknown>, owner: string): void;
  publishResult(jobId: string, result: Record<string, unknown>, owner: string): void;
  startWatchdog(): () => void;
  isWatched(jobId: string): boolean;
  forget(jobId: string): void;
}

export function createDispatcher(deps: DispatcherDeps): Dispatcher {
  const { cfg, logger, catalog, jobs, vars, runtime, publish } = deps;
  const root = cfg.mesh.root;
  const watched = new Map<string, WatchEntry>();

  const publishEvent = (jobId: string, event: Record<string, unknown>, owner: string) =>
    publish(
      jobEventsTopic(root, owner, jobId),
      JSON.stringify({ ...event, jobId, owner, ts: new Date().toISOString() }),
      { qos: 1 },
    );

  /** Terminal result: owner-scoped, retained, QoS 1. */
  const publishResult = (jobId: string, result: Record<string, unknown>, owner: string) =>
    publish(
      jobResultTopic(root, owner, jobId),
      JSON.stringify({ ...result, jobId, owner, ts: new Date().toISOString() }),
      { qos: 1, retain: true },
    );

  const markAgentActivity = (jobId: string) => {
    const w = watched.get(jobId);
    if (w) w.lastAgentEventAt = Date.now();
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

  function executorBriefing(owner: string, jobId: string): string {
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
      `but set "type" yourself.\n` +
      `SCOPE: owner is "${owner}" — publish only to the owner-scoped topics above.`
    );
  }

  function dispatch(data: DispatchRequest, opts: DispatchOptions = {}): DispatchResult {
    const jobId = data.jobId || `job-${Date.now().toString(36)}`;
    const { service, args = {} } = data;

    // ── Owner resolution (protocol 1.2: requestedBy is REQUIRED) ──
    let requestedBy = String(data.requestedBy ?? "").trim();
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

    jobs.cancelled.delete(jobId);
    jobs.active.add(jobId);
    publishEvent(jobId, { type: "accepted", service, requestedBy, args }, owner);
    jobs.record(
      { jobId, service, state: "accepted", requestedBy, owner, lastEvent: `args: ${JSON.stringify(args).slice(0, 200)}` },
      { type: "accepted", note: service },
    );

    const subagentSessionKey = `agent:main:subagent:mesh-${jobId}`;
    const briefing = executorBriefing(owner, jobId);

    const messageText = cap.prompt
      ? `${renderPrompt(String(cap.prompt), varOrWarn, jobId, requestedBy, args)}\n\n${briefing}`
      : `Agent-mesh job.\nJobId: ${jobId}\nService: ${service}\n` +
        `Description: ${cap.description ?? ""}\nArgs: ${JSON.stringify(args)}\n` +
        (requestedBy ? `Requested by: ${requestedBy}\n` : "") + `\n${briefing}`;

    const watch: WatchEntry = {
      jobId, service, owner, messageText,
      dispatchedAt: Date.now(), lastAgentEventAt: Date.now(),
      reinjections: 0, runSettled: false, subagentSessionKey,
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

    // Best effort: drop the executor session so it stops consuming budget.
    const sub = runtime.subagent;
    if (w?.subagentSessionKey && typeof sub?.deleteSession === "function") {
      void Promise.resolve(sub.deleteSession({ sessionKey: w.subagentSessionKey, deleteTranscript: false }))
        .then(() => logger.info(`cancelled job ${jobId} — executor session dropped`))
        .catch((e: any) => logger.warn(`cancel ${jobId}: deleteSession failed: ${e.message}`));
    }
    return true;
  }

  function startWatchdog(): () => void {
    const timer = setInterval(() => {
      const now = Date.now();
      for (const w of [...watched.values()]) {
        // Hard wall-clock cap, regardless of liveness.
        if (now - w.dispatchedAt > cfg.mesh.maxJobDurationMs) {
          const mins = Math.round(cfg.mesh.maxJobDurationMs / 60_000);
          logger.error(`[watchdog] job ${w.jobId} exceeded max duration ${mins}min — failing`);
          watched.delete(w.jobId);
          jobs.active.delete(w.jobId);
          publishEvent(w.jobId, { type: "timeout", note: "exceeded max job duration" }, w.owner);
          publishResult(w.jobId, { type: "error", error: `job exceeded maximum duration of ${mins} minutes` }, w.owner);
          jobs.record({ jobId: w.jobId, state: "timeout", lastEvent: "max duration exceeded" },
            { type: "timeout", note: "max duration" });
          continue;
        }

        if (w.runId && !w.runSettled) continue;   // run in flight → alive by definition

        // Settlement alone is NOT grounds to re-dispatch: a terminal publish
        // may still be in flight. Require genuine silence too, so a
        // misbehaving prompt cannot cause duplicate execution on its own.
        if (now - w.lastAgentEventAt < REINJECT_AFTER_MS) continue;

        if (w.reinjections >= MAX_REINJECTS) {
          logger.error(`[watchdog] job ${w.jobId} failed after ${MAX_REINJECTS} re-injections — publishing timeout`);
          watched.delete(w.jobId);
          jobs.active.delete(w.jobId);
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

  return {
    dispatch, cancel, markAgentActivity, publishEvent, publishResult, startWatchdog,
    isWatched: (jobId) => watched.has(jobId),
    forget: (jobId) => { watched.delete(jobId); },
  };
}
