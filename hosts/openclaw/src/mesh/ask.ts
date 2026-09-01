/**
 * Delegation: asking a peer to do work, and getting the answer back.
 *
 * Publishing to a peer's invoke topic was always possible. What was missing is
 * the return path: an executor runs as a single session that ends when it
 * returns, so it could *notify* a peer but never *ask* one — the answer arrived
 * after the asker had already finished, and nothing routed it back.
 *
 * This closes the loop. An ask registers a pending promise keyed on the job id
 * it created; the bridge already subscribes to every job topic on the mesh, so
 * the peer's terminal result resolves it.
 *
 * Every ask is its own job with its own id. A chain of agents is a chain of
 * jobs, linked by parentJobId/rootJobId rather than one job passed around.
 */

import type { Logger, Peer, Verdict } from "../types.js";
import { peerInvokeTopic, peerInvokeTopicFor, peerCancelTopic, ownerScope } from "./topics.js";

export interface AskRequest {
  agent: string;
  service: string;
  args?: Record<string, unknown>;
  /** The job on whose behalf we are asking — the parent in the chain. */
  parentJobId?: string;
}

export interface AskOutcome {
  ok: boolean;
  jobId: string;
  agent: string;
  result?: any;
  error?: string;
}

interface Pending {
  jobId: string;
  agent: string;
  parentJobId?: string;
  resolve(outcome: AskOutcome): void;
  timer: NodeJS.Timeout;
}

export interface AskDeps {
  /**
   * What a peer says it does with owner-scoped invoke topics, from its retained
   * profile: "off", "accept" or "require". Undefined for a peer we have never
   * seen a profile from.
   */
  peerOwnerTopicMode?(agentId: string): string | undefined;
  selfAgentId: string;
  meshRoot: string;
  maxDepth: number;
  timeoutMs: number;
  logger: Logger;
  publish(topic: string, payload: string, opts?: { qos?: 0 | 1 | 2; retain?: boolean }): void;
  peer(agentId: string): Peer | undefined;
  /** Lineage of the parent job, so the chain continues correctly. */
  lineageOf(jobId?: string): { rootJobId?: string; depth: number };
  onDelegated(info: { jobId: string; agent: string; service: string; parentJobId?: string; rootJobId?: string; depth: number }): void;
  fileVerdict?(agent: string, jobId: string, verdict: Verdict, reason: string): void;
}

export interface AskService {
  ask(req: AskRequest): Promise<AskOutcome>;
  /** Resolve a pending ask from a terminal result seen on the mesh. */
  settle(jobId: string, result: any): boolean;
  /** Jobs this job delegated, for cancel propagation. */
  childrenOf(parentJobId: string): Array<{ jobId: string; agent: string }>;
  /** Cancel everything a job delegated, recursively via each peer. */
  cancelChildren(parentJobId: string, requestedBy: string): number;
  readonly pendingCount: number;
}

export function createAskService(deps: AskDeps): AskService {
  const pending = new Map<string, Pending>();

  const finish = (p: Pending, outcome: AskOutcome) => {
    clearTimeout(p.timer);
    pending.delete(p.jobId);

    // Failure is the only verdict a requester can give without an opinion: an
    // answer that merely arrived is not thereby a good one.
    if (!outcome.ok) {
      deps.fileVerdict?.(p.agent, p.jobId, "unusable",
        outcome.error ?? "the delegation ended without an answer");
    }

    p.resolve(outcome);
  };

  return {
    get pendingCount() { return pending.size; },

    childrenOf(parentJobId) {
      return [...pending.values()]
        .filter((p) => p.parentJobId === parentJobId)
        .map((p) => ({ jobId: p.jobId, agent: p.agent }));
    },

    ask(req) {
      const agent = String(req.agent ?? "").trim();
      const service = String(req.service ?? "").trim();
      const { rootJobId, depth } = deps.lineageOf(req.parentJobId);
      const nextDepth = depth + 1;

      const fail = (error: string): Promise<AskOutcome> =>
        Promise.resolve({ ok: false, jobId: "", agent, error });

      if (!agent || !service) return fail("both agent and service are required");
      if (agent === deps.selfAgentId) {
        return fail(`cannot ask yourself — ${agent} is this agent. Use a peer, or handle it directly.`);
      }

      // Bound the chain before publishing: a cycle that is allowed to start is
      // a cycle that runs until something else stops it.
      if (nextDepth > deps.maxDepth) {
        const err = `delegation depth limit reached (${deps.maxDepth}). This request has already passed through ${depth} agent(s); refusing to go deeper.`;
        deps.logger.warn(`ask refused: ${err}`);
        return fail(err);
      }

      const known = deps.peer(agent);
      if (!known) {
        return fail(`unknown agent "${agent}" — it has not published a profile to this mesh. Check the peer list.`);
      }
      if (!known.capabilities.some((c) => c.service === service)) {
        const offers = known.capabilities.map((c) => c.service).join(", ") || "nothing";
        return fail(`agent "${agent}" does not offer "${service}". It offers: ${offers}.`);
      }
      if (!known.online) {
        deps.logger.warn(`asking ${agent} while it appears offline — the broker will queue the job`);
      }

      const jobId = `ask-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
      const payload = {
        service,
        args: req.args ?? {},
        // We are the requester, so the peer's results route back to our scope
        // and arrive on the jobs/# subscription we already hold.
        requestedBy: deps.selfAgentId,
        jobId,
        parentJobId: req.parentJobId,
        rootJobId: rootJobId ?? req.parentJobId ?? jobId,
        depth: nextDepth,
      };

      // v1.4: publish where the broker can check who we say we are, but only to
      // a peer that has said it serves that form. A peer whose profile we have
      // not seen is not evidence of anything — the old form is what every
      // version understands, so an unknown peer gets that.
      const peerTopic = String(deps.peerOwnerTopicMode?.(agent) ?? "off");
      const selfOwner = ownerScope(deps.selfAgentId);
      const invokeTopic = peerTopic === "accept" || peerTopic === "require"
        ? peerInvokeTopicFor(deps.meshRoot, agent, selfOwner)
        : peerInvokeTopic(deps.meshRoot, agent);

      deps.publish(invokeTopic, JSON.stringify(payload), { qos: 1 });
      deps.onDelegated({
        jobId, agent, service,
        parentJobId: req.parentJobId,
        rootJobId: payload.rootJobId,
        depth: nextDepth,
      });
      deps.logger.info(`asked ${agent} for ${service} (job ${jobId}, depth ${nextDepth})`);

      return new Promise<AskOutcome>((resolve) => {
        const timer = setTimeout(() => {
          const p = pending.get(jobId);
          if (!p) return;
          deps.logger.warn(`ask ${jobId} to ${agent} timed out after ${Math.round(deps.timeoutMs / 60_000)}min`);
          finish(p, {
            ok: false, jobId, agent,
            error: `${agent} did not answer within ${Math.round(deps.timeoutMs / 60_000)} minutes. It may be offline or overloaded.`,
          });
        }, deps.timeoutMs);
        timer.unref?.();
        pending.set(jobId, { jobId, agent, parentJobId: req.parentJobId, resolve, timer });
      });
    },

    settle(jobId, result) {
      const p = pending.get(jobId);
      if (!p) return false;
      const errored = result?.type === "error" || result?.type === "cancelled";
      finish(p, {
        ok: !errored,
        jobId,
        agent: p.agent,
        ...(errored ? { error: String(result?.error ?? result?.note ?? result?.type) } : { result }),
      });
      return true;
    },

    cancelChildren(parentJobId, requestedBy) {
      const kids = [...pending.values()].filter((p) => p.parentJobId === parentJobId);
      for (const kid of kids) {
        // Tell the peer to stop. It will cancel ITS children in turn, so a
        // cancel travels the whole chain without us knowing its shape.
        deps.publish(
          peerCancelTopic(deps.meshRoot, kid.agent),
          JSON.stringify({ jobId: kid.jobId, requestedBy: ownerScope(requestedBy) }),
          { qos: 1 },
        );
        finish(kid, {
          ok: false, jobId: kid.jobId, agent: kid.agent,
          error: "cancelled because the job that asked for it was cancelled",
        });
      }
      if (kids.length) deps.logger.info(`cancelled ${kids.length} delegated job(s) under ${parentJobId}`);
      return kids.length;
    },
  };
}
