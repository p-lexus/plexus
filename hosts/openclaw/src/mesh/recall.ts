/**
 * Asking what past runs of a capability reported, at the moment a command for
 * it arrives.
 *
 * Asked rather than held. An agent that subscribed to every capability's memory
 * would carry lessons for capabilities it does not serve, and a snapshot from
 * whenever it last connected; asking answers with what is true now, for the one
 * capability about to run.
 *
 * The cost is a round trip before the executor starts, and the whole design of
 * this module is about that cost never becoming a job's problem: the wait is
 * short, it is bounded, and running without lessons is a normal outcome rather
 * than a failure.
 *
 * A mesh with no recorder answers nothing, ever — so after a few silences the
 * question goes out only occasionally and nothing waits for it. Otherwise every
 * job on such a mesh pays the full timeout forever for an answer that cannot
 * come, which is this feature's entire cost charged for none of its benefit.
 *
 * That question is also how the agent knows where it is. Memory, postmortems
 * and verdicts are the box's half of the protocol: on a bare broker they are
 * published into topics nothing reads and the broker most likely refuses, which
 * costs an executor run per failure and hides the refusal, because a refused
 * publish is acknowledged at QoS 1. So `heard` — has anything ever answered —
 * gates the lot, and it is a fact the mesh states rather than a setting anyone
 * configures.
 */

import type { Logger } from "../types.js";

export interface RecallDeps {
  meshRoot: string;
  agentId: string;
  /** How long a job waits for its lessons before starting without them. */
  timeoutMs: number;
  logger: Logger;
  publish(topic: string, payload: string, opts?: { qos?: 0 | 1 | 2; retain?: boolean }): void;
  askTopic(service: string): string;
  /** Injected so the re-probe interval is testable without waiting five minutes. */
  now?(): number;
}

export interface Recall {
  /** What past runs reported, or "" — never rejects, never waits long. */
  of(service: string): Promise<string>;
  /** Deliver an answer that arrived. Returns whether anybody was waiting. */
  settle(service: string, rendered: string): boolean;
  readonly waiting: number;
  /** Whether this mesh has stopped being waited on. */
  readonly quiet: boolean;
  /**
   * Whether anything on this mesh has ever answered — the one question that
   * distinguishes a mesh with a recorder from a bare broker.
   *
   * Everything in the feedback cycle is gated on it, so it is deliberately a
   * fact rather than a setting: an operator cannot turn the cycle on where
   * nothing records, and does not have to turn it on where something does.
   */
  readonly heard: boolean;
}

/**
 * Unanswered asks before the wait is dropped.
 *
 * A mesh with no recorder answers nothing, ever, and waiting the full timeout
 * before every job is the whole cost of this feature charged for nothing. Three
 * is enough to tell "no recorder here" from a box that was restarting.
 */
const QUIET_AFTER = 3;

/**
 * How often a quiet mesh is asked again.
 *
 * Once quiet, asking per job is noise on a broker that refuses the topic
 * anyway. Asking never would mean a box added later is never noticed, and the
 * cycle would stay off on a mesh that has one — so the question goes out
 * occasionally, and the first answer turns everything back on.
 */
const REPROBE_MS = 5 * 60_000;

export function createRecall(deps: RecallDeps): Recall {
  const pending = new Map<string, { resolve(v: string): void; timer: NodeJS.Timeout }>();
  const now = () => (deps.now ? deps.now() : Date.now());
  let silences = 0;
  let heard = false;
  let lastAsk = 0;

  const ask = (service: string) => {
    lastAsk = now();
    deps.publish(deps.askTopic(service), JSON.stringify({ agentId: deps.agentId, service }), { qos: 1 });
  };

  return {
    get waiting() { return pending.size; },
    get quiet() { return silences >= QUIET_AFTER; },
    get heard() { return heard; },

    of(service) {
      if (!service) return Promise.resolve("");

      // Asked occasionally, awaited never. The question is what finds a box
      // that appeared after this agent started; its answer is what turns the
      // feedback cycle back on.
      if (silences >= QUIET_AFTER) {
        if (now() - lastAsk >= REPROBE_MS) ask(service);
        return Promise.resolve("");
      }

      // One question per capability at a time. Two jobs for the same capability
      // arriving together are one question, and the second would otherwise
      // replace the first's resolver and leave that job waiting for its whole
      // timeout on an answer already delivered.
      const already = pending.get(service);
      if (already) {
        return new Promise<string>((resolve) => {
          const first = already.resolve;
          already.resolve = (v) => { first(v); resolve(v); };
        });
      }

      return new Promise<string>((resolve) => {
        const timer = setTimeout(() => {
          const p = pending.get(service);
          if (!p) return;
          pending.delete(service);
          silences++;
          // Info, not warn: a mesh with no recorder is a supported deployment,
          // and a warning per job would be a warning about a choice somebody
          // made rather than about anything going wrong.
          deps.logger.info(
            silences >= QUIET_AFTER
              ? `[memory] nothing answers here — no box on this mesh, so memory, postmortems and verdicts are off`
              : `[memory] no answer for ${service} in ${deps.timeoutMs}ms — running without it`);
          p.resolve("");
        }, deps.timeoutMs);
        timer.unref?.();
        pending.set(service, { resolve, timer });

        ask(service);
      });
    },

    settle(service, rendered) {
      // Something answers here. That is true even when nobody was waiting —
      // which is exactly the case while quiet, and is how the cycle resumes.
      silences = 0;
      heard = true;
      const p = pending.get(service);
      if (!p) return false;
      clearTimeout(p.timer);
      pending.delete(service);
      p.resolve(rendered);
      return true;
    },
  };
}
