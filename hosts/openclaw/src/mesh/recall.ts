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
 * Asked only where something answers. Memory, postmortems and verdicts are the
 * box's half of the protocol, and a mesh says whether it has one on <root>/box —
 * retained, so the answer is here before the first job. Nothing is published
 * until it says yes.
 *
 * That replaced guessing. Presence used to be inferred from silence: ask, wait
 * out the timeout, conclude after three of them. It worked and it cost a bare
 * broker a publish it refuses without saying so, every job the wait, and the
 * agent three jobs of not knowing where it was.
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
}

export interface Recall {
  /** What past runs reported, or "" — never rejects, never waits long. */
  of(service: string): Promise<string>;
  /** Deliver an answer that arrived. Returns whether anybody was waiting. */
  settle(service: string, rendered: string): boolean;
  readonly waiting: number;
  /**
   * Whether this mesh has a box — from what the box says, not from silence.
   *
   * Everything in the feedback cycle is gated on it, so it is deliberately a
   * fact rather than a setting: an operator cannot turn the cycle on where
   * nothing records, and does not have to turn it on where something does.
   */
  readonly heard: boolean;

  /**
   * A box announced itself, or withdrew.
   *
   * Retained, so this arrives on connect before the first job — and its will
   * clears it, so a box that dies takes the cycle down with it rather than
   * leaving agents publishing into a topic nobody reads.
   */
  present(there: boolean): void;
}

export function createRecall(deps: RecallDeps): Recall {
  const pending = new Map<string, { resolve(v: string): void; timer: NodeJS.Timeout }>();
  let heard = false;

  const ask = (service: string) =>
    deps.publish(deps.askTopic(service), JSON.stringify({ agentId: deps.agentId, service }), { qos: 1 });

  return {
    get waiting() { return pending.size; },
    get heard() { return heard; },

    present(there) {
      heard = there;
    },

    of(service) {
      // Not asked where nothing answers. On a bare broker this is the whole of
      // the feature's cost — a publish the broker refuses without saying so,
      // and a wait before every job — bought for nothing.
      if (!service || !heard) return Promise.resolve("");

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
          // Info, not warn. The box said it was here, so this is a slow or
          // restarting recorder rather than a mesh without one, and the job
          // proceeds either way.
          deps.logger.info(`[memory] no answer for ${service} in ${deps.timeoutMs}ms — running without it`);
          p.resolve("");
        }, deps.timeoutMs);
        timer.unref?.();
        pending.set(service, { resolve, timer });

        ask(service);
      });
    },

    settle(service, rendered) {
      // Something answered, so something is there — true even if the
      // announcement has not arrived yet, and harmless when it has.
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
