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
 * than a failure. A mesh with no recorder answers nothing, and every job on it
 * proceeds exactly as it did before this existed.
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
}

export function createRecall(deps: RecallDeps): Recall {
  const pending = new Map<string, { resolve(v: string): void; timer: NodeJS.Timeout }>();

  return {
    get waiting() { return pending.size; },

    of(service) {
      if (!service) return Promise.resolve("");

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
          // Info, not warn: a mesh with no recorder is a supported deployment,
          // and a warning per job would be a warning about a choice somebody
          // made rather than about anything going wrong.
          deps.logger.info(`[memory] no answer for ${service} in ${deps.timeoutMs}ms — running without it`);
          p.resolve("");
        }, deps.timeoutMs);
        timer.unref?.();
        pending.set(service, { resolve, timer });

        deps.publish(deps.askTopic(service), JSON.stringify({ agentId: deps.agentId, service }), { qos: 1 });
      });
    },

    settle(service, rendered) {
      const p = pending.get(service);
      if (!p) return false;
      clearTimeout(p.timer);
      pending.delete(service);
      p.resolve(rendered);
      return true;
    },
  };
}
