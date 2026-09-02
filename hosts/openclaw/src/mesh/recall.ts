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
 * question still goes out and nothing waits for it. Otherwise every job on such
 * a mesh pays the full timeout forever for an answer that cannot come, which is
 * this feature's entire cost charged for none of its benefit. An answer arriving
 * at any point says a recorder is there and puts the wait back.
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
  /** Whether this mesh has stopped being waited on. */
  readonly quiet: boolean;
}

/**
 * Unanswered asks before the wait is dropped.
 *
 * A mesh with no recorder answers nothing, ever, and waiting the full timeout
 * before every job is the whole cost of this feature charged for nothing. Three
 * is enough to tell "no recorder here" from a box that was restarting.
 */
const QUIET_AFTER = 3;

export function createRecall(deps: RecallDeps): Recall {
  const pending = new Map<string, { resolve(v: string): void; timer: NodeJS.Timeout }>();
  let silences = 0;

  return {
    get waiting() { return pending.size; },
    get quiet() { return silences >= QUIET_AFTER; },

    of(service) {
      if (!service) return Promise.resolve("");

      // Asked but not awaited. The question still goes out, so a recorder that
      // appears later is heard — and the answer it sends re-arms the wait for
      // the job after this one.
      if (silences >= QUIET_AFTER) {
        deps.publish(deps.askTopic(service), JSON.stringify({ agentId: deps.agentId, service }), { qos: 1 });
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
              ? `[memory] nothing has answered on this mesh — still asking, no longer waiting`
              : `[memory] no answer for ${service} in ${deps.timeoutMs}ms — running without it`);
          p.resolve("");
        }, deps.timeoutMs);
        timer.unref?.();
        pending.set(service, { resolve, timer });

        deps.publish(deps.askTopic(service), JSON.stringify({ agentId: deps.agentId, service }), { qos: 1 });
      });
    },

    settle(service, rendered) {
      // Something answers here. That is true even when nobody was waiting —
      // which is exactly the case while quiet, and is how waiting resumes.
      silences = 0;
      const p = pending.get(service);
      if (!p) return false;
      clearTimeout(p.timer);
      pending.delete(service);
      p.resolve(rendered);
      return true;
    },
  };
}
