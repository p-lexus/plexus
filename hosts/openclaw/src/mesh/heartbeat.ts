/**
 * A liveness claim that goes stale on its own.
 *
 * `<root>/registry/<id>/status` was published once on connect and left there.
 * Retained, it outlives the agent: a console read it hours after the agent had
 * moved to another broker and reported it connected, because nothing about the
 * message said when it was true.
 *
 * The same topic is republished on a timer now, and the payload says how often
 * — so a reader expires it instead of guessing an interval, and an agent that
 * publishes no interval cannot be claimed live at all.
 *
 * Reusing `status` rather than adding a topic is deliberate: it is already
 * retained, already subscribed by every peer, and already has a will behind it
 * for the ungraceful case. A second topic would be a second piece of state to
 * disagree with the first.
 *
 * This is protocol, not product. It needs no box: peers read it, and a mesh
 * with nothing watching is unaffected.
 */
export const DEFAULT_HEARTBEAT_SECONDS = 30;

/** Bounds, because an interval is advertised and therefore trusted by readers. */
export const MIN_HEARTBEAT_SECONDS = 5;
export const MAX_HEARTBEAT_SECONDS = 600;

export interface Beat {
  status: "online";
  timestamp: string;
  /** Seconds until the next one. A reader expires this claim at a multiple. */
  every: number;
}

export function heartbeatSeconds(configured?: number): number {
  if (typeof configured !== "number" || !Number.isFinite(configured)) {
    return DEFAULT_HEARTBEAT_SECONDS;
  }
  return Math.min(MAX_HEARTBEAT_SECONDS, Math.max(MIN_HEARTBEAT_SECONDS, Math.round(configured)));
}

export function beat(every: number, now: () => Date = () => new Date()): Beat {
  return { status: "online", timestamp: now().toISOString(), every };
}

/**
 * How long a reader should believe one beat.
 *
 * Two and a half intervals: one missed beat is a lost packet or a slow tick and
 * says nothing, two in a row is a pattern. Tighter than that turns every
 * garbage-collection pause into a disconnection; looser and a dead agent is
 * reported live for minutes, which is the failure this exists to end.
 */
export const STALE_AFTER = 2.5;

export function staleAfterMs(every: number): number {
  return Math.round(heartbeatSeconds(every) * STALE_AFTER * 1000);
}

/**
 * Whether a status message still means what it says.
 *
 * An `every` that is absent is the pre-v1.8 shape: the claim cannot be expired,
 * so it is not evidence of a live agent. Saying so is the whole point — a
 * reader that assumed live there is exactly what reported an agent connected
 * three hours after it left.
 */
export function isLive(msg: unknown, at: number): { live: boolean; why: string } {
  const m = msg as Partial<Beat> | null;
  if (!m || typeof m !== "object") return { live: false, why: "no status" };
  if (m.status !== "online") return { live: false, why: String(m.status ?? "unknown") };
  if (typeof m.every !== "number" || !Number.isFinite(m.every)) {
    return { live: false, why: "no heartbeat — this agent predates v1.8, so its claim cannot expire" };
  }
  const at0 = Date.parse(String(m.timestamp ?? ""));
  if (!Number.isFinite(at0)) return { live: false, why: "no timestamp" };
  const age = at - at0;
  if (age > staleAfterMs(m.every)) {
    return { live: false, why: `last heartbeat ${Math.round(age / 1000)}s ago` };
  }
  return { live: true, why: "" };
}
