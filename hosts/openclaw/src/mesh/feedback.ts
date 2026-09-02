/**
 * Whether a verdict is one this agent may record, and what it says.
 *
 * A job ends and, until v1.5, nothing came back. The requester used the result
 * or binned it, and the agent never learned which — so a bad review looked
 * exactly like a good one on the wire, and the same capability repeated the
 * same mistake for as long as anyone kept asking.
 *
 * Pure: a topic owner, a payload and the job it claims to be about go in, a
 * decision comes out. No I/O, so the rules are directly testable and the
 * refusal path is not something that only happens against a live broker.
 */

import { feedbackFileTopic, ownerScope } from "./topics.js";
import type { Feedback, JobRecord, Verdict } from "../types.js";

const VERDICTS: ReadonlySet<string> = new Set<Verdict>(["good", "bad", "unusable"]);

/** Long enough for a paragraph of why, short enough not to be a payload. */
export const MAX_REASON = 500;

/** What happened, in the requester's words. Longer, because it is evidence. */
export const MAX_DETAILS = 2000;

/**
 * What a later run should do — the field the whole cycle exists to carry.
 *
 * Written for good work as well as bad: a capability that got it right is worth
 * saying so about specifically, because "keep checking the rate limit" is a
 * lesson and "nice one" is not.
 */
export const MAX_LESSON = 500;

/**
 * Two fields rather than a discriminated union, because this project compiles
 * with `strict: false` and TypeScript will not narrow one there — the caller
 * would have to assert its way through the branch it just tested.
 */
export interface FeedbackDecision {
  /** The verdict to record, or null when it was refused. */
  feedback: Feedback | null;
  /** Why it was refused, in words for the requester. Null when accepted. */
  reason: string | null;
}

const refuse = (reason: string): FeedbackDecision => ({ feedback: null, reason });

/**
 * Read a verdict off the wire.
 *
 * `topicOwner` is the segment the broker matched, passed in exactly as it
 * arrived. It is the authority on who is speaking — the payload's opinion of
 * that is not consulted, because a payload field is whatever the sender typed.
 *
 * A disagreement between the topic and the job is refused rather than
 * reconciled, which is the same rule v1.4 applies to invokes: the mesh does
 * not quietly decide which of two identities somebody meant.
 */
export function readFeedback(
  topicOwner: string,
  data: any,
  job: JobRecord | undefined,
  now: number,
): FeedbackDecision {
  const jobId = String(data?.jobId ?? "").trim();
  if (!jobId) return refuse("a verdict must name the job it is about");

  const verdict = String(data?.verdict ?? "").trim().toLowerCase();
  if (!VERDICTS.has(verdict)) {
    return refuse(
      `unknown verdict ${JSON.stringify(data?.verdict ?? null)} — ` +
      `expected one of good, bad, unusable`);
  }

  // Unknown is not the same as unauthorised, and saying so matters: history is
  // a bounded ring, so a verdict on a job that scrolled out of it is a real and
  // blameless outcome that would otherwise read as a rejection.
  if (!job) {
    return refuse(
      `no job ${jobId} here — it was never served by this agent, or it has ` +
      `already fallen off the end of this agent's history`);
  }

  // Both sides scoped before comparing. The job's owner is usually already the
  // scoped form, having come off a job topic, but one rejection path records
  // the raw topic segment instead — and scoping is idempotent, so normalising
  // both is correct either way and costs nothing.
  const claimed = ownerScope(topicOwner);
  const actual = ownerScope(job.owner ?? job.requestedBy);
  if (claimed !== actual) {
    return refuse(
      `job ${jobId} was requested by ${actual}, and this verdict arrived as ` +
      `${claimed} — refused rather than reattributed`);
  }

  const reason = String(data?.reason ?? "").trim();
  const details = String(data?.details ?? "").trim();
  const lesson = String(data?.lesson ?? "").trim();
  const stamped = Date.parse(String(data?.ts ?? ""));

  return {
    reason: null,
    feedback: {
      verdict: verdict as Verdict,
      ...(reason ? { reason: reason.slice(0, MAX_REASON) } : {}),
      ...(details ? { details: details.slice(0, MAX_DETAILS) } : {}),
      ...(lesson ? { lesson: lesson.slice(0, MAX_LESSON) } : {}),
      // The topic's owner, not the payload's `by`. Whoever the broker let
      // publish here is who this is from.
      by: topicOwner,
      // A sender's own clock when it offers one, so a verdict queued through a
      // disconnect keeps the moment it was formed rather than the moment it
      // was finally delivered.
      ts: Number.isNaN(stamped) ? now : stamped,
    },
  };
}

/**
 * What a requester says about work it asked for.
 *
 * Three fields rather than one, because "bad" on its own teaches nobody
 * anything: why it was bad, what actually happened, and what a later run should
 * do instead — the last of which is the only part that changes a future run,
 * and is as worth writing about work that went well.
 */
export interface Said {
  reason?: string;
  details?: string;
  lesson?: string;
}

/** A verdict on its way out: the topic to publish to, and what to put on it. */
export interface OutboundVerdict {
  topic: string;
  payload: Record<string, unknown>;
}

/**
 * What to publish to file a verdict on a peer's work — or null, when there is
 * nothing worth publishing.
 *
 * It is addressed to the mesh's recorder, never to the peer. There is no
 * configuration that changes this and no second path: an agent cannot deliver a
 * verdict to another agent, only file one, and whether that filing becomes
 * delivery is decided by something else entirely. On a mesh with no recorder it
 * is a message nobody collects — the feature is absent because a participant is
 * absent, which is a stronger guarantee than any flag this agent could hold,
 * since a flag is a line in a file the agent's own operator can edit.
 *
 * `selfScope` is this agent's own scope and is not a caller's free choice: it
 * is the segment a broker rule grants, and a verdict filed under any other name
 * is refused silently, because a refused publish is still ACKed at QoS 1.
 */
export function verdictFor(
  root: string,
  peerId: string,
  selfScope: string,
  jobId: string,
  verdict: Verdict,
  said?: Said,
): OutboundVerdict | null {
  // Refused here rather than put on the wire for the recorder to reject: a
  // message that can only be thrown away is not worth sending.
  if (!jobId || !peerId || !VERDICTS.has(verdict)) return null;
  const trimmed = (v: unknown, cap: number) => {
    const s = String(v ?? "").trim();
    return s ? s.slice(0, cap) : "";
  };
  const reason = trimmed(said?.reason, MAX_REASON);
  const details = trimmed(said?.details, MAX_DETAILS);
  const lesson = trimmed(said?.lesson, MAX_LESSON);
  return {
    topic: feedbackFileTopic(root, selfScope, peerId, jobId),
    payload: {
      jobId,
      verdict,
      ...(reason ? { reason } : {}),
      ...(details ? { details } : {}),
      ...(lesson ? { lesson } : {}),
      ts: new Date().toISOString(),
    },
  };
}
