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

import { ownerScope } from "./topics.js";
import type { Feedback, JobRecord, Verdict } from "../types.js";

const VERDICTS: ReadonlySet<string> = new Set<Verdict>(["good", "bad", "unusable"]);

/** Long enough for a paragraph of why, short enough not to be a payload. */
export const MAX_REASON = 500;

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
  const stamped = Date.parse(String(data?.ts ?? ""));

  return {
    reason: null,
    feedback: {
      verdict: verdict as Verdict,
      ...(reason ? { reason: reason.slice(0, MAX_REASON) } : {}),
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
