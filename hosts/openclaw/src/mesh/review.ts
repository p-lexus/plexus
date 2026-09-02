/**
 * What a requester owes the agent it asked.
 *
 * A delegation used to be judged only when it went wrong, on the reasoning that
 * an answer which merely arrived is not thereby a good one. True, and it left
 * the mesh learning from failure alone: the capability that keeps getting it
 * right is indistinguishable from the one nobody has complained about yet.
 *
 * So on a mesh with a recorder every delegation is reviewed, and the review is
 * a record rather than a label — why, what happened, and what a later run
 * should do. The last part is the only one that changes anything, and it is
 * worth writing about work that went well: "keep checking the rate limit" is a
 * lesson; "nice one" is not.
 *
 * Pure: a job goes in, a prompt comes out, so what the executor is asked can be
 * read in a test rather than inferred from a live run.
 */

import type { JobRecord } from "../types.js";

/** How long the executor gets to judge before the floor verdict is filed. */
export const REVIEW_GRACE_MS = 120_000;

/**
 * The verdict filed when nobody judged in time.
 *
 * Deliberately honest rather than flattering: it records that the work was
 * delivered and that no one looked at it closely, which is a different and much
 * weaker claim than "good". A mesh full of unexamined praise would be worse
 * than one with gaps, because the gaps at least admit what they are.
 */
export const UNJUDGED = {
  verdict: "good" as const,
  reason: "delivered, and the requester did not judge it further",
  lesson: "",
};

export function reviewPromptFor(job: JobRecord, agent: string): string {
  const asked = job.service ?? "a capability";
  const answer = job.result ? JSON.stringify(job.result).slice(0, 1200) : "(nothing was published)";

  return [
    `You asked ${agent} for ${asked} as job ${job.jobId}, and it answered.`,
    "",
    "What came back:",
    `  ${answer}`,
    "",
    "Judge it, now, while you still have the context. This is read back to that capability",
    "before it runs again, so it is worth being specific and worth being honest — including",
    "when the work was good.",
    "",
    `Call mesh_feedback with jobId "${job.jobId}" and:`,
    "  verdict  good if it did the job, bad if it was poor, unusable if it did not answer at all",
    "  reason   why, in one or two sentences",
    "  details  what you asked for, what came back, and what you did with it",
    "  lesson   what a later run should do — specific enough to act on, for good work as well as bad",
    "",
    "Call it exactly once. Do not start any other work.",
  ].join("\n");
}
