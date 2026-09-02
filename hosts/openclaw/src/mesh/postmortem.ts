/**
 * When a job is worth explaining, and what to ask for.
 *
 * Pure: no I/O, so the rate limit and the trigger are testable without a
 * runtime, and the guards are not something only a live agent exercises.
 */

import type { JobRecord, Verdict } from "../types.js";

export const POSTMORTEMS_PER_HOUR = 2;

/** Which verdicts are worth an explanation. */
const POOR: ReadonlySet<string> = new Set<Verdict>(["bad", "unusable"]);

const FAILED: ReadonlySet<string> = new Set(["error", "timeout"]);

export type Trigger = "failure" | "verdict";

/**
 * Why this job should be explained, or null.
 *
 * A postmortem is never written about a postmortem: an agent explaining its own
 * explanations produces work nobody asked for and, on a capability that keeps
 * failing, does so without end.
 */
export function triggerFor(job: JobRecord | undefined): Trigger | null {
  if (!job || job.postmortem) return null;
  if (FAILED.has(job.state)) return "failure";
  if (job.feedback?.some((f) => POOR.has(f.verdict))) return "verdict";
  return null;
}

/**
 * What makes two failures the same failure.
 *
 * A capability failing the same way forty times is one thing to explain, and
 * forty executor runs to explain it are forty runs spent saying it again.
 */
export function signatureOf(job: JobRecord, trigger: Trigger): string {
  const worst = job.feedback?.find((f) => POOR.has(f.verdict))?.verdict ?? "";
  const detail = trigger === "failure" ? job.state : worst;
  return `${job.service ?? "unknown"}:${trigger}:${detail}`;
}

export interface Limiter {
  /** Whether this failure may be explained now, and taking the slot if so. */
  take(signature: string, now: number): boolean;
}

/**
 * A cap per signature per hour.
 *
 * Bounded by signature rather than globally: one flapping capability must not
 * use up the budget that would have explained a different one.
 */
export function createLimiter(perHour = POSTMORTEMS_PER_HOUR): Limiter {
  const seen = new Map<string, number[]>();
  const HOUR = 3_600_000;

  return {
    take(signature, now) {
      const recent = (seen.get(signature) ?? []).filter((at) => now - at < HOUR);
      if (recent.length >= perHour) {
        seen.set(signature, recent);
        return false;
      }
      recent.push(now);
      seen.set(signature, recent);
      return true;
    },
  };
}

/**
 * What the executor is asked to write.
 *
 * It publishes the answer itself: the runtime hands back a run status and never
 * the text, so there is nothing for the bridge to collect. A postmortem that
 * never arrives leaves nobody waiting, which is why that is acceptable here and
 * would not be for a job.
 */
export function promptFor(job: JobRecord, trigger: Trigger, topic: string): string {
  const timeline = (job.events ?? [])
    .map((e) => `  ${new Date(e.ts).toISOString()}  ${e.type}${e.note ? ` — ${e.note}` : ""}`)
    .join("\n") || "  (no milestones were recorded)";

  const judged = (job.feedback ?? [])
    .filter((f) => POOR.has(f.verdict))
    .map((f) => `  ${f.by} called it ${f.verdict}${f.reason ? `: ${f.reason}` : ""}`)
    .join("\n");

  const opening = trigger === "failure"
    ? `Job ${job.jobId} (${job.service ?? "unknown service"}) ended ${job.state}.`
    : `Job ${job.jobId} (${job.service ?? "unknown service"}) finished, and the requester judged it poorly.`;

  return [
    `${opening} Write a short postmortem — this is read back before this capability runs again, so it is worth being specific.`,
    "",
    "What happened:",
    timeline,
    judged ? `\nWhat the requester said:\n${judged}` : "",
    job.result ? `\nWhat was published:\n  ${JSON.stringify(job.result).slice(0, 800)}` : "",
    "",
    "Answer three things: what was asked, what went wrong, and what a later run should do differently.",
    "Do not restate the timeline. If the cause is not recoverable from what is above, say so — a guess recorded as a finding is worse than an admission.",
    "",
    `Then publish it with mqtt_publish to ${topic}, as`,
    `{"jobId":"${job.jobId}","summary":"...","lesson":"...","details":"..."} where`,
    "  summary  one line: what went wrong",
    "  lesson   what a later run should do — this is read back before this capability runs again",
    "  details  the fuller account, for somebody who needs more than the summary",
    "Publish exactly once. Do not publish anything else, and do not start any other work.",
  ].filter(Boolean).join("\n");
}
