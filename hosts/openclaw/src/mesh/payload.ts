/**
 * Outbound job payload normalisation.
 *
 * PROTOCOL.md requires every result to carry jobId/owner/ts/type and to be
 * RETAINED. That used to depend entirely on the executor following a sentence
 * of prose, and a forgotten `retain: true` meant late subscribers saw nothing
 * at all. The guarantee is enforced here, at the transport boundary, so
 * conformance does not rest on prompt-following.
 *
 * Pure: topic in, normalised payload out.
 */

import { parseJobTopic } from "./topics.js";

export interface NormalisedPublish {
  payload: string;
  retain: boolean;
}

export function normalizeJobPublish(
  jobTopicRe: RegExp,
  topic: string,
  payload: string,
  retainRequested?: boolean,
): NormalisedPublish {
  const parsed = parseJobTopic(jobTopicRe, topic);
  // Not a job topic (config, status, anything else) — pass through untouched.
  if (!parsed) return { payload, retain: retainRequested ?? false };

  let obj: any;
  try { obj = JSON.parse(payload); } catch { obj = { note: payload }; }
  // A bare string, array or null is still valid JSON but cannot carry the
  // required fields, so wrap rather than discard it.
  if (obj === null || typeof obj !== "object" || Array.isArray(obj)) obj = { value: obj };

  const normalised = {
    ...obj,
    jobId: obj.jobId ?? parsed.jobId,
    owner: obj.owner ?? parsed.owner,
    ts: obj.ts ?? new Date().toISOString(),
    type: obj.type ?? (parsed.kind === "result" ? "result" : "progress"),
  };

  return {
    payload: JSON.stringify(normalised),
    // Results and postmortems are retained by protocol; events deliberately are
    // not, so a subscriber joining mid-job sees the outcome but not stale
    // progress.
    retain: parsed.kind === "events" ? (retainRequested ?? false) : true,
  };
}

/**
 * Render a capability prompt.
 *
 * Order matters: `${VAR}` expands BEFORE `{{args}}`. Reversed, a caller could
 * pass "${SOME_SECRET}" as an argument value and have the bridge expand it,
 * turning every invoke into an environment read.
 */
/**
 * Whether a publish to a job topic must be refused, and why.
 *
 * Results are retained by protocol, so the last publish wins forever. A second
 * executor — one the watchdog re-dispatched over a job that was merely quiet —
 * therefore does not produce a duplicate anyone can ignore: it overwrites the
 * answer with its own, and a late subscriber collects the overwrite. In the
 * case this was written for, a completed review was replaced by
 * "already_reviewed" from the executor that arrived second.
 *
 * Cancellation refuses everything, which is what the mesh already promises on
 * cancel_acknowledged. Completion refuses further results only: trailing
 * milestones are not retained and cost nothing.
 */
export function publishRefusal(
  kind: "events" | "result" | "postmortem" | null,
  state: { cancelled: boolean; finished: boolean },
  jobId: string,
): string | null {
  if (!kind) return null;
  if (state.cancelled) {
    return `job ${jobId} was cancelled — its terminal result is already published, ` +
      `and the mesh promises no further traffic for it`;
  }
  if (state.finished && kind === "result") {
    return `job ${jobId} already published a terminal result — publishing another would ` +
      `overwrite it on the broker, because results are retained`;
  }
  return null;
}

/**
 * Declared-required arguments the caller did not supply.
 *
 * Narrower than `unresolvedPlaceholders`, and deliberately so. A prompt may
 * reference `{{something}}` the schema never declared — that is an incomplete
 * schema, not a malformed request, and refusing it would break capabilities
 * that work today. But a field the capability's own author declared without a
 * `?` is required by the author, and a job missing it cannot be executed
 * meaningfully: the executor is handed "Review pull request  in acme/app" and
 * spends a real run failing at it.
 */
export function missingRequiredArgs(
  template: string,
  args: Record<string, unknown>,
  requestSchema: Record<string, unknown> = {},
): string[] {
  const out = new Set<string>();
  for (const m of String(template).matchAll(/\{\{(\w+)\}\}/g)) {
    const k = m[1];
    if (k === "jobId" || k === "requestedBy") continue;
    if (args?.[k] !== undefined) continue;
    const declared = requestSchema?.[k];
    if (typeof declared === "string" && !/\?/.test(declared)) out.add(k);
  }
  return [...out];
}

/**
 * Which placeholders in a template will render as nothing.
 *
 * `renderPrompt` substitutes an empty string for anything it cannot resolve.
 * That is correct at render time and wrong to do silently: the executor is
 * handed a prompt with holes in it — "Review pull request  in " — and reviews
 * whatever it can guess from the rest, with nothing anywhere saying an
 * argument never arrived.
 *
 * Optionality comes from the capability's own `requestSchema`, where a `?`
 * marks a field the caller may omit — the same convention the panel reads. An
 * omitted optional argument is not a hole; an omitted required one is, and so
 * is a placeholder the schema never declared at all.
 */
export function unresolvedPlaceholders(
  template: string,
  args: Record<string, unknown>,
  isVarBound: (name: string) => boolean,
  requestSchema: Record<string, unknown> = {},
): string[] {
  const out = new Set<string>();

  for (const m of String(template).matchAll(/\$\{([A-Za-z_][A-Za-z0-9_]*)\}/g)) {
    if (!isVarBound(m[1])) out.add(`\${${m[1]}}`);
  }

  for (const m of String(template).matchAll(/\{\{(\w+)\}\}/g)) {
    const k = m[1];
    if (k === "jobId" || k === "requestedBy") continue;       // injected, never from args
    if (args?.[k] !== undefined) continue;
    const declared = requestSchema?.[k];
    const optional = typeof declared === "string" && /\?/.test(declared);
    if (!optional) out.add(`{{${k}}}`);
  }

  return [...out];
}

export function renderPrompt(
  template: string,
  vars: (name: string) => string,
  jobId: string,
  requestedBy: string,
  args: Record<string, unknown>,
): string {
  return String(template)
    .replace(/\$\{([A-Za-z_][A-Za-z0-9_]*)\}/g, (_m, k: string) => vars(k))
    .replace(/\{\{jobId\}\}/g, jobId)
    .replace(/\{\{requestedBy\}\}/g, requestedBy || "unknown")
    .replace(/\{\{(\w+)\}\}/g, (_m, k: string) => String(args?.[k] ?? ""));
}
