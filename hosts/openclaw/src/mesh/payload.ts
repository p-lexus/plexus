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
    // Results are retained by protocol; events deliberately are not, so a
    // subscriber joining mid-job sees the outcome but not stale progress.
    retain: parsed.kind === "result" ? true : (retainRequested ?? false),
  };
}

/**
 * Render a capability prompt.
 *
 * Order matters: `${VAR}` expands BEFORE `{{args}}`. Reversed, a caller could
 * pass "${SOME_SECRET}" as an argument value and have the bridge expand it,
 * turning every invoke into an environment read.
 */
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
