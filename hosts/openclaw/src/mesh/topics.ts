/**
 * Topic construction and parsing.
 *
 * Pure: no I/O, no state. Everything the mesh addresses is derived here, so
 * the topic layout is defined in exactly one place and is directly testable.
 */

export interface MeshTopics {
  profile: string;
  status: string;
  invoke: string;
  query: string;
  cancel: string;
  config: string;
}

export const escapeRe = (s: string): string => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

export function buildTopics(meshRoot: string, agentId: string): MeshTopics {
  return {
    profile: `${meshRoot}/registry/${agentId}/profile`,
    status: `${meshRoot}/registry/${agentId}/status`,
    invoke: `${meshRoot}/commands/${agentId}/invoke`,
    query: `${meshRoot}/commands/${agentId}/query`,
    cancel: `${meshRoot}/commands/${agentId}/cancel`,
    config: `${meshRoot}/commands/${agentId}/config`,
  };
}

export const jobEventsTopic = (root: string, owner: string, jobId: string): string =>
  `${root}/jobs/${owner}/${jobId}/events`;

export const jobResultTopic = (root: string, owner: string, jobId: string): string =>
  `${root}/jobs/${owner}/${jobId}/result`;

/**
 * Matches any owner-scoped job topic on this mesh, including ones our own
 * executors publish. Anchored, so the unscoped v1.0 form can never match —
 * job topics are always owner-scoped.
 */
export const jobTopicPattern = (meshRoot: string): RegExp =>
  new RegExp(`^${escapeRe(meshRoot)}/jobs/([^/]+)/([^/]+)/(events|result)$`);

export interface ParsedJobTopic {
  owner: string;
  jobId: string;
  kind: "events" | "result";
}

export function parseJobTopic(pattern: RegExp, topic: string): ParsedJobTopic | null {
  const m = pattern.exec(topic);
  if (!m) return null;
  return {
    owner: decodeURIComponent(m[1]),
    jobId: decodeURIComponent(m[2]),
    kind: m[3] as "events" | "result",
  };
}

/**
 * Owner scope for job topics: listeners subscribe jobs/<theirId>/# and see
 * only their own traffic.
 *
 * Lowercased, restricted to [a-z0-9_-], edge separators trimmed. Empty becomes
 * "public" — the shared scope, which is why a client that omits requestedBy
 * would never see its own results.
 */
export const ownerScope = (requestedBy?: string): string => {
  const s = String(requestedBy ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return s || "public";
};

/** Every agent's retained profile and presence — how peers are discovered. */
export const registryProfileFilter = (root: string): string => `${root}/registry/+/profile`;
export const registryStatusFilter = (root: string): string => `${root}/registry/+/status`;

export const registryPattern = (meshRoot: string): RegExp =>
  new RegExp(`^${escapeRe(meshRoot)}/registry/([^/]+)/(profile|status)$`);

export interface ParsedRegistryTopic {
  agentId: string;
  kind: "profile" | "status";
}

export function parseRegistryTopic(pattern: RegExp, topic: string): ParsedRegistryTopic | null {
  const m = pattern.exec(topic);
  return m ? { agentId: decodeURIComponent(m[1]), kind: m[2] as "profile" | "status" } : null;
}

/** Where a peer accepts work, in the v1.3 form: the owner is in the payload. */
export const peerInvokeTopic = (root: string, agentId: string): string =>
  `${root}/commands/${agentId}/invoke`;

/**
 * Where a peer accepts work as a particular owner (v1.4).
 *
 * The owner is already scoped by the caller — this does not scope it, because a
 * topic that silently normalises `Mohanad.Q!` into `mohanad-q` would make one
 * identity two spellings, only one of which a broker ACL matches.
 */
export const peerInvokeTopicFor = (root: string, agentId: string, owner: string): string =>
  `${root}/commands/${agentId}/invoke/${owner}`;

/** Everything published to this agent's invoke topic, in either form. */
export const invokeFilter = (root: string, agentId: string): string =>
  `${root}/commands/${agentId}/invoke/+`;

/**
 * The owner an invoke topic carries, or null if this is not one.
 *
 * Returns the segment as it arrived. A caller that finds it is not already
 * scoped must reject the message rather than fix it: the string in the topic is
 * the one the broker authorised, and any other string is a different identity.
 */
export function invokeTopicOwner(root: string, agentId: string, topic: string): string | null {
  const prefix = `${root}/commands/${agentId}/invoke/`;
  if (!topic.startsWith(prefix)) return null;
  const rest = topic.slice(prefix.length);
  return rest && !rest.includes("/") ? rest : null;
}

export const peerCancelTopic = (root: string, agentId: string): string =>
  `${root}/commands/${agentId}/cancel`;
