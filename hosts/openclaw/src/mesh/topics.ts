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
 * Where an agent writes down why a job went wrong (v1.5).
 *
 * Retained, unlike an event: a requester coming back tomorrow to ask why should
 * find the answer, and a postmortem is a durable fact about a job rather than a
 * moment in its run.
 */
export const jobPostmortemTopic = (root: string, owner: string, jobId: string): string =>
  `${root}/jobs/${owner}/${jobId}/postmortem`;

/**
 * Matches any owner-scoped job topic on this mesh, including ones our own
 * executors publish. Anchored, so the unscoped v1.0 form can never match —
 * job topics are always owner-scoped.
 */
export const jobTopicPattern = (meshRoot: string): RegExp =>
  new RegExp(`^${escapeRe(meshRoot)}/jobs/([^/]+)/([^/]+)/(events|result|postmortem)$`);

export interface ParsedJobTopic {
  owner: string;
  jobId: string;
  kind: "events" | "result" | "postmortem";
}

export function parseJobTopic(pattern: RegExp, topic: string): ParsedJobTopic | null {
  const m = pattern.exec(topic);
  if (!m) return null;
  return {
    owner: decodeURIComponent(m[1]),
    jobId: decodeURIComponent(m[2]),
    kind: m[3] as ParsedJobTopic["kind"],
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

/**
 * Where a requester files a verdict on work an agent did (v1.5).
 *
 * Addressed to the mesh's recorder, not to the agent. A verdict is not a
 * message between two peers — it is an assertion about the past that outlives
 * both of them, and the only thing on a mesh positioned to check it is the one
 * that can see every job: an agent knows the jobs it served, and nothing about
 * whether the requester's claim is consistent with the rest of the mesh.
 *
 * Everything addressable is in the topic — owner, agent, job — so a broker rule
 * can bound who may file one and under whose name. `feedback/ci/+/+` is a rule
 * any MQTT broker can apply.
 *
 * Publishing here is not delivery. Nothing reaches the agent until a recorder
 * relays it, and on a mesh without one this topic is a message nobody collects.
 * That is the design and not a shortcoming: a verdict nobody authenticated is
 * worth less than no verdict, because it is a stranger's opinion filed under
 * the requester's name.
 */
export const feedbackFileTopic = (
  root: string, owner: string, agentId: string, jobId: string,
): string => `${root}/feedback/${owner}/${agentId}/${jobId}`;

/** Every verdict filed on this mesh — the recorder's subscription. */
export const feedbackFileFilter = (root: string): string => `${root}/feedback/+/+/+`;

export interface ParsedFeedbackFile {
  owner: string;
  agentId: string;
  jobId: string;
}

/** The three names a filed verdict carries, or null if this is not one. */
export function parseFeedbackFileTopic(root: string, topic: string): ParsedFeedbackFile | null {
  const prefix = `${root}/feedback/`;
  if (!topic.startsWith(prefix)) return null;
  const parts = topic.slice(prefix.length).split("/");
  if (parts.length !== 3 || parts.some((p) => !p)) return null;
  return { owner: parts[0], agentId: parts[1], jobId: parts[2] };
}

/**
 * Where an agent hears what its work was worth — written by the recorder.
 *
 * The command path and not the job path, because under enforced ACLs an agent
 * subscribes only to `jobs/<its own scope>/#`: a verdict left in the
 * requester's scope is one the agent it is about could never read.
 *
 * Publish here is granted to the recorder alone. That is what makes an arriving
 * verdict worth acting on — it has already been checked against the mesh's own
 * record of who asked for what, which no single agent can do for itself.
 */
export const feedbackTopic = (root: string, agentId: string, owner: string): string =>
  `${root}/commands/${agentId}/feedback/${owner}`;

/** Every verdict relayed to this agent. */
export const feedbackFilter = (root: string, agentId: string): string =>
  `${root}/commands/${agentId}/feedback/+`;

/**
 * The owner a feedback topic carries, or null if this is not one.
 *
 * Returns the segment as it arrived, for the same reason `invokeTopicOwner`
 * does: this is the string the broker authorised, and normalising it here
 * would accept a topic no broker rule would have matched.
 */
export function feedbackTopicOwner(root: string, agentId: string, topic: string): string | null {
  const prefix = `${root}/commands/${agentId}/feedback/`;
  if (!topic.startsWith(prefix)) return null;
  const rest = topic.slice(prefix.length);
  return rest && !rest.includes("/") ? rest : null;
}
