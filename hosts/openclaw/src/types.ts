/**
 * Domain types for the Agent Mesh bridge.
 *
 * Kept free of runtime imports so tests and tooling can pull these in without
 * dragging the MQTT client or the plugin SDK along with them.
 */

export const PROTOCOL_VERSION = "1.4";

// ── Configuration ──────────────────────────────────────

export interface BrokerConfig {
  url: string;
  username?: string;
  password?: string;
  clientId?: string;
  keepalive?: number;
  /** 4 = MQTT 3.1.1 (default, widest compatibility). 5 = MQTT 5 (adds session expiry). */
  protocolVersion?: 4 | 5;
  /** MQTT 5 only: how long the broker keeps our queued messages while we're down. */
  sessionExpirySeconds?: number;
}

export interface MeshConfig {
  root?: string;
  agentId?: string;
  servicesFile?: string;
  /** Where job history is kept so the panel survives a restart. */
  historyFile?: string;
  /** Reject invokes with no requestedBy (protocol 1.2 requires it). Default true. */
  requireOwner?: boolean;
  /**
   * Verify requestedBy against a broker-injected client_username (EMQX rule engine).
   * Fails closed: with this on, an invoke lacking client_username is rejected.
   * Default false — requires broker-side payload enrichment to be wired first.
   */
  verifyOwner?: boolean;
  /**
   * v1.4: whether to serve `commands/<agentId>/invoke/<owner>`.
   *
   * "accept" (default) serves both forms and takes the owner from the topic
   * when it is there. "off" serves the v1.3 form only.
   *
   * There is deliberately no mode in which this agent refuses the old form.
   * Refusing is enforcement, enforcement is the broker's, and an ACL that
   * grants `commands/+/invoke/<owner>` does not grant `commands/+/invoke` — so
   * the old form is already unpublishable wherever it matters. An agent that
   * refused it as well would only be blocking clients the broker already
   * blocks, on a mesh where nobody is being stopped anyway.
   */
  ownerInTopic?: "off" | "accept";
  /**
   * Whether the broker enforces who a requester may claim to be.
   *
   * The agent cannot find this out for itself — it can observe that some
   * subscription was refused, but not that invoke topics are scoped, and
   * guessing from one to the other would advertise a guarantee nobody made.
   * So it is stated in configuration by whoever applied the rules;
   * `plexus-server add-agent --owner-in-topic` writes it into the config it
   * generates, because at that moment it knows exactly what it applied.
   */
  ownerEnforced?: boolean;
  /** Hard wall-clock cap per job before the mesh declares it failed. Default 30 min. */
  maxJobDurationMs?: number;
  /**
   * How many delegation hops a request may travel (A asks B asks C = depth 2).
   * Bounds both runaway fan-out and A→B→A cycles. Default 4.
   */
  maxDepth?: number;
  /** How long mesh_ask waits for a peer's terminal result. Default 10 min. */
  askTimeoutMs?: number;
  /**
   * Which forms of agent-to-agent delegation this deployment permits.
   *
   *   "both"     declared dependencies AND the mesh_ask tool (default)
   *   "declared" only what a capability declares — deterministic, no tool needed
   *   "dynamic"  only mesh_ask — the executor decides mid-job
   *   "off"      no delegation; the agent works alone
   *
   * They fail differently, which is why both exist: declared cannot adapt to
   * what a job turns out to need, and dynamic depends on the executor choosing
   * to call a tool.
   */
  delegation?: "both" | "declared" | "dynamic" | "off";
  /**
   * Deployment values substituted into ${VAR} placeholders in capability
   * prompts, so one catalog runs everywhere and only the bindings differ.
   * Checked before the panel-managed store and before process.env.
   */
  promptVars?: Record<string, string>;
}

export interface WebConfig {
  enabled?: boolean;
  path?: string;
  auth?: string;
  port?: number;
}

export interface PluginConfig {
  broker: BrokerConfig;
  sessionKey?: string;
  mesh?: MeshConfig;
  web?: WebConfig;
}

// ── Capability catalog ─────────────────────────────────

/**
 * A dependency a capability declares up front: "before you run me, ask this
 * agent for this, and bind the answer to {{as}}".
 *
 * Declared delegation is performed by the bridge before the executor starts,
 * so it does not depend on the executor choosing to call a tool — or on tools
 * being reachable from the executor's session at all.
 */
export interface CapabilityDelegate {
  agent: string;
  service: string;
  /** Name the answer binds to in the prompt, as {{as}}. */
  as: string;
  /** Arguments for the peer. Values may reference the parent job's {{args}}. */
  args?: Record<string, unknown>;
  /** If true, a failed delegation fails the whole job. Default false. */
  required?: boolean;
}

export interface Capability {
  service: string;
  delegates?: CapabilityDelegate[];
  description?: string;
  requestSchema?: Record<string, unknown>;
  responseSchema?: Record<string, unknown>;
  avgLatency?: string;
  handler?: "session" | "silent";
  prompt?: string;
  [k: string]: unknown;
}

export interface ServicesFile {
  agentId?: string;
  displayName?: string;
  protocolVersion?: string;
  capabilities: Capability[];
}

// ── Jobs ───────────────────────────────────────────────

/** One milestone on a job's timeline, as published to the events topic. */
export interface JobEvent {
  type: string;
  note?: string;
  ts: number;
}

export type JobState =
  | "accepted" | "started" | "done" | "error"
  | "duplicate" | "timeout" | "cancelled" | "rejected";

export interface JobRecord {
  jobId: string;
  service?: string;
  state: JobState;
  lastEvent?: string;
  result?: unknown;
  requestedBy?: string;
  owner?: string;
  /**
   * Milestone history. Only the most recent event used to survive, which threw
   * away exactly what is needed to explain a job's behaviour — requeues above
   * all.
   */
  events?: JobEvent[];
  /** The job that asked for this one; absent on a request that entered the mesh. */
  parentJobId?: string;
  /** The original request every job in a delegation chain shares. */
  rootJobId?: string;
  /** Hops from the root. 0 for a request that entered the mesh directly. */
  depth?: number;
  /** True when this job is one WE asked a peer to do, rather than one we ran. */
  delegated?: boolean;
  /** For a delegated job: which peer is doing the work. */
  delegatedTo?: string;
  createdAt?: number;
  finishedAt?: number;
  updatedAt: number;
}

export const TERMINAL_STATES: ReadonlySet<string> =
  new Set<JobState>(["done", "error", "duplicate", "timeout", "cancelled", "rejected"]);

/** A job the watchdog is supervising until it produces a terminal result. */
export interface WatchEntry {
  jobId: string;
  service: string;
  owner: string;
  messageText: string;
  dispatchedAt: number;
  /** Last milestone published by the executor. */
  lastAgentEventAt: number;
  reinjections: number;
  /** Set when push dispatch succeeded. */
  runId?: string;
  /** True once waitForRun resolved (ok/error/timeout). */
  runSettled: boolean;
  subagentSessionKey: string;
}

export interface DispatchResult {
  ok: boolean;
  jobId?: string;
  error?: string;
}

export interface DispatchRequest {
  jobId?: string;
  service: string;
  args?: Record<string, unknown>;
  requestedBy?: string;
  /** Lineage, set by the asking agent so a chain can be traced end to end. */
  parentJobId?: string;
  rootJobId?: string;
  depth?: number;
}

/** Another agent on the mesh, learned from its retained registry profile. */
export interface Peer {
  agentId: string;
  displayName?: string;
  protocolVersion?: string;
  online: boolean;
  capabilities: Array<{
    service: string;
    description?: string;
    requestSchema?: Record<string, unknown>;
    avgLatency?: string;
  }>;
  ownerPolicy?: { required?: boolean; verified?: boolean; topic?: "off" | "accept" };
  /** When we last heard anything from this agent. */
  lastSeen: number;
}

export interface DispatchOptions {
  /** Used when requestedBy is absent, instead of rejecting. */
  defaultOwner?: string;
  /** Broker-supplied identity, when owner verification is enabled. */
  clientUsername?: string;
  /**
   * v1.4: the owner carried by the invoke topic, exactly as it arrived.
   *
   * This is the string a broker ACL matched, so it is authoritative — and it is
   * NOT scoped on the way in, because scoping it here would accept topics a
   * broker rule would never have allowed.
   */
  topicOwner?: string;
}

// ── Logging ────────────────────────────────────────────

export interface Logger {
  info(msg: string): void;
  warn(msg: string): void;
  error(msg: string): void;
  /**
   * An operational state change the operator must act on.
   *
   * Emitted at error level for correctness AND mirrored to info, because some
   * deployments capture only info-level plugin output — an alert nobody can
   * see is not an alert.
   */
  alert(msg: string): void;
}
