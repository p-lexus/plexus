/**
 * Domain types for the Agent Mesh bridge.
 *
 * Kept free of runtime imports so tests and tooling can pull these in without
 * dragging the MQTT client or the plugin SDK along with them.
 */

export const PROTOCOL_VERSION = "1.2";

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
  /** Reject invokes with no requestedBy (protocol 1.2 requires it). Default true. */
  requireOwner?: boolean;
  /**
   * Verify requestedBy against a broker-injected client_username (EMQX rule engine).
   * Fails closed: with this on, an invoke lacking client_username is rejected.
   * Default false — requires broker-side payload enrichment to be wired first.
   */
  verifyOwner?: boolean;
  /** Hard wall-clock cap per job before the mesh declares it failed. Default 30 min. */
  maxJobDurationMs?: number;
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

export interface Capability {
  service: string;
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
}

export interface DispatchOptions {
  /** Used when requestedBy is absent, instead of rejecting. */
  defaultOwner?: string;
  /** Broker-supplied identity, when owner verification is enabled. */
  clientUsername?: string;
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
