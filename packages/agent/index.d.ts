/**
 * Type definitions for plexus-agent.
 *
 * Hand-written rather than generated, so the library stays buildless: what you
 * install is what you read.
 */

import type { MqttClient } from "mqtt";

export declare const PROTOCOL_VERSION: string;

export interface Capability {
  service: string;
  description?: string;
  /** Human-readable argument shape, e.g. `{ repo: "string (owner/name)" }`. Advisory. */
  requestSchema?: Record<string, string>;
  [key: string]: unknown;
}

export interface AgentProfile {
  agentId: string;
  displayName: string;
  status: "online" | "offline";
  protocolVersion: string;
  capabilities: Capability[];
  ownerPolicy?: { required: boolean; verified: boolean };
  ts?: string;
}

/** An `invoke` as it arrives on the wire. */
export interface Job {
  service: string;
  args: Record<string, unknown>;
  requestedBy: string;
  jobId: string;
  /** The job that asked for this one. Absent when the request entered the mesh directly. */
  parentJobId?: string;
  /** The original request every job in the chain shares. */
  rootJobId?: string;
  /** Hops from the root. `0` entered the mesh directly. */
  depth?: number;
  ts?: string;
}

export interface AskOptions {
  timeoutMs?: number;
  jobId?: string;
  /** For `ask`: wait this long for a peer offering the service to appear. */
  waitMs?: number;
}

/** Passed to every handler as its second argument. */
export interface JobContext {
  agentId: string;
  root: string;
  /** This job's depth. Children are dispatched at `depth + 1`. */
  depth: number;
  /** Aborted when the job is cancelled. Check it around long work. */
  signal: AbortSignal;
  /** Publish a milestone. Advisory, never retained — late subscribers miss it. */
  progress(message: string, extra?: Record<string, unknown>): void;
  emit(payload: Record<string, unknown>): void;
  peers(): AgentProfile[];
  find(service: string): string | null;
  /** Delegate to a named peer. Lineage and depth are filled in for you. */
  ask<T = any>(peerId: string, service: string, args?: unknown, opts?: AskOptions): Promise<T>;
  /** Find a peer offering `service` and delegate to it. */
  askAny<T = any>(service: string, args?: unknown, opts?: AskOptions): Promise<T>;
}

export type JobHandler = (job: Job, ctx: JobContext) => unknown | Promise<unknown>;

/** A message seen by `watch`. */
export interface WatchedMessage {
  owner: string;
  jobId: string;
  kind: "events" | "result";
  type?: string;
  [key: string]: unknown;
}

export interface ConnectOptions {
  broker: string;
  agentId: string;
  displayName?: string;
  capabilities?: Capability[];
  root?: string;
  username?: string;
  password?: string;
  /** Override the derived client id. Required if two processes share an `agentId`. */
  clientId?: string;
  /** `clean: false` persistent session. Default true. */
  durable?: boolean;
  maxDepth?: number;
  askTimeoutMs?: number;
  requireOwner?: boolean;
  keepalive?: number;
  reconnectPeriod?: number;
  protocolVersion?: 4 | 5;
  log?: (message: string, meta?: object) => void;
}

export interface Agent {
  readonly agentId: string;
  readonly root: string;
  /** The underlying MQTT client, for anything this wrapper does not cover. */
  readonly client: MqttClient;
  serve(service: string, handler: JobHandler, meta?: Partial<Capability>): Agent;
  invoke<T = any>(peerId: string, service: string, args?: unknown, opts?: AskOptions): Promise<T>;
  ask<T = any>(service: string, args?: unknown, opts?: AskOptions): Promise<T>;
  peers(): AgentProfile[];
  find(service: string): string | null;
  waitForPeer(service: string, timeoutMs?: number): Promise<string>;
  /**
   * Observe mesh-wide job traffic. Right for observers, wrong for participants.
   * Await it: it resolves once the subscription swap is complete, and until then
   * an overlapping filter can deliver the same message twice.
   */
  watch(handler: (msg: WatchedMessage) => void, filter?: string): Promise<() => void>;
  /**
   * Observe every `invoke` on the mesh. Results carry the answer, not the
   * question — anything needing the request itself reads it here.
   */
  observeCommands(handler: (targetAgentId: string, invoke: Job) => void): Promise<() => void>;
  cancel(peerId: string, jobId: string): void;
  publish(topic: string, payload: unknown, retain?: boolean): void;
  close(): Promise<void>;
}

export declare function connect(options: ConnectOptions): Promise<Agent>;
export declare function deriveClientId(agentId: string, root: string): string;
export declare function ownerScope(requestedBy?: string): string;
export declare const topics: {
  profile(root: string, id: string): string;
  status(root: string, id: string): string;
  invoke(root: string, id: string): string;
  cancel(root: string, id: string): string;
  events(root: string, owner: string, jobId: string): string;
  result(root: string, owner: string, jobId: string): string;
  jobPattern(root: string): RegExp;
  registryPattern(root: string): RegExp;
};
