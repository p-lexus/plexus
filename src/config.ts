/**
 * Configuration resolution.
 *
 * Every default lives here, so "what happens if I leave this out?" has one
 * answer in one place. Pure and synchronous — takes raw plugin config, returns
 * fully-resolved settings.
 */

import * as path from "path";
import type { PluginConfig } from "./types.js";

export interface ResolvedConfig {
  broker: {
    url: string;
    username?: string;
    password?: string;
    clientId?: string;
    keepalive: number;
    protocolVersion: 4 | 5;
    sessionExpirySeconds: number;
  };
  mesh: {
    root: string;
    agentId: string;
    servicesFile: string;
    secretsFile: string;
    requireOwner: boolean;
    verifyOwner: boolean;
    maxJobDurationMs: number;
    promptVars: Record<string, string>;
  };
  web: {
    enabled: boolean;
    basePath: string;
    port: number;
    auth: string;
    dir: string;
  };
  sessionKey: string;
}

export const DEFAULTS = {
  meshRoot: "agents",
  agentId: "agent",
  keepalive: 30,
  protocolVersion: 4 as const,
  sessionExpirySeconds: 86_400,
  maxJobDurationMs: 30 * 60_000,
  webPort: 8765,
  webBasePath: "/mqtt-bridge/ui",
  sessionKey: "agent:main:main",
} as const;

/**
 * `${ENV_VAR}` indirection for broker credentials, so a password need not sit
 * in the config file. Anything that is not exactly `${NAME}` is returned as-is.
 */
export function resolveEnvRef(val?: string): string | undefined {
  if (!val) return undefined;
  const m = val.match(/^\$\{(.+)\}$/);
  return m ? process.env[m[1]] : val;
}

export function resolveConfig(cfg: Partial<PluginConfig>, pluginDir: string): ResolvedConfig {
  const mesh = cfg.mesh ?? {};
  const web = cfg.web ?? {};
  return {
    broker: {
      url: cfg.broker!.url,
      username: resolveEnvRef(cfg.broker?.username),
      password: resolveEnvRef(cfg.broker?.password),
      clientId: cfg.broker?.clientId,
      keepalive: cfg.broker?.keepalive ?? DEFAULTS.keepalive,
      protocolVersion: (cfg.broker?.protocolVersion ?? DEFAULTS.protocolVersion) as 4 | 5,
      sessionExpirySeconds: cfg.broker?.sessionExpirySeconds ?? DEFAULTS.sessionExpirySeconds,
    },
    mesh: {
      root: mesh.root ?? DEFAULTS.meshRoot,
      agentId: mesh.agentId ?? DEFAULTS.agentId,
      servicesFile: mesh.servicesFile ?? path.join(pluginDir, "services.json"),
      secretsFile: path.join(pluginDir, "mesh.local.json"),
      requireOwner: mesh.requireOwner !== false,   // default true
      verifyOwner: mesh.verifyOwner === true,      // default false
      maxJobDurationMs: mesh.maxJobDurationMs ?? DEFAULTS.maxJobDurationMs,
      promptVars: (mesh.promptVars ?? {}) as Record<string, string>,
    },
    web: {
      enabled: web.enabled !== false,              // default on
      basePath: web.path ?? DEFAULTS.webBasePath,
      port: web.port ?? DEFAULTS.webPort,
      auth: typeof web.auth === "string" && web.auth.trim() ? web.auth.trim() : "",
      dir: path.join(pluginDir, "web"),
    },
    sessionKey: cfg.sessionKey ?? DEFAULTS.sessionKey,
  };
}
