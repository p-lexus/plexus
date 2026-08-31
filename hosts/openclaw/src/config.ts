/**
 * Configuration resolution.
 *
 * Every default lives here, so "what happens if I leave this out?" has one
 * answer in one place. Synchronous, and pure but for one thing: choosing where
 * this deployment's files live looks at whether they are already somewhere
 * else, so an upgrade cannot silently start reading an empty catalog.
 */

import * as fs from "fs";
import * as os from "os";
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
    /** Job history, so the panel is not empty after a restart. */
    historyFile: string;
    requireOwner: boolean;
    verifyOwner: boolean;
    ownerInTopic: "off" | "accept";
    ownerEnforced: boolean;
    feedback: "off" | "accept";
    maxJobDurationMs: number;
    maxDepth: number;
    askTimeoutMs: number;
    delegation: "both" | "declared" | "dynamic" | "off";
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
  maxDepth: 4,
  askTimeoutMs: 10 * 60_000,
  delegation: "both" as const,
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

/**
 * Where this deployment's own files live: the capability catalog, the prompt
 * variables, the job history.
 *
 * Beside `openclaw.json`, not inside the plugin. What an agent DOES is the
 * deployment's, and the plugin is a thing that gets replaced — reinstalled,
 * rebuilt, pulled over. A catalog living inside it is one `rm -rf` from gone,
 * and it rode into `dist/` on every build, which put six capabilities and their
 * prompts inside a build artefact that has no business carrying them.
 *
 * `OPENCLAW_HOME` first, because that is what the gateway itself honours.
 */
export function deploymentDir(env: NodeJS.ProcessEnv = process.env): string {
  const raw = env.OPENCLAW_HOME?.trim();
  const home = raw
    ? path.resolve(raw.replace(/^~(?=$|[/\\])/, os.homedir()))
    : os.homedir();
  return path.join(home, ".openclaw", "plexus");
}

/**
 * The new path, unless a file is already sitting at an old one.
 *
 * TWO old ones, and the second is the reason this needs saying: `pluginDir` is
 * the directory of the BUILT module — `dist/` — not the checkout that contains
 * it. That is exactly why the build used to copy `services.json` into `dist/`:
 * the plugin never read the checkout's copy at all.
 *
 * Checking only `dist/` therefore finds nothing the moment that copy stops
 * being made, and the catalog falls through to the shipped example. Tried on a
 * live agent, that produced six capabilities with the right NAMES and the
 * example's prompts — an agent that looks correct in every list and answers
 * with somebody else's instructions.
 */
function deploymentFile(name: string, pluginDir: string, exists = fs.existsSync): string {
  const current = path.join(deploymentDir(), name);
  if (exists(current)) return current;
  for (const legacy of [path.join(pluginDir, name), path.join(pluginDir, "..", name)]) {
    if (exists(legacy)) return path.resolve(legacy);
  }
  return current;
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
      servicesFile: mesh.servicesFile ?? deploymentFile("services.json", pluginDir),
      secretsFile: deploymentFile("mesh.local.json", pluginDir),
      historyFile: mesh.historyFile ?? deploymentFile("jobs.local.json", pluginDir),
      requireOwner: mesh.requireOwner !== false,   // default true
      // v1.4. Both forms served, the owner taken from the topic when it is
      // there. There is no mode in which the agent refuses the old form:
      // refusing is enforcement, and enforcement belongs to the broker.
      ownerInTopic: mesh.ownerInTopic === "off" ? "off" : "accept",
      // Stated by whoever applied the broker's rules, because only they know.
      ownerEnforced: mesh.ownerEnforced === true,
      // v1.5, and OFF unless somebody says otherwise. A verdict is only worth
      // recording where the broker vouches for who is speaking, and on a broker
      // with no rules anyone can file one in anybody's name.
      feedback: mesh.feedback === "accept" ? "accept" : "off",
      verifyOwner: mesh.verifyOwner === true,      // default false
      maxJobDurationMs: mesh.maxJobDurationMs ?? DEFAULTS.maxJobDurationMs,
      maxDepth: mesh.maxDepth ?? DEFAULTS.maxDepth,
      askTimeoutMs: mesh.askTimeoutMs ?? DEFAULTS.askTimeoutMs,
      delegation: mesh.delegation ?? DEFAULTS.delegation,
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
