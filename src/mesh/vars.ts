/**
 * Deployment variables — the values substituted into `${VAR}` in capability
 * prompts, so one catalog runs in every deployment and only the bindings differ.
 *
 * Three layers, highest wins:
 *
 *   1. openclaw.json  mesh.promptVars   config-as-code, the operator's explicit intent
 *   2. mesh.local.json                  panel-managed, 0600, gitignored
 *   3. process.env                      inherited environment
 *
 * Panel-managed values get their own file rather than being written into
 * openclaw.json: that file belongs to the gateway, is JSONC with comments and
 * trailing commas, and rewriting it programmatically would destroy formatting
 * the operator wrote by hand.
 *
 * Values never leave the process. Callers can ask where a name resolves from,
 * and for a masked hint, but there is deliberately no API that returns a value.
 */

import * as fs from "fs";
import * as path from "path";
import type { Logger } from "../types.js";

export type VarSource = "config" | "local" | "env" | "unset";

export interface VarStore {
  /** Effective value. For internal use only — never send this to a client. */
  value(name: string): string | undefined;
  source(name: string): VarSource;
  /** Every name known from config or the local file, sorted. */
  names(): string[];
  /** Safe to serialise: name, source, masked hint, and whether we may edit it. */
  describe(): Array<{ name: string; source: VarSource; hint: string; editable: boolean }>;
  /** Returns an error string, or null on success. */
  set(name: string, value: string): string | null;
  remove(name: string): string | null;
  /** True when the name is pinned in config and therefore not ours to change. */
  isPinned(name: string): boolean;
  readonly fileName: string;
}

/** Enough to recognise a value, never enough to reconstruct one. */
export function maskValue(v: string): string {
  if (!v) return "";
  return v.length > 8 ? `••••${v.slice(-4)}` : "••••";
}

export function createVarStore(
  secretsFile: string,
  configVars: Record<string, string>,
  logger: Logger,
): VarStore {
  let localVars: Record<string, string> = readLocal();

  function readLocal(): Record<string, string> {
    try {
      const parsed = JSON.parse(fs.readFileSync(secretsFile, "utf8"));
      const vars = parsed?.promptVars;
      return vars && typeof vars === "object" ? vars as Record<string, string> : {};
    } catch {
      return {};   // absent is the normal case, not an error
    }
  }

  function writeLocal(vars: Record<string, string>): string | null {
    try {
      // 0600, and replaced atomically via rename, so a crash mid-write cannot
      // leave a half-file that silently drops every variable.
      const tmp = `${secretsFile}.tmp`;
      fs.writeFileSync(tmp, JSON.stringify({ promptVars: vars }, null, 2) + "\n", { mode: 0o600 });
      fs.renameSync(tmp, secretsFile);
      try { fs.chmodSync(secretsFile, 0o600); } catch { /* best effort on odd filesystems */ }
      localVars = vars;
      return null;
    } catch (e: any) {
      // Never put a value in an error string — this reaches logs.
      logger.error(`writing ${path.basename(secretsFile)} failed: ${e.code ?? e.message}`);
      return `could not write ${path.basename(secretsFile)}`;
    }
  }

  const value = (name: string): string | undefined =>
    configVars[name] ?? localVars[name] ?? process.env[name];

  const source = (name: string): VarSource =>
    name in configVars ? "config"
      : name in localVars ? "local"
        : process.env[name] !== undefined ? "env"
          : "unset";

  const names = (): string[] =>
    [...new Set([...Object.keys(configVars), ...Object.keys(localVars)])].sort();

  if (Object.keys(localVars).length) {
    logger.info(`${Object.keys(localVars).length} local prompt variable(s) loaded`);
  }

  return {
    fileName: path.basename(secretsFile),
    value,
    source,
    names,
    isPinned: (name) => name in configVars,
    describe: () => names().map((name) => ({
      name,
      source: source(name),
      hint: maskValue(value(name) ?? ""),
      editable: source(name) !== "config",
    })),
    set(name, v) {
      if (name in configVars) {
        return `${name} is pinned in openclaw.json (mesh.promptVars) and takes precedence. Remove it there to manage it here.`;
      }
      const err = writeLocal({ ...localVars, [name]: v });
      if (!err) logger.info(`prompt variable ${name} set`);   // name only, never the value
      return err;
    },
    remove(name) {
      if (name in configVars) {
        return `${name} is pinned in openclaw.json (mesh.promptVars). Remove it there instead.`;
      }
      const next = { ...localVars };
      delete next[name];
      const err = writeLocal(next);
      if (!err) logger.info(`prompt variable ${name} removed`);
      return err;
    },
  };
}
