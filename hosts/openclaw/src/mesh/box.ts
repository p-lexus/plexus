/**
 * Where this agent's box lives, and the credential it connects with.
 *
 * Kept beside the deployment variables in `mesh.local.json` — 0600, never in
 * the repository, never in `openclaw.json` — so that connecting an agent to a
 * box is something an operator does in the panel rather than by hand-editing
 * the gateway's configuration and restarting it.
 *
 * The password is write-only, exactly like a deployment variable: there is a
 * path that sets it and none that reads it back. A panel needs to know whether
 * one is set, never what it is.
 */
import * as fs from "fs";
import * as path from "path";

export interface BoxSettings {
  url?: string;
  username?: string;
  password?: string;
}

/** What a panel may see: everything except the secret. */
export interface BoxView {
  url: string;
  username: string;
  hasPassword: boolean;
  /** Where these came from, because "why is it still connecting there" is the question. */
  source: "panel" | "config";
}

export function readBox(file: string): BoxSettings {
  try {
    const parsed = JSON.parse(fs.readFileSync(file, "utf8"));
    const b = parsed?.broker;
    if (!b || typeof b !== "object") return {};
    return {
      url: typeof b.url === "string" ? b.url : undefined,
      username: typeof b.username === "string" ? b.username : undefined,
      password: typeof b.password === "string" ? b.password : undefined,
    };
  } catch {
    return {};   // absent is the normal case, not an error
  }
}

/**
 * Merge and write, keeping everything else in the file.
 *
 * A field left out is left alone: a panel that saves a host without re-typing
 * the password must not blank the password. Sending an empty string is how a
 * value is cleared, which is a different intention and is honoured.
 */
export function writeBox(file: string, patch: BoxSettings): string | null {
  let doc: Record<string, unknown> = {};
  try {
    const parsed = JSON.parse(fs.readFileSync(file, "utf8"));
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) doc = parsed;
  } catch { /* a new file */ }

  const broker = { ...(doc.broker as BoxSettings | undefined ?? {}) };
  for (const key of ["url", "username", "password"] as const) {
    if (patch[key] === undefined) continue;
    if (patch[key] === "") delete broker[key];
    else broker[key] = patch[key];
  }
  doc.broker = broker;

  try {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    // 0600 and replaced atomically, so a crash mid-write cannot leave a
    // half-file that drops a credential and locks the agent out of its mesh.
    const tmp = `${file}.tmp`;
    fs.writeFileSync(tmp, `${JSON.stringify(doc, null, 2)}\n`, { mode: 0o600 });
    fs.renameSync(tmp, file);
    return null;
  } catch (e: any) {
    return e.message;
  }
}

/** A broker URL this agent could actually dial. */
export function checkURL(url: string): string | null {
  if (!url) return "a broker URL is required, for example mqtts://box.example.com:8883";
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return `${url} is not a URL — it needs a scheme, like mqtts://host:8883`;
  }
  const schemes = ["mqtt:", "mqtts:", "ws:", "wss:"];
  if (!schemes.includes(parsed.protocol)) {
    return `${parsed.protocol}// is not an MQTT scheme — use ${schemes.map((s) => `${s}//`).join(", ")}`;
  }
  if (!parsed.hostname) return "that URL names no host";
  return null;
}
