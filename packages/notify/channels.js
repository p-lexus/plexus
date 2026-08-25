/**
 * Delivery channels.
 *
 * A channel takes a rendered payload and puts it somewhere a person will see
 * it. Each one is a plain object with a `send`, so adding a channel is adding a
 * key here — no registration, no plugin loader.
 *
 * Two rules every channel follows:
 *
 *   1. Throw on failure. Hermes retries with backoff and gives up loudly; a
 *      channel that swallows its own error turns a missed page into silence.
 *   2. Never put a credential in an error message. Errors get logged, and a
 *      token in a log is a token in a log forever.
 */

import { appendFile } from "node:fs/promises";
import { render } from "./routes.js";

/** Expand `${VAR}` from the environment. Keeps secrets out of the config file. */
export function expandEnv(value, env = process.env) {
  if (typeof value !== "string") return value;
  return value.replace(/\$\{([A-Za-z_][A-Za-z0-9_]*)\}/g, (_m, name) => {
    const v = env[name];
    if (v === undefined) throw new Error(`config references \${${name}}, which is not set in the environment`);
    return v;
  });
}

/** Strip anything token-shaped from text destined for a log. */
export const redact = (text) =>
  String(text)
    .replace(/\b(gh[pousr]_[A-Za-z0-9]{16,})\b/g, "gh?_***")
    .replace(/\bxox[baprs]-[A-Za-z0-9-]{10,}\b/g, "xox?-***")
    .replace(/(https:\/\/hooks\.slack\.com\/services\/)\S+/g, "$1***")
    .replace(/([?&](?:token|key|secret|password)=)[^&\s]+/gi, "$1***");

async function postJson(url, body, headers = {}) {
  const res = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    // The URL can itself be the secret (Slack webhooks are), so it is never echoed.
    throw new Error(`HTTP ${res.status} ${res.statusText}${text ? ` — ${redact(text).slice(0, 300)}` : ""}`);
  }
  return res;
}

export const channels = {
  /** Prints. Always available, needs no configuration — the default for `--dry-run`. */
  console: {
    async send(payload, config, { log }) {
      const line = config.format === "json"
        ? JSON.stringify({ title: payload.title, body: payload.body, jobId: payload.jobId })
        : `${payload.title}\n${payload.body}`;
      log(line, { level: payload.level });
    },
  },

  /** Appends one JSON object per line. An audit trail that survives a restart. */
  file: {
    async send(payload, config) {
      const path = expandEnv(config.path ?? "./hermes.log.jsonl");
      await appendFile(path, JSON.stringify({
        ts: new Date().toISOString(),
        jobId: payload.jobId, owner: payload.owner, service: payload.service,
        type: payload.type, title: payload.title, body: payload.body,
      }) + "\n");
    },
  },

  /** POSTs the whole payload. The escape hatch for anything not listed here. */
  webhook: {
    async send(payload, config) {
      const headers = Object.fromEntries(
        Object.entries(config.headers ?? {}).map(([k, v]) => [k, expandEnv(v)]));
      await postJson(expandEnv(config.url), {
        title: payload.title, body: payload.body, level: payload.level,
        jobId: payload.jobId, owner: payload.owner, service: payload.service, type: payload.type,
        result: config.includeResult === false ? undefined : payload.context.result,
      }, headers);
    },
  },

  /** Slack incoming webhook. The URL is the credential — treat it as one. */
  slack: {
    async send(payload, config) {
      const emoji = { error: ":rotating_light:", warn: ":warning:", info: ":white_check_mark:" }[payload.level] ?? "";
      await postJson(expandEnv(config.webhookUrl), {
        text: `${emoji} ${payload.title}`.trim(),
        blocks: [
          { type: "section", text: { type: "mrkdwn", text: `${emoji} *${payload.title}*`.trim() } },
          ...(payload.body ? [{ type: "section", text: { type: "mrkdwn", text: payload.body.slice(0, 2900) } }] : []),
          { type: "context", elements: [{ type: "mrkdwn",
            text: `job \`${payload.jobId}\` · ${payload.service ?? "—"} · requested by ${payload.owner}` }] },
        ],
        ...(config.channel ? { channel: config.channel } : {}),
      });
    },
  },

  /** Comments on a pull request or issue. */
  github: {
    async send(payload, config) {
      const repo = render(config.repo ?? "{{repo}}", payload.context);
      const issue = render(config.issue ?? "{{pr}}", payload.context);
      if (!repo || !issue) {
        // Explicit, because the usual cause is a result that simply doesn't
        // carry a repo — which is a routing mistake, not a GitHub outage.
        throw new Error(`github channel needs a repo and an issue/pr number; got repo="${repo}" issue="${issue}". ` +
          `Set them on the channel, e.g. "repo": "{{args.repo}}".`);
      }
      const api = config.apiUrl ?? "https://api.github.com";
      await postJson(`${api}/repos/${repo}/issues/${issue}/comments`,
        { body: `**${payload.title}**\n\n${payload.body}` },
        {
          authorization: `Bearer ${expandEnv(config.token)}`,
          accept: "application/vnd.github+json",
          "user-agent": "hermes-plexus",
        });
    },
  },
};

/** Build the configured channel set, failing fast on anything unusable. */
export function loadChannels(configured = {}) {
  const out = new Map();
  for (const [name, config] of Object.entries(configured)) {
    if (config?.enabled === false) continue;
    const impl = channels[config?.type ?? name];
    if (!impl) {
      throw new Error(`channel "${name}" has unknown type "${config?.type ?? name}". ` +
        `Known types: ${Object.keys(channels).join(", ")}`);
    }
    out.set(name, { name, impl, config: config ?? {} });
  }
  return out;
}
