/**
 * plexus-notify — tell people what the mesh decided.
 *
 * A Plexus plugin. It adds two things to whatever agent loads it:
 *
 *   - a watcher, which matches job results against routes and delivers them to
 *     Slack, GitHub, a webhook, a file or the terminal;
 *   - a capability, `notify.send`, so any agent on the mesh can ask this one to
 *     deliver something directly, mid-job.
 *
 * It is a plugin rather than an agent on purpose. Delivery is not a specialism
 * that deserves its own identity, its own registry entry and its own durable
 * session — it is something an agent *also* does. Loading it into an existing
 * host keeps the mesh describing specialists rather than plumbing.
 *
 * Two behaviours matter before you trust it with a pager.
 *
 * **Results are retained.** Every subscriber receives the full backlog the
 * moment it connects. A notifier that treats those as new pages the whole team
 * about work that finished last Tuesday, every restart. This suppresses the
 * retained flush and remembers what it has delivered, across restarts.
 *
 * **Delivery is at-least-once**, like everything else here. A failed channel
 * retries with backoff; a success whose acknowledgement is lost may send twice.
 * Prefer a duplicate message to a missed one — but if you route to something
 * that *acts* rather than informs, make it idempotent.
 */

import { definePlugin } from "plexus-agent/plugin";
import { readFile, writeFile, rename } from "node:fs/promises";
import { createHash } from "node:crypto";
import { plan } from "./routes.js";
import { loadChannels, redact } from "./channels.js";

const DEFAULTS = {
  state: "./notify.state.json",
  /** `skip` ignores the retained backlog on first sight; `all` delivers it. */
  replay: "skip",
  replayGraceMs: 3_000,
  retries: [1_000, 5_000, 20_000],
  rememberMs: 7 * 24 * 3_600_000,
  maxRemembered: 20_000,
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Remembers what has already been delivered, so a restart is not a re-broadcast.
 *
 * Keyed per route *and* channel: if Slack succeeds and GitHub is rate-limited,
 * the retry re-sends only the GitHub comment.
 */
class DeliveryLog {
  constructor(path, { rememberMs, maxRemembered }) {
    this.path = path;
    this.rememberMs = rememberMs;
    this.maxRemembered = maxRemembered;
    this.entries = new Map();
    this.dirty = false;
    this.timer = null;
  }

  static key(jobId, routeId, channel, payload) {
    // The payload hash matters: a job can legitimately publish a corrected
    // result to the same retained topic, and that is a new thing to say.
    const h = createHash("sha1").update(JSON.stringify(payload ?? "")).digest("hex").slice(0, 8);
    return `${jobId}:${routeId}:${channel}:${h}`;
  }

  async load() {
    try {
      const raw = JSON.parse(await readFile(this.path, "utf8"));
      for (const [k, v] of Object.entries(raw.delivered ?? {})) this.entries.set(k, v);
      this.prune();
    } catch (err) {
      if (err.code !== "ENOENT") throw new Error(`cannot read state file ${this.path}: ${err.message}`);
    }
    return this;
  }

  has(key) { return this.entries.has(key); }

  mark(key) {
    this.entries.set(key, Date.now());
    this.dirty = true;
    // Debounced: a burst writes once, but nothing waits long enough that a
    // crash loses more than a second of history.
    if (!this.timer) {
      this.timer = setTimeout(() => { this.timer = null; this.flush().catch(() => {}); }, 1_000);
      if (this.timer.unref) this.timer.unref();
    }
  }

  prune() {
    const cutoff = Date.now() - this.rememberMs;
    for (const [k, ts] of this.entries) if (ts < cutoff) this.entries.delete(k);
    if (this.entries.size > this.maxRemembered) {
      const sorted = [...this.entries].sort((a, b) => a[1] - b[1]);
      for (const [k] of sorted.slice(0, this.entries.size - this.maxRemembered)) this.entries.delete(k);
    }
  }

  async flush() {
    if (!this.dirty) return;
    this.prune();
    this.dirty = false;
    const tmp = `${this.path}.tmp`;
    // Write-then-rename: a crash mid-write leaves the old state intact rather
    // than a truncated file that fails to parse and re-broadcasts everything.
    await writeFile(tmp, JSON.stringify({ delivered: Object.fromEntries(this.entries) }), { mode: 0o600 });
    await rename(tmp, this.path);
  }
}

export default definePlugin({
  name: "notify",
  description: "Delivers job results to Slack, GitHub, webhooks and files.",

  async setup(agent, config = {}, ctx = {}) {
    const cfg = { ...DEFAULTS, ...config };
    const log = ctx.log ?? ((msg) => console.log(`notify  ${redact(String(msg))}`));

    const channels = loadChannels(cfg.channels);
    const routes = cfg.routes ?? [];
    const deliveryLog = await new DeliveryLog(cfg.state, cfg).load();

    // What was asked, remembered per job.
    //
    // A result carries the answer, not the question — so "changes requested on
    // acme/web#42" cannot be rendered from a result alone. The plugin watches
    // invoke traffic too, and keeps the request beside the outcome.
    const requestOf = new Map();
    const stats = { seen: 0, delivered: 0, failed: 0, suppressed: 0 };
    let acceptingFrom = Infinity;
    let stopped = false;

    /** Send one payload to one channel, retrying transient failures. */
    async function deliver(channelName, payload, routeId) {
      const channel = channels.get(channelName);
      if (!channel) {
        log(`route "${routeId}" names channel "${channelName}", which is not configured`);
        stats.failed++;
        return;
      }

      const key = DeliveryLog.key(payload.jobId, routeId, channelName, payload.context.result);
      if (deliveryLog.has(key)) { stats.suppressed++; return; }

      const attempts = cfg.dryRun ? [0] : [0, ...cfg.retries];
      for (let i = 0; i < attempts.length; i++) {
        if (attempts[i]) await sleep(attempts[i]);
        try {
          if (cfg.dryRun) log(`[dry-run] ${channelName} ← ${payload.title}`);
          else await channel.impl.send(payload, channel.config, { log });
          deliveryLog.mark(key);
          stats.delivered++;
          log(`delivered ${payload.jobId} → ${channelName} (${routeId})`);
          return;
        } catch (err) {
          const last = i === attempts.length - 1;
          log(`${channelName} failed for ${payload.jobId}` +
            `${last ? "" : `, retrying in ${attempts[i + 1]}ms`}: ${err.message}`);
          if (last) stats.failed++;
        }
      }
    }

    /** A result arrived. Decide what to say about it, and to whom. */
    async function onResult(message) {
      stats.seen++;
      const request = requestOf.get(message.jobId);
      const enriched = {
        ...message,
        service: message.service ?? request?.service,
        args: message.args ?? request?.args ?? {},
        agent: request?.target,
      };
      const jobs = plan(routes, enriched);
      if (!jobs.length) return;

      // Everything retained on the broker arrives the instant we subscribe. On a
      // cold start that is the entire history of the mesh, and none of it is news.
      if (Date.now() < acceptingFrom) {
        for (const { route, channels: names, payload } of jobs) {
          for (const name of names) {
            deliveryLog.mark(DeliveryLog.key(payload.jobId, route.id ?? "route", name, payload.context.result));
          }
        }
        stats.suppressed += jobs.length;
        return;
      }

      for (const { route, channels: names, payload } of jobs) {
        // Channels run concurrently: a slow webhook must not delay the page.
        await Promise.all(names.map((name) => deliver(name, payload, route.id ?? "route")));
      }
    }

    // A capability, so an agent can ask for delivery directly — mid-job, before
    // it has a result — exactly as it would ask any other peer.
    agent.serve("notify.send", async (job) => {
      const { title = "Notification", body = "", level = "info", to } = job.args ?? {};
      const names = to ? (Array.isArray(to) ? to : [to]) : [...channels.keys()];
      const payload = {
        title, body, level,
        jobId: job.jobId, owner: job.requestedBy, service: "notify.send", type: "notification",
        context: { title, body, level, result: job.args },
      };
      const results = await Promise.allSettled(names.map((n) => deliver(n, payload, `notify.send:${job.jobId}`)));
      return {
        delivered: names.filter((_, i) => results[i].status === "fulfilled"),
        failed: names.filter((_, i) => results[i].status === "rejected"),
      };
    }, {
      description: "Delivers a message to configured channels (Slack, GitHub, webhook, file).",
      requestSchema: {
        title: "string", body: "string (markdown)",
        level: "info | warn | error", to: "string[] — channel names, defaults to all",
      },
    });

    // Read the questions before the answers, so an invoke is always on record
    // by the time its result arrives.
    const unobserve = await agent.observeCommands((target, invoke) => {
      if (!invoke?.jobId) return;
      requestOf.set(invoke.jobId, {
        target, service: invoke.service, args: invoke.args ?? {}, requestedBy: invoke.requestedBy,
      });
      // Bounded: a long-lived agent must not accumulate every job it ever saw.
      if (requestOf.size > 10_000) {
        for (const k of [...requestOf.keys()].slice(0, 5_000)) requestOf.delete(k);
      }
    });

    // A second subscription rather than a wider one: alerts are not job traffic
    // and folding them into the same filter would mean every plugin watching
    // jobs also receives them whether it wants to or not.
    const unwatchAlerts = await agent.watch((message) => {
      if (message.kind !== "alert") return;
      onResult({ ...message, type: "alert" })
        .catch((err) => log(`routing an alert failed: ${err.message}`));
    }, "alerts");

    const unwatch = await agent.watch((message) => {
      if (message.kind === "events") {
        // `accepted` repeats the service; useful when this plugin started after
        // the invoke was published and so never saw the request.
        if (message.type === "accepted" && message.service && !requestOf.has(message.jobId)) {
          requestOf.set(message.jobId, { service: message.service, args: {} });
        }
        // A verdict is the one milestone worth telling somebody about. It
        // cannot be watched where it actually travels — commands/<agent>/
        // feedback/<owner> belongs to the judged agent and no plugin may read
        // another agent's commands — so the agent republishes it here, on the
        // job's own timeline, which is what makes it reachable at all.
        //
        // Only this one. Every other milestone is progress, and a notification
        // per milestone is how a channel becomes something people mute.
        if (message.type === "feedback") {
          onResult(message).catch((err) => log(`routing failed: ${err.message}`));
        }
        return;
      }
      onResult(message).catch((err) => log(`routing failed: ${err.message}`));
    });

    acceptingFrom = cfg.replay === "all" ? 0 : Date.now() + cfg.replayGraceMs;
    if (cfg.replay !== "all") {
      const timer = setTimeout(() => {
        if (!stopped) log(`listening — ${stats.suppressed} retained result(s) noted but not delivered`);
      }, cfg.replayGraceMs);
      timer.unref?.();
    }

    log(`${channels.size} channel(s): ${[...channels.keys()].join(", ") || "none"} · ${routes.length} route(s)`
      + (cfg.dryRun ? " · DRY RUN, nothing will actually be sent" : ""));

    return {
      stats,
      async stop() {
        stopped = true;
        unwatch?.();
        unwatchAlerts?.();
        unobserve?.();
        await deliveryLog.flush().catch(() => {});
        log(`seen ${stats.seen}, delivered ${stats.delivered}, ` +
          `suppressed ${stats.suppressed}, failed ${stats.failed}`);
      },
    };
  },
});

export { plan, matches, render, deliveryContext } from "./routes.js";
export { channels, loadChannels, redact, expandEnv } from "./channels.js";
