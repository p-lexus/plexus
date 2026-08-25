/**
 * The same two agents, plus a third that runs a plugin.
 *
 *   node examples/with-plugins.mjs
 *
 * `notifier` does none of the work. It is an agent with no capabilities of its
 * own — everything it can do comes from the `plexus-notify` plugin loaded into
 * it. The plugin watches the mesh and turns outcomes into messages: here to the
 * terminal, in practice to Slack or a pull request.
 *
 * This is the whole layering in one file:
 *
 *   Plexus          the protocol — topics and payloads on MQTT
 *   plexus-agent    a client for it, and a host that runs plugins
 *   plexus-notify   a plugin. Add capabilities by loading, not by writing
 *
 * In production this host is a config file and `plexus run`. The host is
 * assembled here in code only so the example is one runnable file.
 */

import { connect } from "../packages/agent/index.js";
import { createHost } from "../packages/agent/plugin.js";
import notify from "../packages/notify/index.js";
import { rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

const broker = process.env.PLEXUS_BROKER ?? "mqtt://localhost:1883";
const root = process.env.PLEXUS_ROOT ?? `plexus-demo-${Math.random().toString(36).slice(2, 6)}`;
const state = join(tmpdir(), `${root}.state.json`);

const dim = (s) => `\x1b[2m${s}\x1b[0m`;
const bold = (s) => `\x1b[1m${s}\x1b[0m`;
const teal = (s) => `\x1b[36m${s}\x1b[0m`;
const amber = (s) => `\x1b[33m${s}\x1b[0m`;
const t0 = Date.now();
// Pad before colouring: escape codes have width on the string and none on screen.
const log = (who, msg, colour = teal) =>
  console.log(`${dim(`${((Date.now() - t0) / 1000).toFixed(1)}s`.padStart(5))}  ${colour(who.padEnd(9))} ${msg}`);

console.log(`\n  ${bold("Plexus")} ${dim("— agents do the work, a plugin tells the team")}`);
console.log(dim(`  broker ${broker}   root ${root}\n`));

// ── the mesh does the work ──────────────────────────────────────────────────

const dba = await connect({ broker, root, agentId: "dba" });
dba.serve("schema.review", async (job, ctx) => {
  log("dba", `reviewing ${bold(job.args.migration)}`);
  ctx.progress("checking lock behaviour");
  await new Promise((r) => setTimeout(r, 400));
  return { risk: "high", finding: "ALTER on users without CONCURRENTLY — locks writes for the whole rebuild" };
}, { description: "Reviews a migration for lock risk." });

const reviewer = await connect({ broker, root, agentId: "reviewer" });
reviewer.serve("code.review", async (job, ctx) => {
  log("reviewer", `reviewing ${bold(`${job.args.repo}#${job.args.pr}`)}`);
  await new Promise((r) => setTimeout(r, 300));
  log("reviewer", "found a migration — delegating that part");
  const answer = await ctx.askAny("schema.review", { migration: "0042_add_users_index.sql" });
  return {
    verdict: answer.risk === "high" ? "REQUEST_CHANGES" : "APPROVE",
    summary: `Application logic is fine. Blocking on the migration: ${answer.finding}`,
    contributedBy: ["dba"],
  };
}, { description: "Reviews a pull request." });

// ── an agent whose only ability is a plugin ─────────────────────────────────

const notifier = createHost({
  broker, root, agentId: "notifier", displayName: "Notifier",
  plugins: {
    "plexus-notify": {
      state, replayGraceMs: 200,
      channels: {
        // Swap this for  { type: "slack", webhookUrl: "${SLACK_WEBHOOK_URL}" }
        // and the same routes post to a channel instead. No agent changes.
        desk: { type: "console" },
      },
      routes: [
        {
          id: "changes-requested",
          when: { service: "code.review", verdict: "REQUEST_CHANGES" },
          to: ["desk"],
          level: "warn",
          // `args` comes from the invoke; `summary` from the result. The plugin
          // keeps the question beside the answer so a message can name both.
          title: "Changes requested on {{args.repo}}#{{args.pr}}",
          body: "{{summary}}\n         reviewed for {{owner}} · job {{jobId}}",
        },
        {
          id: "risky-migrations",
          when: { service: "schema.review", risk: { $in: ["high", "critical"] } },
          to: ["desk"],
          level: "warn",
          title: "Risky migration flagged by {{agent}}",
          body: "{{finding}}",
        },
      ],
    },
  },
}, {
  resolve: () => notify,                     // already imported; no package lookup needed
  log: (msg) => {
    if (!/^[A-Z]/.test(msg) && !msg.includes("\n")) return;      // skip lifecycle chatter
    const [title, ...rest] = msg.split("\n");
    log("notify", bold(title), amber);
    for (const line of rest) console.log(dim(`         ${line.trim()}`));
  },
});
await notifier.start();
await new Promise((r) => setTimeout(r, 400));       // let the replay grace elapse

// ── someone asks ────────────────────────────────────────────────────────────

const alice = await connect({ broker, root, agentId: "alice", durable: false });
await reviewer.waitForPeer("schema.review");
console.log("");
log("alice", `invoke ${bold("code.review")} ${dim("{ repo: acme/web-app, pr: 42 }")}`);

const result = await alice.invoke("reviewer", "code.review", { repo: "acme/web-app", pr: 42 });
await new Promise((r) => setTimeout(r, 500));       // let the plugin finish delivering

console.log("");
log("alice", `${bold("← " + result.verdict)}`);
console.log(`\n  ${dim("Two agents did the work. A plugin told the team. Neither knows the other exists.")}\n`);

await Promise.all([alice.close(), reviewer.close(), dba.close(), notifier.stop()]);
await rm(state, { force: true });
