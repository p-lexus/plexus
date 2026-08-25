/**
 * Two agents. One asks the other for help. About sixty seconds to run.
 *
 *   node examples/demo.mjs
 *
 * `reviewer` is asked to review a pull request. It finds a database migration
 * in the diff — something it has no business judging — so it looks in the mesh
 * registry, finds `dba`, delegates that part, waits, and folds the answer into
 * one combined review. Nobody wrote down that reviewer knows a dba. It looked.
 *
 * Needs a broker. Any broker:
 *   docker run -d -p 1883:1883 eclipse-mosquitto:2 \
 *     sh -c 'printf "listener 1883\nallow_anonymous true\n" > /m.conf && mosquitto -c /m.conf'
 *
 * Point it elsewhere with  PLEXUS_BROKER=mqtt://host:1883
 */

import { connect } from "../packages/agent/index.js";

const broker = process.env.PLEXUS_BROKER ?? "mqtt://localhost:1883";
// A fresh root per run, so a shared broker never crosses two demos.
const root = process.env.PLEXUS_ROOT ?? `plexus-demo-${Math.random().toString(36).slice(2, 6)}`;

const t0 = Date.now();
const dim = (s) => `\x1b[2m${s}\x1b[0m`;
const bold = (s) => `\x1b[1m${s}\x1b[0m`;
const teal = (s) => `\x1b[36m${s}\x1b[0m`;
const log = (who, msg) =>
  console.log(`${dim(`${((Date.now() - t0) / 1000).toFixed(1)}s`.padStart(5))}  ${teal(who.padEnd(9))} ${msg}`);

console.log(`\n  ${bold("Plexus")} ${dim("— two agents, one request")}`);
console.log(dim(`  broker ${broker}   root ${root}\n`));

// ── the specialist ──────────────────────────────────────────────────────────
// Knows migrations. Knows nothing about pull requests, and never needs to.

const dba = await connect({
  broker, root, agentId: "dba", displayName: "Database reviewer",
});

dba.serve("schema.review", async (job, ctx) => {
  log("dba", `asked about ${bold(job.args.migration)} ${dim(`(depth ${job.depth})`)}`);
  ctx.progress("checking lock behaviour");
  await new Promise((r) => setTimeout(r, 500));
  return {
    risk: "high",
    finding: "ALTER on users without CONCURRENTLY — locks writes for the whole rebuild",
  };
}, { description: "Reviews a migration for lock risk." });

// ── the generalist ──────────────────────────────────────────────────────────
// Handles the request, and delegates the part it isn't qualified for.

const reviewer = await connect({
  broker, root, agentId: "reviewer", displayName: "Code reviewer",
});

reviewer.serve("code.review", async (job, ctx) => {
  log("reviewer", `reviewing ${bold(`${job.args.repo}#${job.args.pr}`)}`);
  ctx.progress("reading the diff");
  await new Promise((r) => setTimeout(r, 400));

  log("reviewer", "the diff adds a migration — outside my competence");

  // Discovery, not configuration: ask the registry who can, right now.
  const peer = ctx.find("schema.review");
  if (!peer) return { verdict: "COMMENT", summary: "No schema specialist on the mesh." };

  log("reviewer", `${teal(peer)} offers ${bold("schema.review")} — delegating`);
  const answer = await ctx.askAny("schema.review", { migration: "0042_add_users_index.sql" });

  log("reviewer", `got ${bold(answer.risk)} risk back — folding it in`);
  return {
    verdict: answer.risk === "high" ? "REQUEST_CHANGES" : "APPROVE",
    summary: `Application logic is fine. Blocking on the migration: ${answer.finding}`,
    contributedBy: [peer],
  };
}, { description: "Reviews a pull request." });

// ── the requester ───────────────────────────────────────────────────────────
// A plain client. It asks one agent and gets one answer; that two of them were
// involved is an implementation detail of the mesh.

const alice = await connect({ broker, root, agentId: "alice", durable: false });

// Narrate the wire. This is exactly what an observer agent sees — and is the
// whole basis of Hermes, which turns these into Slack messages instead of logs.
await alice.watch((m) => {
  if (m.kind === "events" && m.type === "progress") log(dim("· mesh"), dim(`${m.jobId}  ${m.message}`));
});

await reviewer.waitForPeer("schema.review");     // let discovery settle
console.log("");
log("alice", `invoke ${bold("code.review")} ${dim("{ repo: acme/web-app, pr: 42 }")}`);

const result = await alice.invoke("reviewer", "code.review", { repo: "acme/web-app", pr: 42 });

console.log("");
log("alice", `${bold("← one result")}`);
console.log(
  JSON.stringify({ verdict: result.verdict, summary: result.summary, contributedBy: result.contributedBy }, null, 2)
    .split("\n").map((l) => "         " + dim(l)).join("\n"));

console.log(`\n  ${dim("reviewer never learned what a migration is. It found the agent that had,")}`);
console.log(`  ${dim("asked, and answered as one voice.")}\n`);

await Promise.all([alice.close(), reviewer.close(), dba.close()]);
