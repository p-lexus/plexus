/**
 * Tests for the published packages: plexus-agent and plexus-notify.
 *
 * The pure parts run everywhere. The end-to-end block needs a broker and is
 * skipped without one — but when a broker IS present it exercises the whole
 * path, because the things that break in a mesh (retained replay, duplicate
 * delivery, lineage) do not show up in unit tests of pure functions.
 *
 *   PLEXUS_TEST_BROKER=mqtt://localhost:1883 node test/packages.mjs
 */

import assert from "node:assert/strict";
import net from "node:net";
import { readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { ownerScope, topics, deriveClientId, PROTOCOL_VERSION, connect } from "plexus-agent";
import { aclFor, permits, topicMatches } from "plexus-agent/acl";
import { get, testCondition, matches, render, plan, deliveryContext } from "plexus-notify/routes";
import { expandEnv, redact, loadChannels } from "plexus-notify/channels";
import { createHost, definePlugin } from "plexus-agent/plugin";
import notifyPlugin from "plexus-notify";

let pass = 0, fail = 0;
const queue = [];
function t(name, fn) {
  queue.push(async () => {
    try { await fn(); console.log(`✅ ${name}`); pass++; }
    catch (e) { console.log(`❌ ${name}\n     ${e.stack?.split("\n").slice(0, 3).join("\n     ")}`); fail++; }
  });
}

// ── plexus-agent: pure ──────────────────────────────────
t("ownerScope matches the bridge's implementation exactly", () => {
  assert.equal(ownerScope("Mohanad.Q!"), "mohanad-q");
  assert.equal(ownerScope("--ci--"), "ci");
  assert.equal(ownerScope(""), "public");
  assert.equal(ownerScope(undefined), "public");
});

t("topics build the documented address space", () => {
  assert.equal(topics.invoke("agents", "dba"), "agents/commands/dba/invoke");
  assert.equal(topics.result("agents", "alice", "j1"), "agents/jobs/alice/j1/result");
  assert.equal(topics.profile("agents", "dba"), "agents/registry/dba/profile");
});

t("job pattern refuses the unscoped form", () => {
  const re = topics.jobPattern("agents");
  assert.ok(re.test("agents/jobs/alice/j1/result"));
  assert.equal(re.test("agents/jobs/j1/result"), false, "unscoped job topics must never match");
});

t("clientId is stable across calls and distinct per agent and mesh", () => {
  // The durability invariant: same inputs must always give the same id, or
  // clean:false buys nothing and queued jobs are orphaned on every restart.
  assert.equal(deriveClientId("dba", "agents"), deriveClientId("dba", "agents"));
  assert.notEqual(deriveClientId("dba", "agents"), deriveClientId("reviewer", "agents"));
  assert.notEqual(deriveClientId("dba", "agents"), deriveClientId("dba", "staging"));
  assert.ok(!deriveClientId("dba", "agents").includes(String(process.pid)));
});

t("protocol version is the one the bridge speaks", () => {
  assert.equal(PROTOCOL_VERSION, "1.5");
});

t("v1.5: the feedback topic builders put the judge where an ACL can see it", () => {
  assert.equal(topics.feedback("acme/agents", "reviewer", "ci"),
    "acme/agents/commands/reviewer/feedback/ci");
  assert.equal(topics.feedbackFilter("acme/agents", "reviewer"),
    "acme/agents/commands/reviewer/feedback/+");
});

// ── plexus-agent: broker rules ──────────────────────────
const R = "acme/agents";

t("a requester sees its own job scope and nobody else's", () => {
  const ci = aclFor({ root: R, role: "requester", id: "ci" });
  assert.ok(permits(ci.subscribe, `${R}/jobs/ci/ci-42/result`));
  assert.ok(!permits(ci.subscribe, `${R}/jobs/mohanad/rev-030/result`),
    "the whole point: one requester cannot read another's answers");
  assert.ok(!permits(ci.subscribe, `${R}/jobs/mohanad/rev-030/events`));
});

t("an agent may publish its own identity and no other", () => {
  const rev = aclFor({ root: R, role: "agent", id: "reviewer" });
  assert.ok(permits(rev.publish, `${R}/registry/reviewer/profile`));
  assert.ok(permits(rev.publish, `${R}/registry/reviewer/status`));
  assert.ok(!permits(rev.publish, `${R}/registry/dba/profile`),
    "impersonation is the attack these rules exist to stop");
});

t("an agent answers for many owners but reads only its own commands", () => {
  const rev = aclFor({ root: R, role: "agent", id: "reviewer" });
  assert.ok(permits(rev.publish, `${R}/jobs/anyone/j1/result`), "it serves whoever asks");
  assert.ok(permits(rev.subscribe, `${R}/commands/reviewer/invoke`));
  assert.ok(!permits(rev.subscribe, `${R}/commands/dba/invoke`),
    "one agent must not receive another's work");
});

t("an agent can delegate, and collect the answer in its own scope", () => {
  const rev = aclFor({ root: R, role: "agent", id: "reviewer" });
  assert.ok(permits(rev.publish, `${R}/commands/dba/invoke`), "asking a peer");
  assert.ok(permits(rev.subscribe, `${R}/jobs/reviewer/ask-9f3c/result`), "the answer comes back");
});

t("nobody but the console may read the whole mesh", () => {
  const con = aclFor({ root: R, role: "console", id: "console" });
  const ci = aclFor({ root: R, role: "requester", id: "ci" });
  assert.ok(permits(con.subscribe, `${R}/jobs/mohanad/rev-030/result`));
  assert.ok(!permits(ci.subscribe, `${R}/jobs/mohanad/rev-030/result`));
});

t("the v1.4 proposal puts the owner where a broker can enforce it", () => {
  const loose = aclFor({ root: R, role: "requester", id: "ci" });
  const tight = aclFor({ root: R, role: "requester", id: "ci", ownerInTopic: true });

  // Today: any authenticated client may publish any invoke, and requestedBy is
  // a payload field no ACL can police.
  assert.ok(permits(loose.publish, `${R}/commands/reviewer/invoke`));

  // With the owner in the topic, claiming to be someone else is refused by the
  // broker, before any agent sees it.
  assert.ok(permits(tight.publish, `${R}/commands/reviewer/invoke/ci`));
  assert.ok(!permits(tight.publish, `${R}/commands/reviewer/invoke/mohanad`));
});

t("v1.4: the invoke topic builders put the owner where an ACL can see it", () => {
  assert.equal(topics.invokeAs("acme/agents", "reviewer", "ci"), "acme/agents/commands/reviewer/invoke/ci");
  assert.equal(topics.invokeFilter("acme/agents", "reviewer"), "acme/agents/commands/reviewer/invoke/+");
  // The v1.3 form is untouched: both are served during the transition.
  assert.equal(topics.invoke("acme/agents", "reviewer"), "acme/agents/commands/reviewer/invoke");
});

t("v1.5: an agent reads the mesh's memory and cannot write it", () => {
  const rev = aclFor({ root: R, role: "agent", id: "reviewer" });
  const ci = aclFor({ root: R, role: "requester", id: "ci" });
  assert.ok(permits(rev.subscribe, `${R}/memory/code.review`));
  assert.ok(!permits(rev.publish, `${R}/memory/code.review`),
    "an agent that could write here would be writing the memory of capabilities it does not serve");
  assert.ok(!permits(ci.subscribe, `${R}/memory/code.review`),
    "and a requester has no business reading what agents were told about themselves");

  // Somebody has to write it, and it is the one identity that sees every job.
  const box = aclFor({ root: R, role: "console", id: "console" });
  assert.ok(permits(box.publish, `${R}/memory/code.review`));
});

t("v1.5: an agent may write down why its own job went wrong", () => {
  const rev = aclFor({ root: R, role: "agent", id: "reviewer" });
  const ci = aclFor({ root: R, role: "requester", id: "ci" });
  assert.ok(permits(rev.publish, `${R}/jobs/ci/j1/postmortem`),
    "an agent serves many owners, so its postmortems cannot be narrowed by id");
  assert.ok(!permits(ci.publish, `${R}/jobs/ci/j1/postmortem`),
    "a requester does not get to write the agent's account of what happened");
});

t("v1.5: a verdict can only be filed under the identity that gives it", () => {
  const ci = aclFor({ root: R, role: "requester", id: "ci" });
  assert.ok(permits(ci.publish, `${R}/feedback/ci/reviewer/j1`), "judging work it asked for");
  assert.ok(permits(ci.publish, `${R}/feedback/ci/dba/j9`), "any agent it asked");
  assert.ok(!permits(ci.publish, `${R}/feedback/mohanad/reviewer/j1`),
    "a verdict in somebody else's name is the forgery these rules exist to stop");
});

t("v1.5: nobody but the recorder can make an agent hear a verdict", () => {
  // The enforcement, in one assertion. A requester may FILE a verdict and may
  // not DELIVER one — the leg that reaches an agent's command topic belongs to
  // the console, which is the mesh's recorder. Granting it to requesters would
  // hand back exactly the thing the two-leg design removes.
  const ci = aclFor({ root: R, role: "requester", id: "ci" });
  const rev = aclFor({ root: R, role: "agent", id: "reviewer" });
  const box = aclFor({ root: R, role: "console", id: "console" });

  assert.ok(!permits(ci.publish, `${R}/commands/reviewer/feedback/ci`),
    "a requester cannot put a verdict on an agent's command topic");
  assert.ok(!permits(rev.publish, `${R}/commands/dba/feedback/reviewer`),
    "and neither can an agent, however honest");
  assert.ok(permits(box.publish, `${R}/commands/reviewer/feedback/ci`), "the recorder relays it");

  // And nobody but the recorder reads the filings.
  assert.ok(permits(box.subscribe, `${R}/feedback/ci/reviewer/j1`));
  assert.ok(!permits(ci.subscribe, `${R}/feedback/mohanad/reviewer/j1`));
  assert.ok(!permits(rev.subscribe, `${R}/feedback/ci/reviewer/j1`));
});

t("v1.5: feedback is enforceable even where invokes are not", () => {
  // ownerInTopic off means requestedBy is a payload field no ACL can police.
  // Feedback was specified after that lesson and has only the one form, so the
  // narrower rule applies either way.
  const loose = aclFor({ root: R, role: "requester", id: "ci" });
  assert.ok(permits(loose.publish, `${R}/commands/reviewer/invoke`), "the old invoke is unpoliced");
  assert.ok(!permits(loose.publish, `${R}/feedback/mohanad/reviewer/j1`),
    "and the verdict still is not");
});

t("v1.5: an agent files as itself, and hears verdicts on its own work", () => {
  const rev = aclFor({ root: R, role: "agent", id: "reviewer" });
  assert.ok(permits(rev.publish, `${R}/feedback/reviewer/dba/ask-1`),
    "an agent that delegates is a requester, and owes a verdict");
  assert.ok(!permits(rev.publish, `${R}/feedback/ci/dba/ask-1`),
    "but not one in a person's name");
  // Its own commands subtree already carries the relayed leg — no new rule.
  assert.ok(permits(rev.subscribe, `${R}/commands/reviewer/feedback/ci`));
  assert.ok(!permits(rev.subscribe, `${R}/commands/dba/feedback/ci`),
    "one agent must not read another's reviews");
});

t("an id that would widen a filter is refused, not sanitised", () => {
  for (const bad of ["ci/+", "#", "+", "a/b", "", "-lead"]) {
    assert.throws(() => aclFor({ root: R, role: "requester", id: bad }), TypeError,
      `id ${JSON.stringify(bad)} must be refused`);
  }
  assert.throws(() => aclFor({ root: "acme/#", role: "requester", id: "ci" }), TypeError);
  assert.throws(() => aclFor({ root: R, role: "admin", id: "x" }), TypeError);
});

t("topic matching follows MQTT, including the $SYS exclusion", () => {
  assert.ok(topicMatches("a/+/c", "a/b/c"));
  assert.ok(!topicMatches("a/+/c", "a/b/x/c"));
  assert.ok(topicMatches("a/#", "a/b/c/d"));
  assert.ok(!topicMatches("a/#", "b/c"));
  assert.ok(!topicMatches("#", "$SYS/broker/uptime"), "a bare wildcard never reaches $SYS");
  assert.ok(topicMatches("$SYS/#", "$SYS/broker/uptime"));
  assert.ok(!topicMatches("a/b", "a/b/c"));
});

t("a failed job is routable, and so is the account of why", () => {
  const routes = [
    { id: "failed", when: { type: ["error", "timeout"] }, to: "ops", title: "{{service}} failed", level: "serious" },
    { id: "why", when: { kind: "postmortem" }, to: "ops", title: "why {{service}} failed", body: "{{lesson}}" },
  ];

  const failure = plan(routes, { jobId: "j1", owner: "ci", kind: "result", service: "code.review", type: "error", error: "timed out reading the diff" });
  assert.equal(failure.length, 1);
  assert.equal(failure[0].route.id, "failed");
  assert.equal(failure[0].payload.title, "code.review failed");
  assert.equal(failure[0].payload.level, "serious");

  // The explanation arrives later and separately. Holding the failure until a
  // postmortem that may never be written would mean never reporting the ones
  // that matter most.
  const account = plan(routes, { jobId: "j1", owner: "ci", kind: "postmortem", service: "code.review",
    summary: "skipped files under db/", lesson: "read migrations before judging a schema change" });
  assert.equal(account.length, 1);
  assert.equal(account[0].route.id, "why");
  assert.equal(account[0].payload.body, "read migrations before judging a schema change");
});

t("a postmortem answers to {{type}} as well as to its kind", () => {
  // So an operator who wrote `when: { type: "postmortem" }` is not silently
  // matched by nothing.
  const ctx = deliveryContext({ jobId: "j1", owner: "ci", kind: "postmortem", lesson: "l" });
  assert.equal(ctx.type, "postmortem");
  assert.equal(ctx.kind, "postmortem");
});

// ── notify: matching ────────────────────────────────────
t("get reads dotted paths and survives missing branches", () => {
  assert.equal(get({ a: { b: { c: 1 } } }, "a.b.c"), 1);
  assert.equal(get({ a: null }, "a.b.c"), undefined);
  assert.equal(get({}, "nope.nope"), undefined);
});

t("conditions cover the documented operators", () => {
  assert.ok(testCondition("x", "x"));
  assert.ok(!testCondition("x", "y"));
  assert.ok(testCondition(["a", "b"], "b"));
  assert.ok(testCondition({ $ne: "APPROVE" }, "REQUEST_CHANGES"));
  assert.ok(testCondition({ $exists: true }, 0), "0 exists");
  assert.ok(testCondition({ $exists: false }, undefined));
  assert.ok(testCondition({ $re: "^request" }, "REQUEST_CHANGES"), "$re is case-insensitive");
  assert.ok(testCondition({ $in: ["high", "critical"] }, "high"));
  assert.ok(testCondition({ $gt: 5 }, 9));
  assert.ok(testCondition({ $contains: "lock" }, "Locks the table"));
});

t("an unknown operator matches nothing rather than everything", () => {
  // Failing open here would silently page the whole team on a typo.
  assert.equal(testCondition({ $regex: "x" }, "x"), false);
});

t("an empty when block is a catch-all", () => {
  assert.ok(matches({}, { anything: 1 }));
  assert.ok(matches(undefined, { anything: 1 }));
});

t("when blocks are an AND across keys", () => {
  const msg = { service: "code.review", verdict: "REQUEST_CHANGES" };
  assert.ok(matches({ service: "code.review", verdict: "REQUEST_CHANGES" }, msg));
  assert.equal(matches({ service: "code.review", verdict: "APPROVE" }, msg), false);
});

t("render fills dotted paths and blanks the missing ones", () => {
  const ctx = { owner: "alice", result: { risk: "high" } };
  assert.equal(render("{{owner}} → {{result.risk}}", ctx), "alice → high");
  assert.equal(render("[{{nope}}]", ctx), "[]", "a gap beats a visible placeholder");
  assert.equal(render("{{ owner }}", ctx), "alice", "whitespace inside braces is allowed");
});

t("envelope fields win a collision with handler output", () => {
  const ctx = deliveryContext({ jobId: "real", owner: "alice", kind: "result", type: "result", jobId2: 1 });
  assert.equal(ctx.jobId, "real");
});

t("result fields are lifted for templates but stay addressable under result.*", () => {
  const ctx = deliveryContext({ jobId: "j1", owner: "alice", kind: "result", verdict: "APPROVE" });
  assert.equal(ctx.verdict, "APPROVE");
  assert.equal(ctx.result.verdict, "APPROVE");
});

t("plan fires every matching route, and stop ends evaluation", () => {
  const routes = [
    { id: "audit", when: {}, to: "file" },
    { id: "page", when: { type: "error" }, to: ["slack"], stop: true },
    { id: "after", when: {}, to: "file" },
  ];
  const fired = plan(routes, { jobId: "j1", owner: "a", kind: "result", type: "error" });
  assert.deepEqual(fired.map((f) => f.route.id), ["audit", "page"]);
});

t("disabled routes never fire", () => {
  assert.equal(plan([{ id: "x", when: {}, to: "log", enabled: false }], { jobId: "j" }).length, 0);
});

t("plan renders titles and bodies from the message", () => {
  const [fired] = plan(
    [{ id: "r", when: {}, to: "log", title: "{{service}} for {{owner}}", body: "{{summary}}" }],
    { jobId: "j1", owner: "alice", kind: "result", service: "code.review", summary: "looks fine" });
  assert.equal(fired.payload.title, "code.review for alice");
  assert.equal(fired.payload.body, "looks fine");
});

// ── notify: channels ────────────────────────────────────
t("expandEnv substitutes and fails loudly on a missing variable", () => {
  assert.equal(expandEnv("a-${TOK}-b", { TOK: "x" }), "a-x-b");
  assert.throws(() => expandEnv("${MISSING_ONE}", {}), /not set in the environment/);
});

t("redact removes token shapes from anything log-bound", () => {
  assert.ok(!redact("ghp_abcdefghijklmnopqrstuvwxyz012345").includes("abcdefghij"));
  assert.ok(!redact("https://hooks.slack.com/services/T0/B0/xyzsecret").includes("xyzsecret"));
  assert.ok(!redact("xoxb-1234567890-abcdefghij").includes("abcdefghij"));
  assert.ok(!redact("https://x.test/cb?token=supersecret").includes("supersecret"));
});

t("an unknown channel type is rejected at load, not at delivery", () => {
  // Failing here means a typo surfaces on startup rather than during the
  // incident the notification was supposed to report.
  assert.throws(() => loadChannels({ oops: { type: "carrier-pigeon" } }), /unknown type/);
});

t("disabled channels are skipped", () => {
  assert.equal(loadChannels({ slack: { type: "slack", enabled: false } }).size, 0);
});

// ── end to end, if a broker is reachable ────────────────
const brokerUrl = process.env.PLEXUS_TEST_BROKER ?? "mqtt://localhost:1883";
const reachable = await new Promise((resolve) => {
  const { hostname, port } = new URL(brokerUrl);
  const sock = net.createConnection({ host: hostname, port: Number(port) || 1883 });
  const done = (ok) => { sock.destroy(); resolve(ok); };
  sock.on("connect", () => done(true));
  sock.on("error", () => done(false));
  setTimeout(() => done(false), 1500);
});

if (!reachable) {
  console.log(`\n⚠️  no broker at ${brokerUrl} — skipping end-to-end tests`);
  console.log(`   start one:  mosquitto -p 1883\n`);
} else {
  const root = `plexus-test-${Math.random().toString(36).slice(2, 8)}`;
  const statePath = join(tmpdir(), `${root}.state.json`);
  const auditPath = join(tmpdir(), `${root}.audit.jsonl`);

  t("end to end: delegation carries lineage and returns one combined result", async () => {
    const dba = await connect({ broker: brokerUrl, root, agentId: "dba" });
    let seenDepth = null, seenParent = null, seenRoot = null;
    dba.serve("schema.review", (job) => {
      seenDepth = job.depth; seenParent = job.parentJobId; seenRoot = job.rootJobId;
      return { risk: "high", finding: "locks writes" };
    });

    const reviewer = await connect({ broker: brokerUrl, root, agentId: "reviewer" });
    reviewer.serve("code.review", async (job, ctx) => {
      const answer = await ctx.askAny("schema.review", { migration: "m.sql" });
      return { verdict: "REQUEST_CHANGES", risk: answer.risk, contributedBy: ["dba"] };
    });

    const alice = await connect({ broker: brokerUrl, root, agentId: "alice", durable: false });
    await reviewer.waitForPeer("schema.review", 5000);

    const result = await alice.invoke("reviewer", "code.review", { repo: "acme/web", pr: 42 }, { jobId: "rev-1" });
    assert.equal(result.verdict, "REQUEST_CHANGES");
    assert.deepEqual(result.contributedBy, ["dba"]);

    assert.equal(seenDepth, 1, "a delegated job is one hop from the root");
    assert.equal(seenParent, "rev-1", "parentJobId links to the asking job");
    assert.equal(seenRoot, "rev-1", "rootJobId is the original request");

    await Promise.all([alice.close(), reviewer.close(), dba.close()]);
  });

  t("end to end: a top-level invoke starts at depth 0", async () => {
    const worker = await connect({ broker: brokerUrl, root: `${root}-d`, agentId: "worker" });
    let depth = null;
    worker.serve("noop", (job) => { depth = job.depth; return { ok: true }; });
    const caller = await connect({ broker: brokerUrl, root: `${root}-d`, agentId: "caller", durable: false });
    await caller.waitForPeer("noop", 5000);
    await caller.invoke("worker", "noop", {});
    assert.equal(depth, 0, "a request entering the mesh is depth 0, not 1");
    await Promise.all([caller.close(), worker.close()]);
  });

  t("end to end: a delegation cycle is stopped by the hop limit", async () => {
    // A→B→A. Without a hop limit this runs until something else stops it, so
    // the limit has to refuse on the asking side, before anything is published.
    const hRoot = `${root}-h`;
    const a = await connect({ broker: brokerUrl, root: hRoot, agentId: "a", maxDepth: 1 });
    const b = await connect({ broker: brokerUrl, root: hRoot, agentId: "b", maxDepth: 1 });
    a.serve("ping", async (job, ctx) => ctx.askAny("pong", {}));
    b.serve("pong", async (job, ctx) => ctx.askAny("ping", {}));
    await a.waitForPeer("pong", 5000);
    await b.waitForPeer("ping", 5000);

    const c = await connect({ broker: brokerUrl, root: hRoot, agentId: "c", durable: false });
    await c.waitForPeer("ping", 5000);

    const res = await c.invoke("a", "ping", {}, { timeoutMs: 10000 });
    assert.equal(res.type, "error");
    assert.match(res.error, /maxDepth/, "the cycle should be refused, not left to run");
    await Promise.all([c.close(), b.close(), a.close()]);
  });

  t("end to end: an unknown service is rejected with a terminal result", async () => {
    const a = await connect({ broker: brokerUrl, root: `${root}-u`, agentId: "a" });
    a.serve("known", () => ({ ok: true }));
    const c = await connect({ broker: brokerUrl, root: `${root}-u`, agentId: "c", durable: false });
    await c.waitForPeer("known", 5000);
    const res = await c.invoke("a", "nope.nope", {}, { timeoutMs: 8000 });
    assert.equal(res.type, "error");
    assert.match(res.error, /unknown service/);
    await Promise.all([c.close(), a.close()]);
  });

  t("end to end v1.4: an invoke carrying its owner in the topic is served", async () => {
    // The whole point of the form: the owner is a topic segment, so a broker
    // ACL can enforce it. Here we simply prove the agent answers it.
    const r = `${root}-v14`;
    const a = await connect({ broker: brokerUrl, root: r, agentId: "a" });
    a.serve("echo", (msg) => ({ heard: msg.args.what }));   // handlers get the whole invoke
    const c = await connect({ broker: brokerUrl, root: r, agentId: "ci", durable: false });
    await c.waitForPeer("echo", 5000);
    const res = await c.invoke("a", "echo", { what: "hello" }, { timeoutMs: 8000 });
    assert.equal(res.heard, "hello");
    await Promise.all([c.close(), a.close()]);
  });

  t("end to end v1.4: a payload that disagrees with the topic is refused", async () => {
    // Publishing by hand, because a well-behaved client never produces this —
    // and it is exactly what a dishonest one would.
    const r = `${root}-v14-mismatch`;
    const a = await connect({ broker: brokerUrl, root: r, agentId: "a" });
    a.serve("echo", () => ({ ok: true }));
    const spy = await connect({ broker: brokerUrl, root: r, agentId: "watcher", durable: false });

    const seen = new Promise((resolve) => {
      spy.watch((msg) => { if (msg?.type === "rejected") resolve(msg); }, "jobs");
    });
    await new Promise((r2) => setTimeout(r2, 300));
    spy.publish(topics.invokeAs(r, "a", "ci"),
      { service: "echo", args: {}, requestedBy: "mohanad", jobId: "mismatch-1" });

    const rejected = await Promise.race([
      seen,
      new Promise((_, rej) => setTimeout(() => rej(new Error("no rejection published")), 8000)),
    ]);
    assert.match(rejected.error, /disagrees with the invoke topic/);
    await Promise.all([spy.close(), a.close()]);
  });

  t("end to end v1.4: the v1.3 form is still served — refusing is the broker's job", async () => {
    // There is deliberately no mode in which an agent refuses the old form.
    // Where rules are applied, an ACL granting commands/+/invoke/<owner> does
    // not grant commands/+/invoke, so the old form cannot be published at all;
    // where they are not, refusing would block the careless and not the
    // dishonest. Either way it is not the agent's call.
    const r = `${root}-v14-both`;
    const a = await connect({ broker: brokerUrl, root: r, agentId: "a" });
    a.serve("echo", (msg) => ({ heard: msg.args.what }));
    const c = await connect({ broker: brokerUrl, root: r, agentId: "ci", durable: false });
    await c.waitForPeer("echo", 5000);

    // The client sees "accept" in the peer's profile and uses the topic form.
    const viaTopic = await c.invoke("a", "echo", { what: "new" }, { timeoutMs: 8000 });
    assert.equal(viaTopic.heard, "new");

    // And the v1.3 form, published deliberately, is served rather than refused.
    let resolveSeen;
    const seen = new Promise((resolve) => { resolveSeen = resolve; });
    // Awaited: watch() swaps the narrow filter for the wide one, and a message
    // published during that swap is lost — which looks exactly like the agent
    // having refused it. And `kind`, not `type`: events carry a type too, and
    // the first of them would resolve this before any answer exists.
    await c.watch((msg) => {
      if (msg?.jobId === "oldform-1" && msg.kind === "result") resolveSeen(msg);
    }, "jobs");
    c.publish(topics.invoke(r, "a"),
      { service: "echo", args: { what: "old" }, requestedBy: "ci", jobId: "oldform-1" });
    const answer = await Promise.race([
      seen,
      new Promise((_, rej) => setTimeout(() => rej(new Error("the v1.3 form was not served")), 8000)),
    ]);
    assert.equal(answer.heard, "old", "the old form must still be answered");
    await Promise.all([c.close(), a.close()]);
  });

  t("end to end v1.4: verified is what the deployment states, not what the agent infers", async () => {
    // The agent cannot observe that its broker scopes invoke topics. Whoever
    // applied the rules knows; plexus-server writes it into the config it
    // generates. So this field follows configuration, and nothing else.
    const r = `${root}-v14-verified`;
    const plain = await connect({ broker: brokerUrl, root: r, agentId: "plain" });
    const stated = await connect({ broker: brokerUrl, root: `${r}-2`, agentId: "stated", ownerEnforced: true });
    const watcher = await connect({ broker: brokerUrl, root: r, agentId: "w", durable: false });
    await watcher.waitForPeer(undefined, 3000).catch(() => {});
    await new Promise((r2) => setTimeout(r2, 500));

    assert.equal(watcher.peers().find((p) => p.agentId === "plain")?.ownerPolicy?.verified, false);
    // Read from the second mesh's own profile rather than across roots.
    const w2 = await connect({ broker: brokerUrl, root: `${r}-2`, agentId: "w2", durable: false });
    await new Promise((r2) => setTimeout(r2, 500));
    assert.equal(w2.peers().find((p) => p.agentId === "stated")?.ownerPolicy?.verified, true);

    await Promise.all([plain.close(), stated.close(), watcher.close(), w2.close()]);
  });

  t("end to end: a handler that throws still produces a terminal result", async () => {
    const a = await connect({ broker: brokerUrl, root: `${root}-e`, agentId: "a" });
    a.serve("boom", () => { throw new Error("kaboom"); });
    const c = await connect({ broker: brokerUrl, root: `${root}-e`, agentId: "c", durable: false });
    await c.waitForPeer("boom", 5000);
    const res = await c.invoke("a", "boom", {}, { timeoutMs: 8000 });
    assert.equal(res.type, "error");
    assert.match(res.error, /kaboom/);
    await Promise.all([c.close(), a.close()]);
  });

  t("end to end: closing withdraws the profile so no husk is left behind", async () => {
    const ghost = await connect({ broker: brokerUrl, root: `${root}-g`, agentId: "ghost" });
    ghost.serve("haunt", () => ({}));
    const watcher = await connect({ broker: brokerUrl, root: `${root}-g`, agentId: "watcher", durable: false });
    await watcher.waitForPeer("haunt", 5000);
    await ghost.close();
    await new Promise((r) => setTimeout(r, 400));
    assert.equal(watcher.find("haunt"), null, "a closed agent must leave the registry");
    await watcher.close();
  });

  /** A host running only the notify plugin, wired to a temp file channel. */
  const notifyHost = (agentId, hRoot) => createHost({
    broker: brokerUrl, root: hRoot, agentId,
    plugins: {
      "plexus-notify": {
        state: statePath, replayGraceMs: 300,
        channels: { audit: { type: "file", path: auditPath } },
        routes: [{ id: "changes", when: { verdict: "REQUEST_CHANGES" }, to: ["audit"],
                   title: "changes on {{args.repo}}", body: "{{summary}}" }],
      },
    },
  }, { log: () => {}, resolve: () => notifyPlugin });

  t("end to end: the notify plugin delivers a matching result exactly once", async () => {
    const hRoot = `${root}-notify`;
    const host = await notifyHost("notifier", hRoot).start();
    await new Promise((r) => setTimeout(r, 500));      // let the replay grace elapse

    const worker = await connect({ broker: brokerUrl, root: hRoot, agentId: "worker" });
    worker.serve("code.review", () => ({ verdict: "REQUEST_CHANGES", summary: "fix the migration" }));
    const client = await connect({ broker: brokerUrl, root: hRoot, agentId: "alice", durable: false });
    await client.waitForPeer("code.review", 5000);
    await client.invoke("worker", "code.review", { repo: "acme/web" }, { jobId: "hj-1" });
    await new Promise((r) => setTimeout(r, 600));

    const lines = (await readFile(auditPath, "utf8")).trim().split("\n").filter(Boolean).map(JSON.parse);
    assert.equal(lines.length, 1, `expected exactly one delivery, got ${lines.length}`);
    // `args` comes from the invoke, not the result — proof the plugin joins the
    // question to the answer rather than rendering a half-empty message.
    assert.equal(lines[0].title, "changes on acme/web");
    assert.equal(lines[0].body, "fix the migration");
    assert.equal(lines[0].service, "code.review");

    await Promise.all([client.close(), worker.close(), host.stop()]);
  });

  t("end to end: a restarted host does not re-deliver the retained backlog", async () => {
    // The failure this guards against: results are retained, so every restart
    // re-receives the entire history. Without suppression, restarting the
    // notifier pages everyone about work that finished days ago.
    const host = await notifyHost("notifier2", `${root}-notify`).start();
    await new Promise((r) => setTimeout(r, 900));      // retained flush + grace

    const lines = (await readFile(auditPath, "utf8")).trim().split("\n").filter(Boolean);
    assert.equal(lines.length, 1, `retained result was re-delivered ${lines.length} times`);
    assert.ok(host.handle("notify").stats.suppressed >= 1, "the retained result should count as suppressed");

    await host.stop();
    await rm(statePath, { force: true });
    await rm(auditPath, { force: true });
  });

  t("end to end: one host serves the capabilities of every plugin it loads", async () => {
    // The reason plugins beat separate processes: two specialisms, one agent,
    // one registry entry, one durable session.
    const hRoot = `${root}-multi`;
    const a = definePlugin({ name: "alpha", setup: (agent) => { agent.serve("alpha.do", () => ({ from: "alpha" })); } });
    const b = definePlugin({ name: "beta", setup: (agent) => { agent.serve("beta.do", () => ({ from: "beta" })); } });

    const host = await createHost(
      { broker: brokerUrl, root: hRoot, agentId: "multi", plugins: { a: {}, b: {} } },
      { log: () => {}, resolve: (spec) => (spec === "a" ? a : b) },
    ).start();

    const client = await connect({ broker: brokerUrl, root: hRoot, agentId: "caller", durable: false });
    await client.waitForPeer("beta.do", 5000);

    const profile = client.peers().find((p) => p.agentId === "multi");
    assert.deepEqual(profile.capabilities.map((c) => c.service).sort(), ["alpha.do", "beta.do"]);
    assert.equal((await client.invoke("multi", "alpha.do", {})).from, "alpha");
    assert.equal((await client.invoke("multi", "beta.do", {})).from, "beta");

    await client.close();
    await host.stop();
  });

  t("end to end: a plugin that fails to start takes the agent down with it", async () => {
    // Half-loading leaves an agent advertising capabilities it cannot serve,
    // which is worse than not starting at all.
    const broken = definePlugin({ name: "broken", setup: () => { throw new Error("no credentials"); } });
    const host = createHost(
      { broker: brokerUrl, root: `${root}-broken`, agentId: "broken", plugins: { broken: {} } },
      { log: () => {}, resolve: () => broken },
    );
    await assert.rejects(() => host.start(), /broken.*failed to start.*no credentials/s);
  });
}

// ── the plugin contract ─────────────────────────────────
t("definePlugin rejects a malformed plugin at declaration", () => {
  assert.throws(() => definePlugin({ setup() {} }), /name/);
  assert.throws(() => definePlugin({ name: "x" }), /setup/);
  assert.equal(definePlugin({ name: "x", setup() {} }).name, "x");
});

t("a plugin specifier set to false is skipped entirely", async () => {
  let loaded = false;
  const host = createHost(
    { broker: "mqtt://unused", agentId: "x", plugins: { skipped: false, off: { enabled: false } } },
    { log: () => {}, connect: async () => ({ capabilities: () => [], close: async () => {} }),
      resolve: () => { loaded = true; return definePlugin({ name: "n", setup() {} }); } },
  );
  await host.start();
  assert.equal(loaded, false, "a disabled plugin must not even be imported");
  await host.stop();
});

t("a module that is not a plugin is rejected with a useful message", async () => {
  const host = createHost(
    { broker: "mqtt://unused", agentId: "x", plugins: { "some-package": {} } },
    { log: () => {}, connect: async () => ({ capabilities: () => [], close: async () => {} }),
      resolve: async () => ({ default: { nope: true } }) },
  );
  await assert.rejects(() => host.start(), /not a Plexus plugin/);
});

for (const run of queue) await run();
console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
