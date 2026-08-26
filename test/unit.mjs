/**
 * Unit tests for the mesh modules.
 *
 * These import the BUILT output, so they also prove the ESM graph resolves —
 * a missing .js extension in an import fails here rather than at gateway start.
 */

import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const dist = (p) => new URL(`../dist/${p}`, import.meta.url).href;

const { ownerScope, buildTopics, jobTopicPattern, parseJobTopic, escapeRe } = await import(dist("mesh/topics.js"));
const { normalizeJobPublish, renderPrompt, unresolvedPlaceholders, publishRefusal } = await import(dist("mesh/payload.js"));
const { createJobStore, MAX_HISTORY } = await import(dist("mesh/jobs.js"));
const { createVarStore, maskValue } = await import(dist("mesh/vars.js"));
const { createAuth } = await import(dist("http/auth.js"));
const { resolveConfig, resolveEnvRef, DEFAULTS } = await import(dist("config.js"));
const { deriveClientId } = await import(dist("mesh/transport.js"));

let pass = 0, fail = 0;
const queue = [];
// Collected then awaited in order, so async assertions cannot report after the
// summary has already printed.
function t(name, fn) {
  queue.push(async () => {
    try { await fn(); console.log(`✅ ${name}`); pass++; }
    catch (e) { console.log(`❌ ${name}\n     ${e.message}`); fail++; }
  });
}
const quietLogger = { info() {}, warn() {}, error() {}, alert() {} };

// ── topics ──────────────────────────────────────────────
t("ownerScope sanitises to the documented charset", () => {
  assert.equal(ownerScope("Mohanad.Q!"), "mohanad-q");
  assert.equal(ownerScope("--ci--"), "ci");
});
t("ownerScope falls back to public", () => {
  assert.equal(ownerScope(""), "public");
  assert.equal(ownerScope(undefined), "public");
});
t("buildTopics derives the whole address space from root + agentId", () => {
  const tp = buildTopics("agents", "conan");
  assert.equal(tp.invoke, "agents/commands/conan/invoke");
  assert.equal(tp.profile, "agents/registry/conan/profile");
});
t("escapeRe neutralises regex metacharacters in a mesh root", () => {
  assert.ok(jobTopicPattern("a.b").test("a.b/jobs/o/j/result"));
  assert.ok(!jobTopicPattern("a.b").test("axb/jobs/o/j/result"));
});
t("job topics are always owner-scoped — the v1.0 flat form never matches", () => {
  const re = jobTopicPattern("agents");
  assert.ok(re.test("agents/jobs/alice/job-1/result"));
  assert.equal(re.test("agents/jobs/job-1/result"), false);
});
t("parseJobTopic decodes owner, id and kind", () => {
  const p = parseJobTopic(jobTopicPattern("agents"), "agents/jobs/alice/job-1/events");
  assert.deepEqual(p, { owner: "alice", jobId: "job-1", kind: "events" });
});

// ── payload ─────────────────────────────────────────────
const RE = jobTopicPattern("agents");
t("results are forced retained, even against an explicit false", () => {
  assert.equal(normalizeJobPublish(RE, "agents/jobs/o/j/result", "{}").retain, true);
  assert.equal(normalizeJobPublish(RE, "agents/jobs/o/j/result", "{}", false).retain, true);
});
t("events are never retained", () => {
  assert.equal(normalizeJobPublish(RE, "agents/jobs/o/j/events", "{}").retain, false);
});
t("required fields are injected from the topic", () => {
  const o = JSON.parse(normalizeJobPublish(RE, "agents/jobs/alice/job-9/result", '{"verdict":"APPROVE"}').payload);
  assert.equal(o.jobId, "job-9");
  assert.equal(o.owner, "alice");
  assert.equal(o.type, "result");
  assert.equal(o.verdict, "APPROVE");
  assert.ok(!Number.isNaN(Date.parse(o.ts)));
});
t("explicit fields are never clobbered", () => {
  const o = JSON.parse(normalizeJobPublish(RE, "agents/jobs/o/j/result", '{"type":"review","owner":"keep"}').payload);
  assert.equal(o.type, "review");
  assert.equal(o.owner, "keep");
});
t("non-object payloads are wrapped, not dropped", () => {
  assert.equal(JSON.parse(normalizeJobPublish(RE, "agents/jobs/o/j/result", "not json").payload).note, "not json");
  assert.equal(JSON.parse(normalizeJobPublish(RE, "agents/jobs/o/j/result", "[1,2]").payload).value.length, 2);
});
t("non-job topics pass through untouched", () => {
  const r = normalizeJobPublish(RE, "agents/registry/x/profile", "raw");
  assert.equal(r.payload, "raw");
  assert.equal(r.retain, false);
});

// ── prompt rendering: the injection guard ───────────────
t("env expands before args, so an arg cannot smuggle ${SECRET}", () => {
  const vars = (k) => (k === "SECRET" ? "hunter2" : "");
  const out = renderPrompt("Review {{repo}}", vars, "j1", "alice", { repo: "${SECRET}" });
  assert.ok(!out.includes("hunter2"), "arg-borne ${} must not expand");
  assert.ok(out.includes("${SECRET}"));
});
t("placeholders and vars both render", () => {
  const out = renderPrompt("DM ${WHO} about {{pr}} in {{repo}} (job {{jobId}})",
    () => "U123", "j1", "alice", { pr: 7, repo: "acme/app" });
  assert.ok(out.includes("U123") && out.includes("7") && out.includes("acme/app") && out.includes("j1"));
});

// ── history outlives the process ────────────────────────
const flushed = () => new Promise((r) => setTimeout(r, 600));   // the store debounces its writes

t("job history is restored after a restart", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "plexus-hist-"));
  const file = path.join(dir, "jobs.local.json");

  const first = createJobStore(() => {}, { file });
  first.record({ jobId: "ci-1", service: "code.review", owner: "ci", state: "started" },
    { type: "accepted" });
  first.record({ jobId: "ci-1", state: "done", result: { verdict: "APPROVE" } }, { type: "review" });
  await flushed();

  const restarted = createJobStore(() => {}, { file });
  const rec = restarted.find("ci-1");
  assert.ok(rec, "the job survived the restart");
  assert.equal(rec.service, "code.review", "and kept its service, which a retained result does not carry");
  assert.equal(rec.state, "done");
  assert.equal(rec.events.length, 2, "and kept its timeline");
  fs.rmSync(dir, { recursive: true, force: true });
});

t("a replayed retained result merges into the real record, it does not flatten it", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "plexus-hist-"));
  const file = path.join(dir, "jobs.local.json");

  const before = createJobStore(() => {}, { file });
  before.record({ jobId: "ci-2", service: "build.diagnose", owner: "ci", state: "started" },
    { type: "started" });
  await flushed();

  // What the broker replays on resubscribe: the answer, and nothing else.
  const after = createJobStore(() => {}, { file });
  after.record({ jobId: "ci-2", state: "done", result: { type: "diagnosis" } }, { type: "result" });
  const rec = after.find("ci-2");
  assert.equal(rec.service, "build.diagnose", "service is not lost to the replay");
  assert.equal(rec.events.length, 2);
});

t("the ring stays bounded across restarts", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "plexus-hist-"));
  const file = path.join(dir, "jobs.local.json");
  const s1 = createJobStore(() => {}, { file });
  for (let i = 0; i < MAX_HISTORY + 25; i++) s1.record({ jobId: `j${i}`, state: "done" });
  await flushed();
  const s2 = createJobStore(() => {}, { file });
  assert.equal(s2.history().length, MAX_HISTORY);
  fs.rmSync(dir, { recursive: true, force: true });
});

t("a corrupt history file starts empty rather than throwing", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "plexus-hist-"));
  const file = path.join(dir, "jobs.local.json");
  fs.writeFileSync(file, "{ not json");
  const said = [];
  const s = createJobStore(() => {}, { file, log: (m) => said.push(m) });
  assert.equal(s.history().length, 0);
  assert.ok(said.some((m) => /unreadable/.test(m)), "and says so");
  fs.rmSync(dir, { recursive: true, force: true });
});

t("with no file configured the store still works, just without persistence", () => {
  const s = createJobStore(() => {});
  s.record({ jobId: "x", state: "done" });
  assert.equal(s.history().length, 1);
});

// ── terminal really is terminal ─────────────────────────
t("a second result for a finished job is refused — retained means last wins", () => {
  const why = publishRefusal("result", { cancelled: false, finished: true }, "rev-030");
  assert.ok(why && /already published a terminal result/.test(why));
});

t("trailing milestones after completion are allowed — events are not retained", () => {
  assert.equal(publishRefusal("events", { cancelled: false, finished: true }, "rev-030"), null);
});

t("a cancelled job refuses everything, milestones included", () => {
  assert.ok(publishRefusal("result", { cancelled: true, finished: false }, "j1"));
  assert.ok(publishRefusal("events", { cancelled: true, finished: false }, "j1"));
});

t("a job still running publishes freely", () => {
  assert.equal(publishRefusal("result", { cancelled: false, finished: false }, "j1"), null);
  assert.equal(publishRefusal("events", { cancelled: false, finished: false }, "j1"), null);
});

t("a topic that is not job traffic is never refused", () => {
  assert.equal(publishRefusal(null, { cancelled: true, finished: true }, "j1"), null);
});

// ── prompts that render with holes in them ──────────────
const bound = (names) => (k) => names.includes(k);

t("a required argument that never arrived is reported", () => {
  const holes = unresolvedPlaceholders(
    "Review {{pr}} in {{repo}}", { repo: "acme/app" }, bound([]), { repo: "string", pr: "number" });
  assert.deepEqual(holes, ["{{pr}}"]);
});

t("an omitted optional argument is not a hole", () => {
  const holes = unresolvedPlaceholders(
    "Review {{pr}} in {{repo}}. Focus: {{focus}}",
    { repo: "acme/app", pr: 42 }, bound([]),
    { repo: "string", pr: "number", focus: "string? (optional)" });
  assert.deepEqual(holes, []);
});

t("a placeholder the schema never declared is a hole", () => {
  const holes = unresolvedPlaceholders("Ship {{version}}", {}, bound([]), {});
  assert.deepEqual(holes, ["{{version}}"]);
});

t("an unbound variable is reported, a bound one is not", () => {
  const holes = unresolvedPlaceholders(
    "DM ${SLACK_REVIEW_RECIPIENTS} and ${MISSING}", {}, bound(["SLACK_REVIEW_RECIPIENTS"]), {});
  assert.deepEqual(holes, ["${MISSING}"]);
});

t("jobId and requestedBy are injected, never counted as missing args", () => {
  const holes = unresolvedPlaceholders("job {{jobId}} for {{requestedBy}}", {}, bound([]), {});
  assert.deepEqual(holes, []);
});

t("a falsy argument that was actually supplied is not a hole", () => {
  const holes = unresolvedPlaceholders("PR {{pr}}", { pr: 0 }, bound([]), { pr: "number" });
  assert.deepEqual(holes, []);
});

// ── job store ───────────────────────────────────────────
t("job store stamps finishedAt once and keeps a timeline", () => {
  const seen = [];
  const s = createJobStore((r) => seen.push(r.state));
  s.record({ jobId: "j", state: "accepted" }, { type: "accepted" });
  s.record({ jobId: "j", state: "started" }, { type: "started" });
  s.record({ jobId: "j", state: "done" }, { type: "result" });
  const rec = s.find("j");
  assert.equal(rec.events.length, 3);
  const first = rec.finishedAt;
  s.record({ jobId: "j", state: "done" });          // a late duplicate publish
  assert.equal(rec.finishedAt, first, "finishedAt must not restart the clock");
  assert.deepEqual(seen.slice(0, 3), ["accepted", "started", "done"]);
});
t("job store bounds its history ring", () => {
  const s = createJobStore(() => {});
  for (let i = 0; i < 150; i++) s.record({ jobId: `j${i}`, state: "done" });
  assert.equal(s.history().length, 100);
  assert.equal(s.find("j0"), undefined, "oldest entries are evicted");
  assert.equal(s.recent()[0].jobId, "j149", "recent() is newest first");
});

// ── variable store: precedence and secrecy ──────────────
t("variables resolve config > local > env, and never expose values", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-vars-"));
  const file = path.join(dir, "mesh.local.json");
  process.env.MESH_TEST_ENV = "from-env";
  const store = createVarStore(file, { PINNED: "from-config" }, quietLogger);

  assert.equal(store.set("LOCAL_ONE", "from-local"), null);
  assert.equal(store.value("PINNED"), "from-config");
  assert.equal(store.value("LOCAL_ONE"), "from-local");
  assert.equal(store.value("MESH_TEST_ENV"), "from-env");
  assert.equal(store.source("PINNED"), "config");
  assert.equal(store.source("LOCAL_ONE"), "local");
  assert.equal(store.source("MESH_TEST_ENV"), "env");
  assert.equal(store.source("NOPE"), "unset");

  // config wins, and the panel must refuse to shadow it rather than lose silently
  assert.ok(store.set("PINNED", "x")?.includes("pinned"));
  assert.equal(store.value("PINNED"), "from-config");

  const described = JSON.stringify(store.describe());
  assert.ok(!described.includes("from-local"), "describe() must not leak values");
  assert.ok(!described.includes("from-config"), "describe() must not leak values");

  assert.equal(store.remove("LOCAL_ONE"), null);
  assert.equal(store.value("LOCAL_ONE"), undefined);
  delete process.env.MESH_TEST_ENV;
  fs.rmSync(dir, { recursive: true, force: true });
});
t("the local variable file is written 0600", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-perm-"));
  const file = path.join(dir, "mesh.local.json");
  createVarStore(file, {}, quietLogger).set("K", "v");
  assert.equal(fs.statSync(file).mode & 0o777, 0o600);
  fs.rmSync(dir, { recursive: true, force: true });
});
t("masking reveals only a tail", () => {
  assert.equal(maskValue("U0EXAMPLE42"), "••••HC6B");
  assert.equal(maskValue("short"), "••••");
  assert.equal(maskValue(""), "");
});

// ── auth and CSRF ───────────────────────────────────────
const reqWith = (headers = {}) => ({ headers });
const urlNo = new URL("http://x/api/status");
const urlTok = (t) => new URL(`http://x/api/status?token=${t}`);

t("with no token configured, data is open but secrets are not", () => {
  const a = createAuth("");
  assert.equal(a.authorized(reqWith(), urlNo), true);
  assert.equal(a.elevated(reqWith(), urlNo), false, "secrets must fail closed");
  assert.equal(a.configured, false);
});
t("with a token, both header and query forms are accepted", () => {
  const a = createAuth("s3cret");
  assert.equal(a.authorized(reqWith(), urlNo), false);
  assert.equal(a.authorized(reqWith({ authorization: "Bearer s3cret" }), urlNo), true);
  assert.equal(a.authorized(reqWith(), urlTok("s3cret")), true, "EventSource cannot set headers");
  assert.equal(a.authorized(reqWith(), urlTok("wrong")), false);
  assert.equal(a.elevated(reqWith({ authorization: "Bearer s3cret" }), urlNo), true);
});
t("CSRF: the panel header is required and a cross-site origin is refused", () => {
  const a = createAuth("");
  assert.equal(a.sameOrigin(reqWith()), false, "missing header must be refused");
  assert.equal(a.sameOrigin(reqWith({ "x-mesh-panel": "1" })), true, "non-browser client");
  assert.equal(a.sameOrigin(reqWith({ "x-mesh-panel": "1", origin: "http://127.0.0.1:8765", host: "127.0.0.1:8765" })), true);
  assert.equal(a.sameOrigin(reqWith({ "x-mesh-panel": "1", origin: "https://evil.example", host: "127.0.0.1:8765" })), false);
});

// ── config ──────────────────────────────────────────────
t("config defaults are applied, and booleans default the documented way", () => {
  const c = resolveConfig({ broker: { url: "mqtt://h:1883" } }, "/plugins/mesh");
  assert.equal(c.mesh.root, DEFAULTS.meshRoot);
  assert.equal(c.mesh.requireOwner, true, "requireOwner defaults ON");
  assert.equal(c.mesh.verifyOwner, false, "verifyOwner defaults OFF (needs broker setup)");
  assert.equal(c.web.enabled, true);
  assert.equal(c.web.auth, "");
  assert.equal(c.broker.keepalive, DEFAULTS.keepalive);
});
t("explicit config overrides defaults", () => {
  const c = resolveConfig({
    broker: { url: "mqtt://h:1883", keepalive: 15, protocolVersion: 5 },
    mesh: { root: "acme/agents", agentId: "conan", requireOwner: false },
    web: { enabled: false, port: 9000, auth: "  tok  " },
  }, "/p");
  assert.equal(c.mesh.root, "acme/agents");
  assert.equal(c.mesh.requireOwner, false);
  assert.equal(c.web.enabled, false);
  assert.equal(c.web.port, 9000);
  assert.equal(c.web.auth, "tok", "auth is trimmed");
  assert.equal(c.broker.protocolVersion, 5);
});
t("${ENV_VAR} indirection resolves broker credentials", () => {
  process.env.MESH_PW = "hunter2";
  assert.equal(resolveEnvRef("${MESH_PW}"), "hunter2");
  assert.equal(resolveEnvRef("literal"), "literal");
  assert.equal(resolveEnvRef(undefined), undefined);
  delete process.env.MESH_PW;
});

// ── client id: the durability invariant ─────────────────
t("clientId is stable across calls and independent of the pid", () => {
  const a = deriveClientId("/plugins/mesh");
  const b = deriveClientId("/plugins/mesh");
  assert.equal(a, b, "a changing id defeats clean:false and loses offline jobs");
  assert.ok(!a.includes(String(process.pid)));
});
t("clientId differs per install path, and honours an explicit override", () => {
  assert.notEqual(deriveClientId("/plugins/a"), deriveClientId("/plugins/b"));
  assert.equal(deriveClientId("/plugins/a", "mine"), "mine");
});


// ── peer registry ───────────────────────────────────────
const { createPeerRegistry } = await import(dist("mesh/peers.js"));

t("peers are learned from retained profiles, and self is ignored", () => {
  const r = createPeerRegistry("me", quietLogger, () => {});
  r.onProfile("me", { capabilities: [{ service: "x" }] });
  assert.equal(r.size, 0, "our own profile comes back on the wildcard and must be skipped");
  r.onProfile("dba", { displayName: "DBA", capabilities: [{ service: "schema.review" }] });
  assert.equal(r.size, 1);
  assert.equal(r.get("dba").capabilities[0].service, "schema.review");
});
t("an agent that clears its retained profile leaves the mesh", () => {
  const r = createPeerRegistry("me", quietLogger, () => {});
  r.onProfile("gone", { capabilities: [{ service: "s" }] });
  assert.equal(r.size, 1);
  // An empty retained payload parses to null: the agent deleted its profile.
  r.onProfile("gone", null);
  assert.equal(r.size, 0, "a departed agent must not linger as an empty row");
  assert.equal(r.get("gone"), undefined);
});
t("clearing status does not resurrect a peer that already left", () => {
  const r = createPeerRegistry("me", quietLogger, () => {});
  r.onProfile("gone", { capabilities: [{ service: "s" }] });
  r.onProfile("gone", null);      // profile cleared -> removed
  r.onStatus("gone", null);       // status cleared -> must NOT recreate it
  assert.equal(r.size, 0, "a cleared status must not revive a departed agent");
});
t("presence tracks status messages", () => {
  const r = createPeerRegistry("me", quietLogger, () => {});
  r.onProfile("dba", { capabilities: [{ service: "s" }] });
  r.onStatus("dba", { status: "online" });
  assert.equal(r.get("dba").online, true);
  r.onStatus("dba", { status: "offline" });
  assert.equal(r.get("dba").online, false);
});
t("providersOf finds agents by capability, online first", () => {
  const r = createPeerRegistry("me", quietLogger, () => {});
  r.onProfile("a", { capabilities: [{ service: "review" }] });
  r.onProfile("b", { capabilities: [{ service: "review" }] });
  r.onProfile("c", { capabilities: [{ service: "other" }] });
  r.onStatus("b", { status: "online" });
  const found = r.providersOf("review");
  assert.equal(found.length, 2);
  assert.equal(found[0].agentId, "b", "online peers rank first");
  assert.equal(r.providersOf("nothing").length, 0);
});
t("summary lists only online peers with capabilities", () => {
  const r = createPeerRegistry("me", quietLogger, () => {});
  r.onProfile("dba", { capabilities: [{ service: "schema.review" }] });
  assert.match(r.summary(), /No other agents/);
  r.onStatus("dba", { status: "online" });
  assert.match(r.summary(), /dba: schema\.review/);
});

// ── delegation ──────────────────────────────────────────
const { createAskService } = await import(dist("mesh/ask.js"));

function askHarness(opts = {}) {
  const published = [];
  const peers = new Map(Object.entries(opts.peers ?? {
    dba: { agentId: "dba", online: true, capabilities: [{ service: "schema.review" }], lastSeen: 0 },
  }));
  const svc = createAskService({
    selfAgentId: "conan",
    meshRoot: "agents",
    maxDepth: opts.maxDepth ?? 4,
    timeoutMs: opts.timeoutMs ?? 50,
    logger: quietLogger,
    publish: (topic, payload) => published.push({ topic, payload: JSON.parse(payload) }),
    peer: (id) => peers.get(id),
    lineageOf: opts.lineageOf ?? (() => ({ depth: 0 })),
    onDelegated: () => {},
  });
  return { svc, published };
}

t("ask publishes to the peer's invoke topic with us as requester", async () => {
  const { svc, published } = askHarness();
  const p = svc.ask({ agent: "dba", service: "schema.review", args: { m: "1" }, parentJobId: "rev-1" });
  assert.equal(published.length, 1);
  assert.equal(published[0].topic, "agents/commands/dba/invoke");
  const body = published[0].payload;
  assert.equal(body.requestedBy, "conan", "results must route back to us");
  assert.equal(body.parentJobId, "rev-1");
  assert.equal(body.depth, 1);
  svc.settle(body.jobId, { type: "review", verdict: "OK" });
  const out = await p;
  assert.equal(out.ok, true);
  assert.equal(out.result.verdict, "OK");
});
t("a terminal result resolves the waiting ask", async () => {
  const { svc, published } = askHarness();
  const p = svc.ask({ agent: "dba", service: "schema.review" });
  const jobId = published[0].payload.jobId;
  assert.equal(svc.pendingCount, 1);
  assert.equal(svc.settle(jobId, { type: "review", ok: 1 }), true);
  assert.equal(svc.settle(jobId, { type: "review" }), false, "settling twice is a no-op");
  await p;
  assert.equal(svc.pendingCount, 0);
});
t("an error result surfaces as a failed ask, not a hang", async () => {
  const { svc, published } = askHarness();
  const p = svc.ask({ agent: "dba", service: "schema.review" });
  svc.settle(published[0].payload.jobId, { type: "error", error: "boom" });
  const out = await p;
  assert.equal(out.ok, false);
  assert.match(out.error, /boom/);
});
t("an unanswered ask times out rather than hanging forever", async () => {
  const { svc } = askHarness({ timeoutMs: 30 });
  const out = await svc.ask({ agent: "dba", service: "schema.review" });
  assert.equal(out.ok, false);
  assert.match(out.error, /did not answer/);
  assert.equal(svc.pendingCount, 0);
});
t("asking yourself is refused", async () => {
  const { svc } = askHarness();
  const out = await svc.ask({ agent: "conan", service: "schema.review" });
  assert.equal(out.ok, false);
  assert.match(out.error, /cannot ask yourself/);
});
t("asking an unknown agent, or for a capability it lacks, is refused", async () => {
  const { svc } = askHarness();
  assert.match((await svc.ask({ agent: "ghost", service: "x" })).error, /unknown agent/);
  assert.match((await svc.ask({ agent: "dba", service: "nope" })).error, /does not offer/);
});
t("the hop limit stops a cycle before it is published", async () => {
  const { svc, published } = askHarness({ maxDepth: 2, lineageOf: () => ({ depth: 2 }) });
  const out = await svc.ask({ agent: "dba", service: "schema.review", parentJobId: "j" });
  assert.equal(out.ok, false);
  assert.match(out.error, /depth limit/);
  assert.equal(published.length, 0, "nothing may go on the wire once the limit is hit");
});
t("cancelling a parent cancels what it delegated, and tells the peer", async () => {
  const { svc, published } = askHarness({ timeoutMs: 5000 });
  const p = svc.ask({ agent: "dba", service: "schema.review", parentJobId: "rev-9" });
  assert.equal(svc.childrenOf("rev-9").length, 1);
  const n = svc.cancelChildren("rev-9", "alice");
  assert.equal(n, 1);
  const cancelMsg = published.find((m) => m.topic === "agents/commands/dba/cancel");
  assert.ok(cancelMsg, "the peer must be told to stop");
  const out = await p;
  assert.equal(out.ok, false);
  assert.match(out.error, /cancelled/);
  assert.equal(svc.pendingCount, 0);
});


// ── delegation modes ────────────────────────────────────
const { createDispatcher } = await import(dist("mesh/dispatch.js"));

function dispatchHarness({ delegation = "both", delegates, askResult } = {}) {
  const published = [];
  const asked = [];
  const runs = [];
  const cfg = resolveConfig({
    broker: { url: "mqtt://x:1883" },
    mesh: { root: "agents", agentId: "conan", delegation },
  }, "/p");
  const jobs = createJobStore(() => {});
  const dispatcher = createDispatcher({
    cfg, logger: quietLogger, jobs,
    catalog: { read: () => ({ capabilities: [{ service: "code.review", prompt: "Review {{repo}}.", ...(delegates ? { delegates } : {}) }] }) },
    vars: { value: () => "" },
    runtime: {
      subagent: {
        run: async ({ message }) => { runs.push(message); return { runId: "r1" }; },
        waitForRun: async () => new Promise(() => {}),   // never settles during the test
      },
      system: { enqueueSystemEvent: () => {}, runHeartbeatOnce: async () => ({ status: "ran" }) },
    },
    publish: (topic, payload) => published.push({ topic, payload: JSON.parse(payload) }),
    peerSummary: () => "- dba: schema.review",
    performAsk: async (req) => {
      asked.push(req);
      return askResult ?? { ok: true, jobId: "ask-1", agent: req.agent, result: { verdict: "LGTM" } };
    },
  });
  return { dispatcher, published, asked, runs, jobs };
}
const settle = () => new Promise((r) => setTimeout(r, 30));

t("declared delegation runs BEFORE the executor, and injects the answer", async () => {
  const h = dispatchHarness({
    delegates: [{ agent: "dba", service: "schema.review", as: "schemaReview", args: { migration: "{{file}}" } }],
  });
  h.dispatcher.dispatch({ service: "code.review", requestedBy: "alice", args: { repo: "acme/app", file: "001.sql" } });
  await settle();
  assert.equal(h.asked.length, 1, "the bridge must perform the declared ask itself");
  assert.equal(h.asked[0].agent, "dba");
  assert.equal(h.asked[0].args.migration, "001.sql", "{{arg}} placeholders fill from the parent job");
  assert.equal(h.runs.length, 1, "the executor starts once, after delegation");
  assert.match(h.runs[0], /CONTEXT FROM OTHER AGENTS/);
  assert.match(h.runs[0], /schemaReview — answered by dba/);
  assert.match(h.runs[0], /LGTM/);
});
t("with no declared dependencies the executor starts immediately", async () => {
  const h = dispatchHarness();
  h.dispatcher.dispatch({ service: "code.review", requestedBy: "alice", args: {} });
  await settle();
  assert.equal(h.asked.length, 0);
  assert.equal(h.runs.length, 1);
  assert.ok(!/CONTEXT FROM OTHER AGENTS/.test(h.runs[0]));
});
t("delegation:dynamic ignores declared dependencies", async () => {
  const h = dispatchHarness({
    delegation: "dynamic",
    delegates: [{ agent: "dba", service: "schema.review", as: "x" }],
  });
  h.dispatcher.dispatch({ service: "code.review", requestedBy: "alice", args: {} });
  await settle();
  assert.equal(h.asked.length, 0, "declared must not run in dynamic-only mode");
  assert.equal(h.runs.length, 1);
});
t("delegation:off disables declared delegation and hides the peer directory", async () => {
  const h = dispatchHarness({
    delegation: "off",
    delegates: [{ agent: "dba", service: "schema.review", as: "x" }],
  });
  h.dispatcher.dispatch({ service: "code.review", requestedBy: "alice", args: {} });
  await settle();
  assert.equal(h.asked.length, 0);
  assert.ok(!/DELEGATION:/.test(h.runs[0]), "the briefing must not offer a tool that is disabled");
});
t("delegation:declared still gathers dependencies but hides the tool", async () => {
  const h = dispatchHarness({
    delegation: "declared",
    delegates: [{ agent: "dba", service: "schema.review", as: "x" }],
  });
  h.dispatcher.dispatch({ service: "code.review", requestedBy: "alice", args: {} });
  await settle();
  assert.equal(h.asked.length, 1);
  assert.ok(!/DELEGATION:/.test(h.runs[0]), "mesh_ask is not available, so do not advertise it");
});
t("an optional delegation that fails does not stop the job", async () => {
  const h = dispatchHarness({
    delegates: [{ agent: "dba", service: "schema.review", as: "x" }],
    askResult: { ok: false, jobId: "a", agent: "dba", error: "offline" },
  });
  h.dispatcher.dispatch({ service: "code.review", requestedBy: "alice", args: {} });
  await settle();
  assert.equal(h.runs.length, 1, "the executor still runs");
  assert.match(h.runs[0], /could not answer/, "and is told the dependency failed");
});
t("a required delegation that fails fails the job before the executor runs", async () => {
  const h = dispatchHarness({
    delegates: [{ agent: "dba", service: "schema.review", as: "x", required: true }],
    askResult: { ok: false, jobId: "a", agent: "dba", error: "offline" },
  });
  const r = h.dispatcher.dispatch({ service: "code.review", requestedBy: "alice", args: {} });
  await settle();
  assert.equal(h.runs.length, 0, "the executor must not start without a required dependency");
  const rec = h.jobs.find(r.jobId);
  assert.equal(rec.state, "error");
  const terminal = h.published.filter((m) => m.topic.endsWith("/result"));
  assert.ok(terminal.length, "a terminal result must still be published so nobody waits forever");
  assert.match(terminal.at(-1).payload.error, /required delegation/);
});

// Ask timeouts are unref'd so a pending delegation never keeps the gateway
// alive. In a bare test process that means the loop can drain before one
// fires, so hold it open for the duration of the run.
const keepAlive = setInterval(() => {}, 1000);
for (const run of queue) await run();
clearInterval(keepAlive);

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
