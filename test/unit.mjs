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
const { normalizeJobPublish, renderPrompt } = await import(dist("mesh/payload.js"));
const { createJobStore } = await import(dist("mesh/jobs.js"));
const { createVarStore, maskValue } = await import(dist("mesh/vars.js"));
const { createAuth } = await import(dist("http/auth.js"));
const { resolveConfig, resolveEnvRef, DEFAULTS } = await import(dist("config.js"));
const { deriveClientId } = await import(dist("mesh/transport.js"));

let pass = 0, fail = 0;
function t(name, fn) {
  try { fn(); console.log(`✅ ${name}`); pass++; }
  catch (e) { console.log(`❌ ${name}\n     ${e.message}`); fail++; }
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

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
