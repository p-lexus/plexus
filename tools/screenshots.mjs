/**
 * Capture panel screenshots for the documentation.
 *
 *   node tools/screenshots.mjs
 *
 * Renders the REAL panel — the same dist/web/index.html the plugin serves —
 * against the redacted fixtures in test/fixtures, with fetch and EventSource
 * stubbed. That means the images always match shipped behaviour, and can never
 * contain a live deployment's topics, job ids or repo names.
 *
 * Requires Chrome. Set CHROME to override the binary path.
 */

import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("..", import.meta.url));
const panel = path.join(root, "dist/web/index.html");
const outDir = path.join(root, "docs/screenshots");
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-shots-"));

const CHROME = process.env.CHROME
  || "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";

if (!fs.existsSync(panel)) {
  console.error("dist/web/index.html missing — run `npm run build` first.");
  process.exit(1);
}

const fixture = (n) => JSON.parse(fs.readFileSync(path.join(root, `test/fixtures/${n}.json`), "utf8"));
const profile = fixture("profile");
const status = fixture("status");
const jobs = fixture("jobs");

// Give the Variables view something to show. Sources only — never values.
status.promptVars = [
  { name: "SLACK_REVIEW_RECIPIENTS", source: "config" },
  { name: "NOTIFY_CHANNEL", source: "local" },
];
status.secretsAuth = true;

const secrets = {
  ok: true,
  file: "mesh.local.json",
  vars: [
    { name: "SLACK_REVIEW_RECIPIENTS", source: "config", hint: "••••7B2A", editable: false },
    { name: "NOTIFY_CHANNEL", source: "local", hint: "••••eploy", editable: true },
  ],
};

// Fixtures use a FIXED epoch so they are deterministic. The browser clock is
// real, so "2h ago" would render as "7667h ago" unless time is frozen to match.
const fixtureNow = Math.max(...(jobs.history || []).map((j) => j.updatedAt || 0)) + 90_000;

// Expand the first job that has a timeline, so the screenshot shows one.
const expanded = (jobs.history || []).find((j) => (j.events || []).length > 1)?.jobId ?? "";

const stub = `<script>
// Offline harness: no network, no live gateway, no open connections.
(function () {
  const DATA = ${JSON.stringify({ profile, status, jobs, secrets })};
  const route = (u) => u.includes('/profile') ? DATA.profile
    : u.includes('/status') ? DATA.status
    : u.includes('/jobs') ? DATA.jobs
    : u.includes('/secrets') ? DATA.secrets : {};
  window.fetch = (u) => Promise.resolve({
    ok: true, status: 200, json: () => Promise.resolve(route(String(u))),
  });
  // A no-op EventSource: a real one would hold the page open and stall capture.
  window.EventSource = function () { this.close = () => {}; this.addEventListener = () => {}; };
  // Recurring timers never let Chrome's virtual-time budget drain, so the
  // capture hangs. Nothing periodic matters for a still image.
  window.setInterval = () => 0;
  window.clearInterval = () => {};
  // Freeze the clock to the fixtures' epoch so relative times are meaningful
  // and every capture is byte-comparable across runs.
  const FROZEN = ${fixtureNow};
  const RealDate = Date;
  window.Date = class extends RealDate {
    constructor(...a) { return a.length ? new RealDate(...a) : new RealDate(FROZEN); }
    static now() { return FROZEN; }
  };
  try { localStorage.setItem('mesh-token', 'demo'); } catch (e) {}
})();
</script>`;

const after = (page, theme) => `<script>
document.documentElement.setAttribute('data-theme', ${JSON.stringify(theme)});
setTimeout(function () {
  try {
    page = ${JSON.stringify(page)};
    // Expand a job only on the Jobs view; Overview should read as a summary.
    ${expanded && page === "jobs" ? `open.add(${JSON.stringify(expanded)});` : ""}
    if (${JSON.stringify(page)} === 'vars') secrets = ${JSON.stringify(secrets)};
    render();
    document.documentElement.setAttribute('data-ready', '1');
  } catch (e) {
    document.title = 'RENDER ERROR: ' + e.message;
  }
}, 250);
</script>`;

const SHOTS = [
  { page: "overview", theme: "light", name: "overview" },
  { page: "jobs", theme: "light", name: "jobs" },
  { page: "jobs", theme: "dark", name: "jobs-dark" },
  { page: "services", theme: "light", name: "services" },
  { page: "run", theme: "light", name: "run" },
  { page: "session", theme: "dark", name: "session-dark" },
  { page: "vars", theme: "light", name: "variables" },
];

fs.mkdirSync(outDir, { recursive: true });
const src = fs.readFileSync(panel, "utf8");

for (const shot of SHOTS) {
  // Stub goes BEFORE the panel script so it is in place when the panel boots.
  const html = src
    .replace("<script>", `${stub}\n<script>`)
    .replace("</body>", `${after(shot.page, shot.theme)}\n</body>`);
  const file = path.join(tmp, `${shot.name}.html`);
  fs.writeFileSync(file, html);

  const out = path.join(outDir, `${shot.name}.png`);
  execFileSync(CHROME, [
    "--headless", "--disable-gpu", "--no-sandbox", "--hide-scrollbars",
    "--force-device-scale-factor=2",          // crisp on high-density displays
    "--window-size=1400,1000",
    "--virtual-time-budget=4000",
    `--screenshot=${out}`,
    `file://${file}`,
  ], { stdio: "ignore" });

  const kb = Math.round(fs.statSync(out).size / 1024);
  console.log(`  ${shot.name.padEnd(14)} ${String(kb).padStart(4)} KB  (${shot.page}/${shot.theme})`);
}

fs.rmSync(tmp, { recursive: true, force: true });
console.log(`\nWrote ${SHOTS.length} screenshots to docs/screenshots/`);
