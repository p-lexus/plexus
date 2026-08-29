/**
 * The offline harness that renders the real control panel for capture.
 *
 * Shared by `screenshots.mjs` and `panel-video.mjs`, because both need exactly
 * the same guarantee: what is captured is the **shipped** panel — the same
 * `dist/web/index.html` the plugin serves — driven by the redacted fixtures in
 * `test/fixtures`. Not a mockup, and never a live deployment's topics, job ids
 * or repository names.
 *
 * Everything here exists to make a browser hold still long enough to be
 * photographed: the network is stubbed, the event stream is inert, recurring
 * timers are removed, and the clock is frozen to the fixtures' epoch.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const root = fileURLToPath(new URL("..", import.meta.url));
export const panelPath = path.join(root, "dist/web/index.html");

export const CHROME = process.env.CHROME
  || "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";

export function loadFixtures() {
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
  // real, so "2h ago" would render as "7667h ago" unless time is frozen to it.
  const frozenNow = Math.max(...(jobs.history || []).map((j) => j.updatedAt || 0)) + 90_000;
  const expanded = (jobs.history || []).find((j) => (j.events || []).length > 1)?.jobId ?? "";

  return { profile, status, jobs, secrets, frozenNow, expanded };
}

export function stubScript({ profile, status, jobs, secrets, frozenNow }) {
  return `<script>
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
  const FROZEN = ${frozenNow};
  const RealDate = Date;
  window.Date = class extends RealDate {
    constructor(...a) { return a.length ? new RealDate(...a) : new RealDate(FROZEN); }
    static now() { return FROZEN; }
  };
  try { localStorage.setItem('mesh-token', 'demo'); } catch (e) {}
})();
</script>`;
}

export function afterScript(page, theme, { secrets, expanded }) {
  return `<script>
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
}

/** Write a ready-to-capture copy of the panel for one view. */
export function writeFrame(dir, name, page, theme, fixtures) {
  if (!fs.existsSync(panelPath)) {
    throw new Error("dist/web/index.html missing — run `npm run build` first.");
  }
  // The panel LINKS its stylesheet rather than inlining it, so a frame written
  // to a temporary directory has to take the file with it. Without this the
  // capture is a correctly-working panel with no CSS at all — which renders as
  // an unstyled document and is obvious in the output, but only if somebody
  // looks at the output.
  const styles = path.join(path.dirname(panelPath), "theme.css");
  if (fs.existsSync(styles)) fs.copyFileSync(styles, path.join(dir, "theme.css"));
  const src = fs.readFileSync(panelPath, "utf8");
  // The stub goes BEFORE the panel script so it is in place when the panel boots.
  const html = src
    .replace("<script>", `${stubScript(fixtures)}\n<script>`)
    .replace("</body>", `${afterScript(page, theme, fixtures)}\n</body>`);
  const file = path.join(dir, `${name}.html`);
  fs.writeFileSync(file, html);
  return file;
}
