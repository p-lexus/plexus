/**
 * Record a tour of the control panel as a video.
 *
 *   node tools/panel-video.mjs
 *
 * Frames come from the same offline harness the screenshots use, so this is
 * the **shipped** panel against redacted fixtures — not a mockup, and never a
 * live deployment's data. Each view is held, then cross-faded to the next,
 * with the Plexus wordmark burned into the corner.
 *
 * Output: `site/assets/console.mp4` plus a poster frame. MP4/H.264 rather than
 * WebM because it is the one format every browser and every social preview
 * will play, and this file exists to be looked at by people who have not
 * installed anything.
 *
 * Requires Chrome and ffmpeg.
 */

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { CHROME, root, loadFixtures, writeFrame } from "./panel-harness.mjs";

const OUT_DIR = path.join(root, "site/assets");
const VIDEO = path.join(OUT_DIR, "console.mp4");
const POSTER = path.join(OUT_DIR, "console-poster.png");

const W = 1440, H = 900;          // 16:10, matching the panel's natural shape
const HOLD = 3.2;                 // seconds each view is held
const FADE = 0.7;                 // cross-fade duration
const FPS = 30;

// A tour, not a feature list: connection first, then a job's history, then the
// two screens that show capabilities and secrets are handled as data.
const STORYBOARD = [
  { page: "overview", theme: "light" },
  { page: "jobs",     theme: "light" },
  { page: "services", theme: "light" },
  { page: "vars",     theme: "light" },
  { page: "jobs",     theme: "dark"  },
  { page: "session",  theme: "dark"  },
];

const have = (bin) => { try { execFileSync("which", [bin], { stdio: "ignore" }); return true; } catch { return false; } };
if (!have("ffmpeg")) {
  console.error("tools/panel-video.mjs needs ffmpeg.\n\n  brew install ffmpeg\n");
  process.exit(1);
}

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "plexus-video-"));
const fixtures = loadFixtures();
fs.mkdirSync(OUT_DIR, { recursive: true });

// ── 1. capture each view ────────────────────────────────────────────────────

console.log(`capturing ${STORYBOARD.length} views at ${W}×${H}`);
const frames = STORYBOARD.map((shot, i) => {
  const name = `${String(i).padStart(2, "0")}-${shot.page}-${shot.theme}`;
  const html = writeFrame(tmp, name, shot.page, shot.theme, fixtures);
  const png = path.join(tmp, `${name}.png`);
  execFileSync(CHROME, [
    "--headless", "--disable-gpu", "--no-sandbox", "--hide-scrollbars",
    // Full-size viewport at 2x density: halving the window to compensate for
    // the scale factor gives a 1440x900 image of a 720x450 *layout*, which
    // collapses the panel's responsive nav into overlapping wrapped text.
    "--force-device-scale-factor=2",
    `--window-size=${W},${H}`,
    "--virtual-time-budget=4000",
    `--screenshot=${png}`,
    `file://${html}`,
  ], { stdio: "ignore" });
  console.log(`  ${name}`);
  return png;
});

// ── 2. the watermark ────────────────────────────────────────────────────────
// Rendered from the same wordmark the site and README use, so the video cannot
// drift from the brand. White, because it sits on both light and dark frames
// inside a darkened plate.

const markHtml = path.join(tmp, "mark.html");
fs.writeFileSync(markHtml, `<!doctype html><meta charset="utf-8">
<style>
  html,body{margin:0;background:transparent}
  .plate{display:inline-flex;align-items:center;gap:14px;padding:14px 22px;
         background:rgba(8,16,15,.72);border-radius:10px}
  svg{color:#fff}
</style>
<div class="plate">
  <svg width="26" height="26" viewBox="0 0 64 64" fill="none" stroke="currentColor"
       stroke-width="7" stroke-linecap="square">
    <path d="M12 12 L26 26"/><path d="M38 38 L52 52"/>
    <path d="M52 12 L38 26"/><path d="M26 38 L12 52"/>
  </svg>
  <svg width="150" viewBox="0 0 600 120" fill="none" stroke="currentColor" stroke-width="10"
       stroke-linecap="square" stroke-linejoin="miter">
    <path d="M28 94V26H62C77 26 86 34 86 47C86 60 77 68 62 68H28"/>
    <path d="M118 26V94H166"/>
    <path d="M198 26H248"/><path d="M198 60H239"/><path d="M198 94H248"/>
    <path d="M282 26L338 94"/><path d="M338 26L282 94"/>
    <path d="M372 26V70C372 87 381 96 398 96C415 96 424 87 424 70V26"/>
    <path d="M508 26H474C460 26 452 34 452 45C452 56 460 61 474 61H492C506 61 514 68 514 79C514 89 506 94 492 94H452"/>
  </svg>
</div>`);
const markPng = path.join(tmp, "mark.png");
execFileSync(CHROME, [
  "--headless", "--disable-gpu", "--no-sandbox", "--hide-scrollbars",
  "--default-background-color=00000000",     // keep the plate's own transparency
  "--window-size=300,72", "--force-device-scale-factor=2", "--virtual-time-budget=1500",
  `--screenshot=${markPng}`, `file://${markHtml}`,
], { stdio: "ignore" });

// ── 3. assemble ─────────────────────────────────────────────────────────────
// Each still becomes a clip; xfade chains them. Offsets accumulate as
// (HOLD - FADE) per transition, which is what keeps the hold times even.

const args = [];
for (const png of frames) {
  args.push("-loop", "1", "-t", String(HOLD), "-i", png);
}
args.push("-i", markPng);

const scale = `scale=${W}:${H}:force_original_aspect_ratio=decrease,`
            + `pad=${W}:${H}:(ow-iw)/2:(oh-ih)/2:color=0x0b1110,setsar=1,fps=${FPS}`;

let filter = frames.map((_, i) => `[${i}:v]${scale}[v${i}]`).join(";") + ";";
let last = "v0";
let offset = HOLD - FADE;
for (let i = 1; i < frames.length; i++) {
  const out = `x${i}`;
  filter += `[${last}][v${i}]xfade=transition=fade:duration=${FADE}:offset=${offset.toFixed(3)}[${out}];`;
  last = out;
  offset += HOLD - FADE;
}
// Watermark last, so it rides above every transition rather than fading with them.
filter += `[${frames.length}:v]scale=300:-1[wm];[${last}][wm]overlay=W-w-40:H-h-36:format=auto[out]`;

const total = (HOLD * frames.length) - (FADE * (frames.length - 1));
console.log(`\nassembling ${total.toFixed(1)}s at ${FPS}fps`);

execFileSync("ffmpeg", [
  "-y", ...args,
  "-filter_complex", filter, "-map", "[out]",
  "-c:v", "libx264", "-preset", "slow", "-crf", "23",
  "-pix_fmt", "yuv420p",                     // required for Safari and QuickTime
  "-movflags", "+faststart",                 // metadata first, so it streams
  "-an", VIDEO,
], { stdio: ["ignore", "ignore", "pipe"] });

// A poster, so the <video> has something to show before it plays or if it
// cannot autoplay — which is most mobile browsers on a metered connection.
execFileSync("ffmpeg", ["-y", "-i", VIDEO, "-ss", "0.4", "-frames:v", "1", POSTER],
  { stdio: ["ignore", "ignore", "pipe"] });

fs.rmSync(tmp, { recursive: true, force: true });

const mb = (p) => (fs.statSync(p).size / 1024 / 1024).toFixed(2);
console.log(`\nwrote site/assets/console.mp4        ${mb(VIDEO)} MB`);
console.log(`      site/assets/console-poster.png  ${mb(POSTER)} MB`);
