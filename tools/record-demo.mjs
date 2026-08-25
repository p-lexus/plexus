/**
 * Record an example as an animated SVG for the README.
 *
 *   node tools/record-demo.mjs examples/with-hermes.mjs docs/demo.svg
 *
 * Runs the example for real, timestamps each line as it arrives, and renders
 * the result as a self-contained animated SVG. The point of running it rather
 * than hand-writing the frames: the picture in the README cannot drift from
 * what the code actually does, because it *is* what the code did.
 *
 * The output uses CSS animation only — no script, no external font, no raster —
 * so it works as an <img> on GitHub and stays legible in light and dark.
 */

import { spawn } from "node:child_process";
import { writeFile } from "node:fs/promises";

const [, , scriptPath = "examples/with-hermes.mjs", outPath = "docs/demo.svg"] = process.argv;

// ── capture ─────────────────────────────────────────────────────────────────

const lines = await new Promise((resolve, reject) => {
  const child = spawn(process.execPath, [scriptPath], {
    env: { ...process.env, FORCE_COLOR: "1", PLEXUS_ROOT: `plexus-record-${Date.now().toString(36)}` },
  });
  const captured = [];
  const started = Date.now();
  let buffer = "";

  const consume = (chunk) => {
    buffer += chunk.toString();
    const parts = buffer.split("\n");
    buffer = parts.pop() ?? "";
    for (const line of parts) captured.push({ t: (Date.now() - started) / 1000, text: line });
  };

  child.stdout.on("data", consume);
  child.stderr.on("data", consume);
  child.on("error", reject);
  child.on("close", (code) => {
    if (buffer) captured.push({ t: (Date.now() - started) / 1000, text: buffer });
    if (code !== 0) return reject(new Error(`${scriptPath} exited ${code}`));
    resolve(captured);
  });
});

console.log(`captured ${lines.length} lines from ${scriptPath}`);

// ── retime ──────────────────────────────────────────────────────────────────
// Real gaps include connection setup nobody wants to watch. Long pauses are
// capped and a minimum step is enforced, so bursts stay readable.

const MAX_GAP = 0.75, MIN_GAP = 0.11;
let clock = 0, previous = 0;
const timed = lines.map((line) => {
  const gap = Math.min(Math.max(line.t - previous, MIN_GAP), MAX_GAP);
  previous = line.t;
  clock += gap;
  return { ...line, at: clock };
});

const HOLD = 2.6;                                   // pause on the final frame
const total = Math.max(clock + HOLD, 1);

// ── ANSI → tspan ────────────────────────────────────────────────────────────

const ANSI = {
  0: null, 1: "b", 2: "d", 33: "amber", 36: "teal", 31: "red", 32: "green",
};
const esc = (s) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

/** Split a line into styled runs. Unknown codes reset rather than leak a style. */
function toSpans(text) {
  const out = [];
  const classes = new Set();
  let index = 0;
  const re = /\x1b\[([0-9;]*)m/g;
  let match;
  while ((match = re.exec(text))) {
    if (match.index > index) out.push({ text: text.slice(index, match.index), classes: [...classes] });
    for (const codeText of match[1].split(";")) {
      const code = Number(codeText || 0);
      if (code === 0) classes.clear();
      else if (ANSI[code]) classes.add(ANSI[code]);
    }
    index = re.lastIndex;
  }
  if (index < text.length) out.push({ text: text.slice(index), classes: [...classes] });
  return out.filter((run) => run.text.length);
}

// ── render ──────────────────────────────────────────────────────────────────

const MAX_WIDTH = 1160, PAD_X = 22, TOP = 52;
const CH_RATIO = 0.6237;                            // advance width ÷ font size, for these faces

const widest = Math.max(...timed.map((l) => toSpans(l.text).reduce((n, r) => n + r.text.length, 0)), 64);
// Shrink to fit rather than clip: one long summary line should not push the
// rest of the transcript off the edge of the picture.
const fontSize = Math.min(13.5, (MAX_WIDTH - PAD_X * 2) / (widest * CH_RATIO));
const CH = fontSize * CH_RATIO;
const LH = Math.round(fontSize * 1.56);
const width = Math.min(Math.round(widest * CH + PAD_X * 2), MAX_WIDTH);
const height = TOP + timed.length * LH + 20;

const body = timed.map((line, i) => {
  const spans = toSpans(line.text);
  if (!spans.length) return "";
  const y = TOP + i * LH;
  const content = spans.map((run) => {
    const cls = run.classes.join(" ");
    return cls ? `<tspan class="${cls}">${esc(run.text)}</tspan>` : esc(run.text);
  }).join("");
  return `<text class="l" x="${PAD_X}" y="${y}" style="animation-name:r${i}">${content}</text>`;
}).filter(Boolean).join("\n  ");

// One keyframes per line: each holds at opacity 0 until its moment, then stays
// visible for the rest of the loop. A shared animation cannot express that.
//
// The loop opens on a *poster* — the finished transcript, held briefly — before
// clearing and replaying. Without it the first thing anyone sees is an empty
// terminal that takes several seconds to say anything, which reads as broken
// rather than as an animation about to start.
const POSTER = 9;                                   // % of the loop spent on the poster
const keyframes = timed.map((line, i) => {
  const p = Math.max(Math.min((line.at / total) * 100, 99.4), POSTER + 1.2).toFixed(2);
  return `@keyframes r${i}{`
    + `0%,${POSTER}%{opacity:1}`                    // poster: everything visible
    + `${(POSTER + 0.4).toFixed(2)}%,${p}%{opacity:0}`   // cleared, waiting its turn
    + `${(Number(p) + 0.35).toFixed(2)}%,100%{opacity:1}}`;
}).join("\n    ");

const svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${width} ${height}" width="${width}" height="${height}" role="img" aria-label="Two Plexus agents collaborating on one request, with Hermes delivering the outcome">
  <style>
    :root{--bg:#0f1518;--fg:#c9d6d3;--dim:#6d8078;--teal:#3fb9a5;--amber:#d8a44b;--red:#e26d6d;--green:#5fbf8a;--chrome:#1b2427}
    @media (prefers-color-scheme: light){
      :root{--bg:#f6f8f7;--fg:#22312e;--dim:#7d8f89;--teal:#12796a;--amber:#9a6b12;--red:#b23b3b;--green:#2f7d55;--chrome:#e5eae8}
    }
    .bg{fill:var(--bg)}
    .chrome{fill:var(--chrome)}
    text{font-family:ui-monospace,"SF Mono",SFMono-Regular,Menlo,Consolas,"Liberation Mono",monospace;font-size:${fontSize.toFixed(2)}px;fill:var(--fg);white-space:pre}
    .d{fill:var(--dim)}
    .b{font-weight:700}
    .teal{fill:var(--teal)}
    .amber{fill:var(--amber)}
    .red{fill:var(--red)}
    .green{fill:var(--green)}
    /* Base state is VISIBLE, and the animation hides lines before revealing
       them. If animations never run — a paused tab, a renderer that does not
       animate images, a reduced-motion setting — the reader still gets the
       whole transcript. The opposite default fails to a blank terminal, which
       reads as broken rather than as an animation that has not started. */
    .l{opacity:1;animation-duration:${total.toFixed(2)}s;animation-iteration-count:infinite;animation-timing-function:steps(1,end)}
    @media (prefers-reduced-motion: reduce){.l{opacity:1;animation:none}}
    ${keyframes}
  </style>
  <rect class="bg" width="${width}" height="${height}" rx="10"/>
  <rect class="chrome" width="${width}" height="32" rx="10"/>
  <rect class="chrome" y="22" width="${width}" height="10"/>
  <circle cx="20" cy="16" r="5" fill="#e26d6d"/>
  <circle cx="38" cy="16" r="5" fill="#d8a44b"/>
  <circle cx="56" cy="16" r="5" fill="#5fbf8a"/>
  <text class="d" x="76" y="21" style="font-size:11.5px;opacity:1;animation:none">node ${esc(scriptPath)}</text>
  ${body}
</svg>
`;

await writeFile(outPath, svg);
console.log(`wrote ${outPath} — ${width}×${height}, ${total.toFixed(1)}s loop`);
