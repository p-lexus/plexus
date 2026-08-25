/**
 * Render a markdown document to PDF.
 *
 *   node tools/pdf.mjs PROTOCOL.md PROTOCOL.pdf
 *
 * The specification is the one document people read away from a screen, so it
 * is worth having a real typeset version rather than a printed web page.
 *
 * Everything is inlined and Chrome runs with no network access: the mermaid
 * bundle comes from node_modules, the fonts are system stacks, and no request
 * leaves the machine. A build step that silently depends on a CDN produces a
 * different document depending on where it ran.
 */

import { readFile, writeFile, rm } from "node:fs/promises";
import { existsSync } from "node:fs";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

// Loaded on demand rather than declared as devDependencies: mermaid alone is
// ~3.6 MB, this repository is cloned onto the machines that run agents, and a
// documentation tool has no business inflating a deployment.
let marked;
try {
  ({ marked } = await import("marked"));
  if (!existsSync("node_modules/mermaid/dist/mermaid.min.js")) throw new Error("mermaid");
} catch {
  console.error(
    "tools/pdf.mjs needs two packages that are deliberately not dependencies:\n\n" +
    "  npm install --no-save marked mermaid\n",
  );
  process.exit(1);
}

const run = promisify(execFile);
const [, , input = "PROTOCOL.md", output = "PROTOCOL.pdf"] = process.argv;

const CHROME = [
  "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
  "/Applications/Chromium.app/Contents/MacOS/Chromium",
  "/usr/bin/google-chrome",
  "/usr/bin/chromium",
  "/usr/bin/chromium-browser",
].find((path) => existsSync(path));

if (!CHROME) {
  console.error("tools/pdf.mjs: no Chrome or Chromium found. Install one, or set CHROME_PATH.");
  process.exit(1);
}

const source = await readFile(input, "utf8");
const mermaidBundle = await readFile("node_modules/mermaid/dist/mermaid.min.js", "utf8");

// Fenced mermaid becomes a <pre> the bundle picks up; everything else is
// ordinary markdown.
marked.use({
  renderer: {
    code({ text, lang }) {
      if (lang === "mermaid") return `<pre class="mermaid">${text}</pre>`;
      const escaped = text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
      return `<pre><code>${escaped}</code></pre>`;
    },
  },
});

const title = source.match(/^#\s+(.+)$/m)?.[1] ?? input;
const body = marked.parse(source);

const html = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>${title}</title>
<style>
  @page { size: A4; margin: 18mm 16mm 20mm; }
  :root {
    --ink: #16211f; --muted: #5c6b66; --accent: #12796a;
    --rule: #d9e2df; --panel: #f4f7f6; --code: #0f1518;
  }
  * { box-sizing: border-box; }
  body {
    font: 10.5pt/1.6 "Charter", "Iowan Old Style", Georgia, "Times New Roman", serif;
    color: var(--ink); margin: 0; -webkit-print-color-adjust: exact; print-color-adjust: exact;
  }
  h1, h2, h3, h4 {
    font-family: "SF Pro Text", -apple-system, "Segoe UI", Helvetica, Arial, sans-serif;
    line-height: 1.25; text-wrap: balance; margin: 1.6em 0 0.5em;
  }
  h1 { font-size: 26pt; letter-spacing: -0.02em; margin-top: 0; }
  h2 {
    font-size: 15pt; letter-spacing: -0.01em; color: var(--accent);
    border-bottom: 1px solid var(--rule); padding-bottom: 0.25em;
    /* A heading stranded at the foot of a page is the classic print bug. */
    break-after: avoid; break-inside: avoid;
  }
  h3 { font-size: 12pt; break-after: avoid; }
  h4 { font-size: 10.5pt; color: var(--muted); break-after: avoid; }
  p, li { orphans: 3; widows: 3; }
  a { color: var(--accent); text-decoration: none; }
  strong { font-weight: 650; }

  code {
    font-family: ui-monospace, "SF Mono", Menlo, Consolas, monospace;
    font-size: 0.86em; background: var(--panel); padding: 0.1em 0.32em;
    border-radius: 3px; word-break: break-word;
  }
  pre {
    background: var(--code); color: #cfe0dc; padding: 10pt 12pt; border-radius: 5px;
    font-size: 8.4pt; line-height: 1.45; overflow-x: auto; break-inside: avoid;
  }
  pre code { background: none; color: inherit; padding: 0; font-size: inherit; }

  table {
    width: 100%; border-collapse: collapse; margin: 1em 0;
    font-size: 9pt; break-inside: avoid;
  }
  th, td { border: 1px solid var(--rule); padding: 5pt 7pt; text-align: left; vertical-align: top; }
  th { background: var(--panel); font-weight: 650; }
  td code { font-size: 0.9em; }

  blockquote {
    margin: 1em 0; padding: 0.4em 0 0.4em 12pt;
    border-left: 2.5pt solid var(--accent); color: var(--muted);
  }
  hr { border: 0; border-top: 1px solid var(--rule); margin: 2em 0; }

  .mermaid { background: var(--panel); text-align: center; padding: 8pt; break-inside: avoid; }
  .mermaid svg { max-width: 100%; height: auto; }
</style></head>
<body>
${body}
<script>${mermaidBundle}</script>
<script>
  mermaid.initialize({
    startOnLoad: true, theme: "neutral", securityLevel: "loose",
    themeVariables: { fontFamily: "ui-monospace, SF Mono, Menlo, monospace", fontSize: "12px" },
    // Without wrapping, a long "Note over" runs past its own box and collides
    // with the lifelines behind it — legible on screen at full width, not on A4.
    sequence: { useMaxWidth: true, wrap: true, width: 160, noteAlign: "left", messageFontSize: 11 },
    flowchart: { useMaxWidth: true },
  });
</script>
</body></html>
`;

const temp = `${output}.tmp.html`;
await writeFile(temp, html);

try {
  await run(CHROME, [
    "--headless", "--disable-gpu", "--no-sandbox", "--hide-scrollbars",
    "--no-pdf-header-footer",
    // Long enough for mermaid to lay out every diagram. Chrome prints whatever
    // is on the page when this expires, so too short means blank figures.
    "--virtual-time-budget=20000",
    `--print-to-pdf=${output}`,
    `file://${process.cwd()}/${temp}`,
  ], { maxBuffer: 1 << 26 });
} finally {
  await rm(temp, { force: true });
}

const { size } = await import("node:fs").then((fs) => fs.promises.stat(output));
console.log(`wrote ${output} — ${(size / 1024).toFixed(0)} KB from ${input}`);
