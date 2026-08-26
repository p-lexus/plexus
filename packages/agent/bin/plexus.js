#!/usr/bin/env node
/**
 * plexus — run an agent that hosts plugins.
 *
 *   plexus run --config plexus.json
 *   plexus run --config plexus.json --dry-run
 *
 * The agent itself does nothing. Plugins give it capabilities, and they share
 * one connection, one registry entry and one durable session — so an agent that
 * is good at four things is still one agent on the mesh, not four.
 */

import { readFile } from "node:fs/promises";
import { resolve as resolvePath } from "node:path";
import { pathToFileURL } from "node:url";
import { createHost } from "../plugin.js";

const args = process.argv.slice(2);
const command = args.find((a) => !a.startsWith("--")) ?? "run";
const flag = (name, fallback) => {
  const i = args.indexOf(`--${name}`);
  return i === -1 ? fallback : (args[i + 1]?.startsWith("--") ? true : args[i + 1] ?? true);
};
const has = (name) => args.includes(`--${name}`);

if (has("help") || command === "help") {
  console.log(`
  plexus — run an agent that hosts plugins

    plexus run [options]

    --config <path>    config file (default: ./plexus.json, or $PLEXUS_CONFIG)
    --broker <url>     override the broker URL
    --agent <id>       override the agent id
    --root <name>      override the mesh topic root
    --dry-run          passed to plugins; side effects are suppressed
    --help

  A config names the agent and the plugins it loads:

    { "broker": "mqtt://localhost:1883",
      "agentId": "reviewer",
      "plugins": {
        "plexus-notify": { "channels": {…}, "routes": […] }
      } }

  Use \${VAR} anywhere a secret would otherwise sit in the file.
`);
  process.exit(0);
}

const configPath = flag("config", process.env.PLEXUS_CONFIG ?? "./plexus.json");

let config;
try {
  config = JSON.parse(await readFile(configPath, "utf8"));
} catch (err) {
  if (err.code === "ENOENT") {
    console.error(`plexus: no config at ${configPath}\n` +
      `Pass --config <path>, or create one — see packages/agent/plexus.example.json.`);
    process.exit(1);
  }
  console.error(`plexus: ${configPath} is not valid JSON — ${err.message}`);
  process.exit(1);
}

if (typeof flag("broker") === "string") config.broker = flag("broker");
if (typeof flag("agent") === "string") config.agentId = flag("agent");
if (typeof flag("root") === "string") config.root = flag("root");
if (has("dry-run")) {
  for (const key of Object.keys(config.plugins ?? {})) {
    if (config.plugins[key] && typeof config.plugins[key] === "object") config.plugins[key].dryRun = true;
  }
}

const host = createHost(config, {
  // A relative specifier means a file in the user's project, not a package in
  // this CLI's own node_modules — so it is resolved against the config's
  // directory rather than against this file.
  resolve: (specifier) =>
    specifier.startsWith(".") || specifier.startsWith("/")
      ? import(pathToFileURL(resolvePath(configPath, "..", specifier)).href)
      : import(specifier),
});

try {
  await host.start();
} catch (err) {
  console.error(`plexus: ${err.message}`);
  process.exit(1);
}

let closing = false;
for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, async () => {
    if (closing) process.exit(1);          // a second ^C means "now"
    closing = true;
    await host.stop().catch(() => {});
    process.exit(0);
  });
}
