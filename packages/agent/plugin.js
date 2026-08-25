/**
 * Plugins, and the host that runs them.
 *
 * Plexus is the frame. A plugin is what makes an agent *do* something: it gets
 * a connected agent and adds capabilities to it. Hermes is one. Whatever you
 * write is another.
 *
 * The contract is deliberately tiny — a name and a `setup` function:
 *
 *   import { definePlugin } from "plexus-agent/plugin";
 *
 *   export default definePlugin({
 *     name: "echo",
 *     setup(agent, config) {
 *       agent.serve("echo", (job) => ({ echoed: job.args.phrase }));
 *     },
 *   });
 *
 * One agent hosts many plugins over **one** connection. That matters: two
 * plugins running as two processes would be two agents on the mesh, two entries
 * in the registry, and two sessions to keep durable. Loading both into one host
 * gives you a single agent that happens to be good at several things — which is
 * how an agent's capabilities are meant to grow.
 */

import { connect } from "./index.js";

/**
 * Declare a plugin. Validates the shape now, so a mistake surfaces at load
 * rather than as a missing capability nobody notices.
 *
 * @param {object} spec
 * @param {string} spec.name           Identifies the plugin in logs and config.
 * @param {string} [spec.description]
 * @param {(agent: object, config: object, ctx: object) => (object|Promise<object>)} spec.setup
 *        Receives the connected agent. Register capabilities with `agent.serve`.
 *        May return `{ stop() }` to clean up on shutdown.
 */
export function definePlugin(spec) {
  if (!spec?.name) throw new Error("definePlugin: `name` is required");
  if (typeof spec.setup !== "function") throw new Error(`definePlugin(${spec.name}): \`setup\` must be a function`);
  return { description: "", ...spec, __plexusPlugin: true };
}

const isPlugin = (value) => Boolean(value && value.__plexusPlugin);

/** Accepts a plugin, a module namespace with one, or a factory returning one. */
function coercePlugin(loaded, source) {
  const candidate = isPlugin(loaded) ? loaded
    : isPlugin(loaded?.default) ? loaded.default
    : isPlugin(loaded?.plugin) ? loaded.plugin
    : typeof loaded === "function" ? loaded()
    : typeof loaded?.default === "function" ? loaded.default()
    : null;
  if (!isPlugin(candidate)) {
    throw new Error(`"${source}" is not a Plexus plugin — it should default-export definePlugin({ name, setup })`);
  }
  return candidate;
}

/**
 * Run one agent hosting a set of plugins.
 *
 * @param {object} config
 * @param {string} config.broker
 * @param {string} config.agentId
 * @param {object} config.plugins   `{ "<specifier>": <plugin config> }`. A specifier is a
 *                                  package name or a path; `false` disables an entry.
 * @param {object} [deps]           `{ log, connect, resolve }` — injection points for tests.
 */
export function createHost(config = {}, deps = {}) {
  const log = deps.log ?? ((msg) => console.log(`${new Date().toISOString()}  plexus  ${msg}`));
  const connectFn = deps.connect ?? connect;
  const resolve = deps.resolve ?? ((specifier) => import(specifier));

  const entries = Object.entries(config.plugins ?? {})
    .filter(([, pluginConfig]) => pluginConfig !== false && pluginConfig?.enabled !== false);

  let agent = null;
  const started = [];

  return {
    get agent() { return agent; },
    get plugins() { return started.map((s) => s.plugin.name); },

    /** Whatever a plugin's `setup` returned — its stats, its own API, anything. */
    handle(name) { return started.find((s) => s.plugin.name === name)?.handle; },

    async start() {
      agent = await connectFn({
        broker: config.broker,
        username: config.username,
        password: config.password,
        root: config.root,
        agentId: config.agentId,
        displayName: config.displayName,
        clientId: config.clientId,
        maxDepth: config.maxDepth,
        askTimeoutMs: config.askTimeoutMs,
        log: (m) => log(m),
      });

      for (const [specifier, pluginConfig] of entries) {
        // Load sequentially and fail loudly. A plugin that half-loads leaves an
        // agent advertising capabilities it cannot actually serve, which is
        // worse than not starting at all.
        let plugin;
        try {
          plugin = coercePlugin(await resolve(specifier), specifier);
        } catch (err) {
          await agent.close().catch(() => {});
          throw new Error(`cannot load plugin "${specifier}": ${err.message}`);
        }
        try {
          const handle = await plugin.setup(agent, pluginConfig ?? {}, { log, config });
          started.push({ plugin, handle });
          log(`loaded plugin "${plugin.name}"`);
        } catch (err) {
          await agent.close().catch(() => {});
          throw new Error(`plugin "${plugin.name}" failed to start: ${err.message}`);
        }
      }

      log(`agent "${config.agentId}" online on ${config.broker} — ${started.length} plugin(s), ` +
        `serving ${agent.capabilities().map((c) => c.service).join(", ") || "nothing"}`);
      return this;
    },

    async stop() {
      // Reverse order, so a plugin that depends on an earlier one is torn down
      // while that one is still alive.
      for (const { plugin, handle } of [...started].reverse()) {
        try { await handle?.stop?.(); } catch (err) { log(`plugin "${plugin.name}" failed to stop: ${err.message}`); }
      }
      if (agent) await agent.close();
      log("stopped");
    },
  };
}

export default { definePlugin, createHost };
