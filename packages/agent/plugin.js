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
 * One agent hosts many plugins over **one** connection per mesh. That matters:
 * two plugins running as two processes would be two agents on the mesh, two
 * entries in the registry, and two sessions to keep durable. Loading both into
 * one host gives you a single agent that happens to be good at several things —
 * which is how an agent's capabilities are meant to grow.
 *
 * A host may join several meshes, and then every plugin runs once per mesh. One
 * connection each is what the protocol requires rather than what was easiest:
 * see `connectAll`.
 */

import { connectAll } from "./index.js";

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
 * Run one agent hosting a set of plugins, on one mesh or several.
 *
 * A plugin is set up **once per mesh**, with that mesh's agent. It is not a
 * choice between that and one instance spanning all of them: a plugin keeps
 * state per job — what was asked, what has already been delivered — and a jobId
 * is unique within a mesh and nowhere else. One instance across two meshes
 * would file two different jobs under one id and deliver the wrong request
 * beside the right answer.
 *
 * @param {object} config
 * @param {string} [config.broker]  Default for every mesh, and the whole of it when there is one.
 * @param {Array}  [config.meshes]  One entry per mesh; see `connectAll`.
 * @param {string} config.agentId
 * @param {object} config.plugins   `{ "<specifier>": <plugin config> }`. A specifier is a
 *                                  package name or a path; `false` disables an entry.
 * @param {object} [deps]           `{ log, connect, resolve }` — injection points for tests.
 */
export function createHost(config = {}, deps = {}) {
  const log = deps.log ?? ((msg) => console.log(`${new Date().toISOString()}  plexus  ${msg}`));
  const resolve = deps.resolve ?? ((specifier) => import(specifier));

  const entries = Object.entries(config.plugins ?? {})
    .filter(([, pluginConfig]) => pluginConfig !== false && pluginConfig?.enabled !== false);

  let meshes = null;
  const started = [];

  return {
    /**
     * The agent, when there is exactly one mesh.
     *
     * Undefined when there are several, rather than a guess at which was meant:
     * `meshes.on(name)` is the question that has an answer.
     */
    get agent() { return meshes?.names().length === 1 ? meshes.all()[0] : undefined; },

    /** Every membership. Null until `start()`. */
    get meshes() { return meshes; },

    get plugins() { return [...new Set(started.map((s) => s.plugin.name))]; },

    /**
     * Whatever a plugin's `setup` returned — its stats, its own API, anything.
     *
     * A plugin runs once per mesh, so name the mesh when there is more than
     * one. Refusing to pick beats returning whichever was set up first, which
     * is a plausible-looking answer about the wrong mesh.
     */
    handle(name, mesh) {
      const found = started.filter((s) => s.plugin.name === name && (mesh === undefined || s.mesh === mesh));
      if (found.length > 1) {
        throw new Error(
          `plugin "${name}" runs on ${found.map((s) => s.mesh).join(", ")} — name one of them: handle("${name}", "${found[0].mesh}")`);
      }
      return found[0]?.handle;
    },

    async start() {
      meshes = await connectAll({
        broker: config.broker,
        username: config.username,
        password: config.password,
        root: config.root,
        agentId: config.agentId,
        displayName: config.displayName,
        clientId: config.clientId,
        maxDepth: config.maxDepth,
        askTimeoutMs: config.askTimeoutMs,
        capabilities: config.capabilities,
        meshes: config.meshes,
        log: (m) => log(m),
      }, { connect: deps.connect });

      const names = meshes.names();
      const where = (name) => (names.length > 1 ? ` on ${name}` : "");

      for (const name of names) {
        const agent = meshes.on(name);
        for (const [specifier, pluginConfig] of entries) {
          // Load sequentially and fail loudly. A plugin that half-loads leaves
          // an agent advertising capabilities it cannot actually serve, which
          // is worse than not starting at all — and with several meshes that
          // now means every mesh comes down, not just the one that failed.
          let plugin;
          try {
            plugin = coercePlugin(await resolve(specifier), specifier);
          } catch (err) {
            await meshes.close().catch(() => {});
            throw new Error(`cannot load plugin "${specifier}"${where(name)}: ${err.message}`);
          }
          try {
            const handle = await plugin.setup(agent, pluginConfig ?? {}, {
              log, config,
              // Which mesh this instance is on, and how many there are. A
              // plugin that writes state to a path needs both: one per mesh
              // when there are several, and the path it has always used when
              // there is one — a plugin that renamed its own state file would
              // wake up with an empty one and re-deliver its whole backlog.
              mesh: { name, root: agent.root },
              meshes: [...names],
            });
            started.push({ plugin, handle, mesh: name });
            log(`loaded plugin "${plugin.name}"${where(name)}`);
          } catch (err) {
            await meshes.close().catch(() => {});
            throw new Error(`plugin "${plugin.name}"${where(name)} failed to start: ${err.message}`);
          }
        }

        log(`agent "${config.agentId}" online on ${agent.root} — ${entries.length} plugin(s), ` +
          `serving ${agent.capabilities().map((c) => c.service).join(", ") || "nothing"}`);
      }
      return this;
    },

    async stop() {
      // Reverse order, so a plugin that depends on an earlier one is torn down
      // while that one is still alive.
      for (const { plugin, handle, mesh } of [...started].reverse()) {
        try { await handle?.stop?.(); }
        catch (err) { log(`plugin "${plugin.name}" on ${mesh} failed to stop: ${err.message}`); }
      }
      if (meshes) await meshes.close();
      log("stopped");
    },
  };
}

export default { definePlugin, createHost };
