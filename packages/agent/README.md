<div align="center">
  <img src="../../docs/logo/plexus-mark-light.svg#gh-light-mode-only" width="56" alt="">
  <img src="../../docs/logo/plexus-mark-dark.svg#gh-dark-mode-only" width="56" alt="">
  <h1>plexus-agent</h1>
  <p>Put an agent on the mesh in fifteen lines.</p>
</div>

---

```bash
npm install plexus-agent
```

A small client for the [Agent Mesh Protocol](../../PROTOCOL.md). One dependency (`mqtt`), no build step, no framework. An agent written with this is indistinguishable on the broker from one running inside an OpenClaw gateway — the protocol is the only contract.

## Offering a capability

```js
import { connect } from "plexus-agent";

const agent = await connect({
  broker: "mqtt://localhost:1883",
  agentId: "dba",
});

agent.serve("schema.review", async (job, ctx) => {
  ctx.progress("checking lock behaviour");
  return { risk: "high", finding: "ALTER without CONCURRENTLY locks writes" };
}, { description: "Reviews a migration for lock risk." });
```

That agent is now discoverable by every other agent on the mesh, and reachable while it's offline — invokes queue on the broker and arrive when it reconnects.

## Asking another agent

```js
const result = await agent.invoke("dba", "schema.review", { migration: "0042.sql" });
```

Or don't name one. Ask whoever can:

```js
const peer = agent.find("schema.review");     // returns an agentId, or null
const result = await agent.ask("schema.review", { migration: "0042.sql" });
```

## Delegating mid-job

The interesting case: you're handling a request, and part of it isn't yours.

```js
agent.serve("code.review", async (job, ctx) => {
  const answer = await ctx.askAny("schema.review", { migration: "0042.sql" });
  return {
    verdict: answer.risk === "high" ? "REQUEST_CHANGES" : "APPROVE",
    summary: `Logic is fine. Blocking on the migration: ${answer.finding}`,
  };
});
```

`ctx.ask` and `ctx.askAny` fill in the lineage — `parentJobId`, `rootJobId`, `depth` — so a chain of agents is traceable back to the one request that started it, and a cancel can find everything to stop. The requester gets **one** answer; that two agents produced it is an implementation detail of the mesh.

## What it handles for you

Each of these is a way a mesh breaks in production:

- **Durable sessions.** The client id is derived from hostname + agent + mesh root, never a pid. With a changing id every restart is a new MQTT session, so `clean: false` buys nothing and jobs published while you were down are lost silently.
- **Owner-scoped routing.** Results go to the requester's scope, which is how a reply finds its way home without a callback channel.
- **Hop limits.** A delegation cycle is refused *before* it reaches the wire.
- **A terminal result on every path**, including when your handler throws. A client is never left waiting on a job that quietly stopped existing.
- **Duplicate rejection**, and cancel that propagates to whatever this job delegated.
- **Clean withdrawal** on `close()`, so the registry doesn't fill with agents that aren't there.

## API

| | |
|---|---|
| `connect(options)` | → `Promise<Agent>`. Resolves once the profile is published and the agent is discoverable |
| `agent.serve(service, handler, meta?)` | Offer a capability |
| `agent.invoke(peerId, service, args?, opts?)` | Ask a named peer, wait for its terminal result |
| `agent.ask(service, args?, opts?)` | Find a peer offering it, then ask |
| `agent.peers()` / `agent.find(service)` | Read the retained registry |
| `agent.waitForPeer(service, ms?)` | Resolve once someone offers it |
| `agent.watch(handler)` | Observe job traffic mesh-wide. **await it** |
| `agent.observeCommands(handler)` | Observe invokes — who is asking whom, and with what |
| `agent.cancel(peerId, jobId)` | Cooperative cancel |
| `agent.close()` | Withdraw and disconnect |

Inside a handler, `ctx` gives you `progress()`, `ask()`, `askAny()`, `peers()`, `find()`, `depth`, and a `signal` that aborts on cancel.

TypeScript definitions are hand-written and ship with the package.

### Two calls must be awaited

`watch` and `observeCommands` both swap a narrow subscription for a wide one. Until that completes, an overlapping filter can deliver the same message twice — MQTT 3.1.1 gives the client no way to tell which subscription caused a delivery, so the fix is to not overlap. Awaiting them means no message is ever seen twice.

## Plugins

An agent gains abilities by loading plugins rather than by growing code. A plugin gets a connected agent and adds capabilities to it:

```js
import { definePlugin } from "plexus-agent/plugin";

export default definePlugin({
  name: "echo",
  description: "Says things back.",
  setup(agent, config, ctx) {
    agent.serve("echo", (job) => ({ echoed: job.args.phrase }));
    return { stop() { /* optional cleanup */ } };
  },
});
```

Then run an agent that hosts it:

```json
{ "broker": "mqtt://localhost:1883",
  "agentId": "reviewer",
  "plugins": {
    "plexus-notify": { "channels": { }, "routes": [] },
    "./my-plugin.js": { }
  } }
```

```bash
npx plexus run --config plexus.json
```

A specifier is a package name or a relative path; relative paths resolve against the config file, not against this package. Set an entry to `false`, or give it `"enabled": false`, to skip it — a disabled plugin is never even imported.

**Why plugins rather than separate processes.** Two processes would be two agents on the mesh: two registry entries, two durable sessions, two things to keep alive. Loading both into one host gives you a single agent that happens to be good at several things, which is how an agent's capabilities are meant to grow. `agent.capabilities()` shows the combined set, and peers see one profile.

If a plugin throws during `setup`, the host **shuts the agent down** rather than continuing. A half-loaded agent advertises capabilities it cannot serve, which is worse than not starting.

To embed the host instead of using the CLI:

```js
import { createHost } from "plexus-agent/plugin";

const host = createHost({ broker, agentId, plugins: { "plexus-notify": config } });
await host.start();
host.handle("notify");        // whatever that plugin's setup returned
await host.stop();            // stops plugins in reverse order, then disconnects
```

### Putting a whole platform on the mesh

A *host plugin* is the other direction: it teaches an entire agent platform to speak Plexus, the way [`hosts/openclaw/`](../../hosts/openclaw) does for OpenClaw. See **[docs/HOSTS.md](../../docs/HOSTS.md)**.

## Options

```js
await connect({
  broker: "mqtt://host:1883",
  agentId: "dba",
  displayName: "Database reviewer",
  capabilities: [{ service: "schema.review", description: "…" }],
  root: "agents",          // topic root — isolates one mesh from another
  username, password,
  clientId,                // required if two processes share an agentId
  durable: true,           // clean:false persistent session
  maxDepth: 4,             // delegation hop limit
  askTimeoutMs: 300000,
  requireOwner: true,      // reject invokes with no requestedBy
  log: console.log,
});
```

## Try it

```bash
node examples/demo.mjs          # two agents, one delegation
node examples/with-plugins.mjs  # …and a plugin delivering the outcome
```

---

Apache 2.0 · part of [Plexus](../../README.md)
