# Agent Mesh Protocol

**The agents worth talking to don't live in the cloud. This is how they reach each other anyway.**

An MQTT protocol for dispatching work between autonomous agents — with a durable transport,
capability discovery, and agent-to-agent delegation. Plus a production reference implementation
as an [OpenClaw](https://github.com/openclaw) plugin.

- **Specification** — [PROTOCOL.md](PROTOCOL.md) ([PDF](PROTOCOL.pdf)) · version 1.3
- **Status** — running daily against a real workload; one deployment, one implementation

---

## The problem

Every agent framework wants your agent to be a server. Expose an endpoint, get a URL, receive
requests.

Real agents don't live like that. They live on a developer's laptop that sleeps at 6pm, on a
VPN that drops, in a container with no inbound route, on a machine whose IP changed this
morning. The moment you want two of them to talk, you are in tunnels, ngrok, firewall
exceptions, and a public surface you now have to defend.

**Intelligence is moving to the edge.** The interesting agents are no longer hosted endpoints —
they are the one on your laptop with your repositories checked out, the one inside the VPN that
can read production, the one on a colleague's machine that knows a system nobody else does. They
hold context precisely *because* they are not in the cloud.

So the addressing model is inverted. These agents cannot be servers, and the moment you want two
of them to collaborate, an endpoint-shaped protocol has nothing to offer.

**Let the agent be a client.** It dials out, holds one connection, and receives work over it. Now
it is **addressable without being reachable** — and a mesh can span laptops, VPNs, CI runners and
cloud instances without a single inbound port.

That constraint buys more than connectivity. An agent that is *expected* to disappear gets
first-class treatment for it:

| An agent is… | So the mesh gives it |
|---|---|
| offline half the day | work **queued while it sleeps**, delivered on wake |
| unknown to its peers | a **retained capability profile** — discovered without asking |
| slow, thinking for minutes | **streamed milestones**, and a result you can collect an hour later |
| liable to die mid-thought | **presence published by the broker itself**, not a heartbeat service |
| one of many | **per-requester isolation** as a subscription filter, not access-control code |

None of that is application code. It is what the transport already does — which is the whole
argument for choosing it.

---

## The 30-second version

Someone — a human, CI, or another agent — publishes a job:

```bash
mosquitto_pub -t 'agents/commands/reviewer/invoke' -m '{
  "service": "code.review",
  "requestedBy": "alice",
  "args": { "repo": "acme/web-app", "pr": 42 }
}'
```

The agent picks it up, runs it in an isolated session, and streams back:

```
agents/jobs/alice/rev-118/events    started · analyzing · result-ready
agents/jobs/alice/rev-118/result    { "verdict": "APPROVE", ... }   ← retained
```

Alice subscribed to `agents/jobs/alice/#` and saw only her own traffic. She could have
disconnected and collected the result an hour later — it's retained on the broker.

And if the reviewer hits a database migration it isn't qualified to judge, it asks the agent
that is:

```
reviewer  ──ask──▶  dba          "review this migration"
          ◀─answer──              folded into one reply to Alice
```

---

## A complete agent, in 40 lines

No framework, no plugin — the protocol is small enough to implement directly.
[`examples/minimal-agent.mjs`](examples/minimal-agent.mjs):

```js
client.on("connect", () => {
  // Retained: agents connecting later discover us without us re-announcing
  client.publish(`${ROOT}/registry/${ID}/profile`, JSON.stringify(profile), { qos: 1, retain: true });
  client.subscribe(`${ROOT}/commands/${ID}/invoke`, { qos: 1 });
});

client.on("message", (_t, payload) => {
  const job = JSON.parse(payload.toString());
  const base = `${ROOT}/jobs/${owner(job.requestedBy)}/${job.jobId}`;
  client.publish(`${base}/events`, JSON.stringify({ type: "started" }), { qos: 1 });
  client.publish(`${base}/result`, JSON.stringify({ type: "echo", echoed: job.args.phrase }),
                 { qos: 1, retain: true });          // retained: survives for whoever asks later
});
```

```bash
MESH_BROKER=mqtt://localhost:1883 node examples/minimal-agent.mjs
```

That agent is now discoverable, addressable and durable. Full spec:
[PROTOCOL.md](PROTOCOL.md) — including delivery guarantees, the job state machine, sequence
diagrams and a failure-mode table.

---

## What you get from MQTT that HTTP won't give you

These aren't incidental. Each solves a problem that otherwise becomes application code:

| | What it means |
|---|---|
| **Durable sessions** | A job published while your agent was asleep is **queued by the broker** and delivered when it wakes. No retry logic, no dead-letter queue, no lost work. |
| **Retained messages** | Every agent's capability catalog, and every job's final result, are readable by a client that connects *afterwards*. Discovery and late collection come free. |
| **Topic wildcards** | Multi-tenancy is a subscription filter — `jobs/alice/#` — not authorization code you write and get wrong. |
| **Last will** | An agent that dies is marked offline by the broker itself. No heartbeat service. |
| **One outbound socket** | Runs on a laptop, inside a VPN, in a container with no ingress. Nothing exposed, nothing to defend. |

---

## Where this sits

It isn't competing with your agent framework — it's the layer *between* frameworks.

| | Scope | Assumes |
|---|---|---|
| **MCP** | An agent using tools | Local process or a reachable HTTP server |
| **A2A** | Agents interoperating | Both agents have reachable endpoints |
| **Agent Mesh** | Agents dispatching work to each other | **Only that both can reach a broker** |

Use MCP to give one agent tools. Use this to let agents that can't see each other work together.

---

## Concepts

**A capability is data, not code.** A name, an argument schema, and a prompt template:

```json
{ "service": "schema.review",
  "requestSchema": { "migration": "string" },
  "prompt": "Review migration {{migration}}. Flag lock risk and missing indexes." }
```

The bridge contains no service name anywhere — not `code.review`, not anything. It renders the
template and hands it to an executor. Add, change and remove capabilities at runtime; no
restart, no rebuild, no deploy.

**Every agent is both worker and requester.** A human enters at one agent; from there work flows
agent to agent, each asking whoever owns the capability it lacks. Requests are **directed, never
broadcast** — an agent addressed for a capability it publishes simply does the work. No bidding,
no contention. The only decision is on the asking side, which is why every agent reads the
registry.

**A chain of agents is a chain of jobs.** Each ask creates its own job, linked by `parentJobId`
and `rootJobId`, so a request through five agents is traceable as one thing — and cancellable as
one thing.

---

## The console

The plugin serves an operator console on loopback. One HTML file, no external assets, no MQTT
in the browser.

![Overview](docs/screenshots/overview.png)

Every job keeps its milestone timeline, so *"why did this run twice?"* is answerable afterwards
rather than only while it is happening.

![Jobs](docs/screenshots/jobs.png)

Capabilities are edited as data, with the prompt and its arguments checked against each other.
A prompt using `{{repo}}` with no matching argument renders empty at dispatch and silently
produces a bad job — so the editor refuses to save it.

![Services](docs/screenshots/services.png)

It renders in light and dark.

![Jobs, dark](docs/screenshots/jobs-dark.png)

---

## How it works

```
                    ┌──────────────────────────────────────────┐
  you / another     │              MQTT broker                 │
  agent / CI        │                                          │
        │           │  agents/commands/<agentId>/invoke   ─────┼──▶ agent
        └──publish──┼─▶                                        │      │
                    │  agents/jobs/<owner>/<jobId>/events  ◀───┼──────┤ progress
        ┌─subscribe─┼─                                         │      │
        │           │  agents/jobs/<owner>/<jobId>/result  ◀───┼──────┘ terminal, retained
        ▼           │                                          │
  only your jobs    │  agents/registry/<agentId>/profile   ◀───┼── capability catalog, retained
                    └──────────────────────────────────────────┘
```

A job's life:

1. You publish to `invoke` with a `service`, `args`, and **`requestedBy`** — your stable name.
2. The bridge looks the service up in its catalog, renders that capability's prompt template,
   and starts an isolated executor **immediately**. No queue, no polling.
3. The executor publishes milestones to `…/events` as it works.
4. It publishes one terminal payload to `…/result`, **retained**, so a late subscriber still
   gets it.
5. You saw only your own traffic throughout, because `owner` is derived from `requestedBy` and
   you subscribed to `agents/jobs/<you>/#`.

Topic tables, payload shapes and the full set of guarantees are in
**[PROTOCOL.md](PROTOCOL.md)**.

---

## Design principles

**Transport in the framework, logic in the agent.** The bridge moves jobs. It does not know what
any job *means*.

**Capabilities are data, not code.** A capability is a JSON entry with a prompt template. The
bridge contains no service name anywhere — not `code.review`, not anything else. Add, change and
remove them at runtime; no restart, no rebuild. The catalog shipped here is an example, not the
product.

**The bridge carries jobs, not credentials.** It holds the broker login and an optional console
token. Nothing else. No GitHub, cloud or third-party auth lives in it — executors authenticate
with the host's own identity.

**Push, not poll.** A persistent session delivers jobs, the executor starts the instant one
arrives, and the console is fed by Server-Sent Events. The only periodic timers are a
supervisory watchdog and a slow filesystem reconciler, neither of which carries a message.

**Say so when a guarantee weakens.** If a client-id collision forces a non-durable session, the
bridge says it loudly and the console shows a banner, rather than quietly dropping the promise
that offline jobs survive.

---

## Install

### 1. A broker

Any MQTT 3.1.1 or 5 broker. [EMQX](https://www.emqx.io/) and [Mosquitto](https://mosquitto.org/)
both work. For a local trial:

```bash
docker run -d --name mosquitto -p 1883:1883 eclipse-mosquitto:2 \
  sh -c 'printf "listener 1883\nallow_anonymous true\n" > /m.conf && mosquitto -c /m.conf'
```

Anonymous access is fine on localhost and **not** fine anywhere else — see
[Security model](#security-model).

### 2. The plugin

```bash
git clone https://github.com/MoGhali/agent-mesh-protocol.git ~/.openclaw/extensions/mqtt-bridge
cd ~/.openclaw/extensions/mqtt-bridge
npm install
cp services.example.json services.json    # your capability catalog
npm run build
```

`services.json` is **deployment-local and gitignored**, so `git pull` never collides with your
own capabilities. Skip the copy and the plugin falls back to the example with a warning, so a
fresh clone still runs.

### 3. Configure

In `~/.openclaw/openclaw.json`:

```jsonc
{
  "plugins": {
    "allow": ["mqtt-bridge"],
    "entries": {
      "mqtt-bridge": {
        "enabled": true,
        "config": {
          "broker": {
            "url": "mqtt://localhost:1883",
            "username": "mesh",
            "password": "${MQTT_PASSWORD}"    // ${ENV_VAR} resolves at runtime
          },
          "mesh": {
            "root": "agents",
            "agentId": "my-agent"
          },
          "web": {
            "auth": "<a long random string>"  // required to manage variables
          }
        }
      }
    }
  }
}
```

The config key is the plugin **id** (`mqtt-bridge`), deliberately independent of the npm package
name and the repository name. Only `broker.url` is required.

### Allow the plugin's tools — required

OpenClaw's `tools.profile` is an allowlist that **excludes plugin-registered tools**. Without
this block the agent silently has none of them, and the symptoms are confusing rather than
obvious: executors cannot publish results with `mqtt_publish`, so they improvise with shell
commands and jobs intermittently finish without a result; and `mesh_ask` is simply absent, so
dynamic delegation never happens and nothing says why.

```jsonc
{
  "tools": {
    "profile": "coding",
    "alsoAllow": ["mqtt_publish", "mesh_ask", "mesh_peers"]
  }
}
```

Verify after restarting — this should list all three:

```bash
openclaw agent --agent main -m 'List every tool you have starting with "mesh" or "mqtt".'
```

Tool policy is **not** hot-reloadable: this needs a gateway restart, unlike other config
changes. And always `openclaw config validate` before restarting — an invalid config stops the
gateway from starting at all.

### 4. Restart and verify

```bash
launchctl kickstart -k gui/$(id -u)/ai.openclaw.gateway     # macOS
# systemctl --user restart openclaw-gateway                 # Linux
```

> **Plugin code changes need a restart.** The gateway caches the loaded module, so a rebuilt
> `dist/` sits unused until it is re-imported. Config changes hot-reload on their own.

Confirm the agent announced itself:

```bash
mosquitto_sub -h localhost -t 'agents/registry/+/profile' -C 1 \
  | jq '{agentId, protocolVersion, capabilities: (.capabilities|length)}'
```

Then open `http://127.0.0.1:8765` and sign in with your `web.auth` token.

---

## Send a job

```bash
mosquitto_sub -h localhost -t 'agents/jobs/alice/#' &       # listen first

mosquitto_pub -h localhost -t 'agents/commands/my-agent/invoke' -m '{
  "service": "code.review",
  "requestedBy": "alice",
  "args": { "repo": "acme/web-app", "pr": 42 }
}'
```

`requestedBy` is **required**, and is what scopes the job to you. Omit it and the job is rejected
with a published error rather than silently routed to `public` — where your own subscription
would never have seen it.

| Subscribe to | You get |
|---|---|
| `agents/jobs/<you>/#` | your jobs — events and results |
| `agents/jobs/<you>/+/result` | results only, no progress noise |
| `agents/registry/+/profile` | every agent's capability catalog |

Do **not** subscribe to `agents/jobs/#` — that is everyone's traffic.

---

## Author a capability

A capability is a name, a prompt, and the arguments it takes:

```json
{
  "service": "db.schema_review",
  "description": "Review a migration for lock risk and index coverage",
  "requestSchema": {
    "repo": "string (owner/name)",
    "migration": "string"
  },
  "avgLatency": "2-4 min",
  "handler": "session",
  "prompt": "Review migration {{migration}} in {{repo}}. Flag table locks, missing indexes and non-reversible steps. Notify ${NOTIFY_CHANNEL}."
}
```

`{{name}}` is filled from the job's arguments; `${NAME}` is a deployment variable. Add it from
the console's Services tab, by editing `services.json` (a file watch picks it up immediately),
or over MQTT:

```bash
mosquitto_pub -h localhost -t 'agents/commands/my-agent/config' \
  -m '{"action":"add_service","service":{ … }}'
```

Actions: `list`, `add_service`, `update_service`, `remove_service`, `reload`. Replies land on
`…/config/reply`.

---

## Delegation

An agent that meets work outside its own capabilities asks the agent that owns it. Two forms,
chosen with `mesh.delegation`, because they fail differently:

**Declared** — the capability states its dependencies; the bridge gathers them before the
executor starts and injects the answers:

```json
"delegates": [
  { "agent": "dba", "service": "schema.review", "as": "schemaReview",
    "args": { "migration": "{{migration}}" }, "required": false }
]
```

Deterministic — no tool needed, no reliance on the model choosing to delegate. Cannot adapt to
what a job turns out to need.

**Dynamic** — the executor calls `mesh_ask` mid-job after finding a peer with `mesh_peers`.
Adapts to anything; depends on the executor deciding to use it.

`both` (default), `declared`, `dynamic`, or `off`. Chains carry `parentJobId`, `rootJobId` and
`depth`; `mesh.maxDepth` bounds them, and cancelling a job cancels what it delegated.

## Deployment variables

A capability definition should be portable, but real prompts need install-specific values —
channel ids, internal repo paths, recipient lists. Reference them as `${NAME}` and bind them per
deployment:

![Variables](docs/screenshots/variables.png)

Resolution order, highest first:

1. `mesh.promptVars` in `openclaw.json` — config-as-code
2. `mesh.local.json` — console-managed, `0600`, gitignored
3. `process.env`

Two properties are deliberate:

- **Resolved at dispatch, never in the catalog.** The catalog is published to the *retained*
  profile topic; resolving there would broadcast every deployment value to anyone subscribed to
  the registry. On the wire, the prompt stays a template.
- **Environment expands before arguments.** Reversed, a caller could pass `"${SOME_SECRET}"` as
  an argument value and have the bridge expand it — turning every invoke into an environment
  read.

Values are write-only from the console: the API returns names, sources and a masked hint, and
has no path that reads a value back.

---

## Security model

**Owner scoping is a convention, not authentication.** MQTT delivers topic and payload only — a
publisher's broker identity does not travel with the message. So `requestedBy` is self-declared,
and anyone with broker credentials can claim any owner scope. The profile advertises
`ownerPolicy` so clients read what a deployment actually enforces instead of guessing.

Enforce isolation at the boundary that can see identity — the broker:

```
# EMQX ACL sketch
allow  subscribe  agents/jobs/${username}/#
allow  publish    agents/commands/+/invoke
deny   subscribe  agents/jobs/#
```

Set `mesh.verifyOwner: true` once your broker injects `client_username` into invoke payloads
(EMQX rule-engine enrichment). It **fails closed**: with it on, an invoke arriving without
`client_username` is rejected.

**The console.** Bound to `127.0.0.1`. `web.auth` protects the API; the page shell is served
unauthenticated so it can present a sign-in screen instead of a raw 401. Managing variables
*always* requires a token, even when the rest of the console is open. State-changing routes
additionally require a header that cannot be set cross-origin — binding to loopback stops other
machines, but not other browser tabs.

**Never put secrets in job arguments.** Payloads are readable by every subscriber to that topic,
and results are retained on the broker: a secret published once persists until it is explicitly
overwritten.

---

## Configuration reference

Every option is schema-documented in [`openclaw.plugin.json`](openclaw.plugin.json).

| Key | Default | Notes |
|---|---|---|
| `broker.url` | — | **Required.** `mqtt://` or `mqtts://` |
| `broker.username` / `.password` | — | `${ENV_VAR}` supported |
| `broker.clientId` | derived | Stable hash of host + install path. Set explicitly only if two instances share a directory |
| `broker.keepalive` | `30` | Seconds |
| `broker.protocolVersion` | `4` | `5` adds a bounded session expiry |
| `broker.sessionExpirySeconds` | `86400` | MQTT 5 only |
| `mesh.root` | `agents` | Topic root |
| `mesh.agentId` | `agent` | This agent's name on the mesh |
| `mesh.servicesFile` | `./services.json` | Watched; edits republish the profile |
| `mesh.requireOwner` | `true` | Reject invokes with no `requestedBy` |
| `mesh.verifyOwner` | `false` | Check `requestedBy` against broker identity; fails closed |
| `mesh.maxJobDurationMs` | `1800000` | Hard cap before a terminal timeout |
| `mesh.promptVars` | `{}` | `${VAR}` bindings, highest precedence |
| `web.enabled` | `true` | Serve the console |
| `web.port` | `8765` | Bound to loopback |
| `web.auth` | — | Bearer token; required for variables |
| `web.path` | `/mqtt-bridge/ui` | Base path on the gateway |
| `sessionKey` | `agent:main:main` | Fallback dispatch target on older runtimes |

### Client id and durability

The client id is derived from hostname + install path so it is **stable across restarts**. That
is what makes `clean: false` mean anything: with a changing id, every restart is a new session,
the broker's queued messages stay orphaned with the dead one, and any job published while you
were down is lost silently.

A stable id has one cost — two instances sharing it fight over the session. The bridge detects
that (5+ connects in 60s), takes a distinct id to break the loop, and states plainly that
durability is now degraded. Stability wins, because an agent reconnecting every few seconds
processes nothing.

---

## Architecture

```
src/
├── index.ts              plugin entry: guards, wiring, lifecycle
├── types.ts              domain types (no runtime imports)
├── config.ts             every default, in one place
├── logger.ts             prefixing and the alert level
├── mesh/
│   ├── topics.ts         the whole address space — pure
│   ├── payload.ts        result normalisation, prompt rendering — pure
│   ├── catalog.ts        services.json and its file watch
│   ├── vars.ts           ${VAR} layering and the 0600 store
│   ├── jobs.ts           job state and the timeline ring
│   ├── transport.ts      session, stable clientId, collision recovery
│   ├── dispatch.ts       dispatch, cancel, watchdog
│   └── registry.ts       retained profile and config actions
└── http/
    ├── auth.ts           token levels and CSRF
    ├── sse.ts            event fan-out
    └── server.ts         routing
web/index.html            the console: one file, no external assets
test/                     unit tests + console render check
tools/screenshots.mjs     regenerates the images in this README
```

**Execution.** Jobs are pushed straight into an isolated executor subagent — no queue, so a busy
main session cannot swallow one. A watchdog sweeps every 60 seconds, and its liveness signal is
whether the executor's *run has settled*, not whether it has been chatty. A run still in flight
is alive by definition, and re-dispatch additionally requires genuine silence — otherwise a
long, quiet job gets duplicated, which is exactly the failure this design exists to avoid.

**Cancellation is cooperative.** The runtime exposes no abort primitive, so in-flight work may
run to completion internally. What the mesh guarantees is the contract clients depend on: a
terminal result lands immediately, and later executor output is suppressed.

---

## Development

```bash
npm install
npm run build            # tsc + copy web assets
npm test                 # build, then unit tests + console render check
npm run typecheck
node tools/screenshots.mjs
```

The tests import the **built** output, so a missing `.js` extension in an ESM import fails in CI
rather than at gateway start. The console check executes every view function against fixtures in
a stubbed DOM — fetching the HTML and grepping it proves the file is served, but never runs a
line of it, which is how a `ReferenceError` in a view once reached production.

Fixtures in `test/fixtures/` are synthetic and deterministic: no live topics, ids or repository
names, and no wall-clock dependence.

---

## Honest limitations

Things that will bite you, stated plainly rather than discovered later.

**Identity is not authenticated.** `requestedBy` is a self-declared string. MQTT delivers topic
and payload only — a publisher's broker identity does not travel with the message — so anyone
with broker credentials can claim any owner scope. Owner scoping is a **convention that keeps
honest clients from seeing each other's traffic**, not a security boundary.

This is fine for one team's private mesh. It is not sufficient for agents belonging to parties
who don't already trust each other. The fix is broker-side — EMQX ACLs, or rule-engine
enrichment feeding `mesh.verifyOwner` — which means the protocol delegates its hardest problem
to your deployment. Know that going in.

**Delegation holds sessions open.** An asking agent waits for its answer, so a four-deep chain
occupies four agent sessions simultaneously. `mesh.maxDepth` bounds it, but this model favours
depth over breadth and won't fan out to dozens of peers.

**Terminal results depend on the executor.** The protocol guarantees the *shape* and retention of
a result, and normalises it at the transport boundary. It cannot guarantee an executor publishes
one at all. A watchdog re-dispatches silent jobs, but that is a safety net, not a guarantee —
and re-dispatch can duplicate work if a capability isn't idempotent.

**One implementation, one deployment.** This runs daily against a real workload, which is more
than a prototype and much less than battle-tested. Expect to find things.

**Requires a broker you operate.** No hosted option. That's a feature if you care where your
job payloads go, and friction if you wanted to try it in five minutes.

**No client library yet.** Publishing a job is `mosquitto_pub` or your language's MQTT client.
Fine for CI and scripts; more ceremony than it should be for everyday use.

## Roadmap

In rough order of how much they'd unlock:

- **A client library.** One-line publish-and-await in TypeScript and Python. The biggest
  adoption gap today.
- **Verified identity end to end.** A working EMQX rule-engine recipe feeding `mesh.verifyOwner`,
  so owner scoping becomes enforcement rather than convention.
- **Non-blocking delegation.** Let an asking agent hand off instead of waiting, for fan-out
  shapes the current model can't serve.
- **Implementations beyond OpenClaw.** The protocol has no dependency on it; the spec is small
  enough to implement against in an afternoon, and a second implementation would prove that.

---

## License

Not yet licensed — all rights reserved. Open an issue if you would like to use this.
