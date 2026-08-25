<div align="center">
  <img src="../../docs/logo/plexus-mark-light.svg#gh-light-mode-only" width="56" alt="">
  <img src="../../docs/logo/plexus-mark-dark.svg#gh-dark-mode-only" width="56" alt="">
  <h1>Plexus for Hermes</h1>
  <p>Put a <a href="https://hermes-agent.nousresearch.com">Hermes Agent</a> on the mesh, so it can work with agents that aren't Hermes.</p>
</div>

---

This is a **host plugin**: it teaches Hermes to speak [Plexus](../../PROTOCOL.md). Once loaded,
your Hermes agent is a peer — other agents discover what it can do and send it work, and it can
ask them for help with anything outside its own expertise.

The agents on the other side don't have to be Hermes. In the test suite, a Hermes agent and a
JavaScript agent delegate to each other in both directions with nothing in common but the
specification.

## Install

```bash
git clone https://github.com/MoGhali/plexus
cp -r plexus/hosts/hermes ~/.hermes/plugins/plexus
pip install "paho-mqtt>=2.1"
```

Then tell it where the mesh is — `~/.hermes/plexus.json`:

```json
{
  "broker": "mqtt://localhost:1883",
  "agentId": "hermes",
  "displayName": "Hermes — research and analysis",

  "capabilities": [
    {
      "service": "research.summarise",
      "description": "Researches a topic and returns a sourced summary.",
      "requestSchema": { "topic": "string", "depth": "string? (quick|thorough)" },
      "prompt": "Research {{topic}} at {{depth}} depth. Return JSON with keys: summary, sources, confidence."
    }
  ]
}
```

Restart Hermes. It's on the mesh.

Credentials use `${VAR}` and resolve from the environment, so the file is safe to commit:

```json
{ "username": "${MQTT_USERNAME}", "password": "${MQTT_PASSWORD}" }
```

No broker configured means the plugin stays quietly offline rather than failing — a mesh being
unreachable is not a reason to stop someone using their agent.

## What a capability is

A name, an argument schema, and a prompt template. **No code, no deploy, no restart of anything
but Hermes.** The plugin contains no service name anywhere; it renders the template and hands it
to the agent. Everything domain-specific lives in your config.

Three kinds of placeholder, and **the order they expand in is a security property**:

| | Filled from | |
|---|---|---|
| `${VAR}` | `promptVars`, then the environment | **first** |
| `{{jobId}}` `{{requestedBy}}` | the job | second |
| `{{topic}}` … | the caller's arguments | **last** |

Environment expansion happens before argument substitution. Reversed, a caller passes
`"${MQTT_PASSWORD}"` as an argument value, the next pass expands it, and every invoke becomes an
arbitrary environment read by anyone who can publish to the mesh. There's a test for it.

The config is also checked at load: a prompt using `{{repo}}` with no `repo` argument is refused,
because it would render empty and silently produce a bad job.

## Tools your agent gets

| | |
|---|---|
| `mesh_publish` | Report progress or the final result of a mesh job |
| `mesh_peers` | Who else is on the mesh, and what they can do |
| `mesh_ask` | Ask another agent for something, and wait for the answer |
| `mesh_status` | Connection, capabilities, peers, active jobs |

`mesh_ask` is the interesting one. When a request needs expertise your agent doesn't have, it
finds a peer and delegates — and the lineage (`parentJobId`, `rootJobId`, `depth`) is filled in
automatically, so a chain through five agents stays traceable to the one request that started it,
and cancelling the root cancels all of it.

## How a job actually runs

This is the only genuinely Hermes-specific part, and Hermes offers two routes that differ in the
one way that matters: whether you get the answer back.

### `api` — preferred

Posts to Hermes' OpenAI-compatible `/v1/chat/completions`. The turn is isolated and the response
returns synchronously, so the job settles as soon as the agent finishes.

```json
{ "executor": "api", "apiUrl": "http://127.0.0.1:8000/v1", "model": "hermes" }
```

Requires the API server enabled in your Hermes config. Use this when you can.

### `inject` — the fallback

`ctx.inject_message()` pushes the job into a session. Nothing comes back: the agent works in its
own time and reports by calling `mesh_publish`. The briefing tells it so, in as many words —
*"nothing you say in chat reaches the requester, only that tool call does."*

This depends on the agent **choosing** to call the tool, which is a real weakness. A watchdog
(`maxJobSeconds`, default 30 min) publishes a terminal error if it never does, so a requester is
never left waiting forever — but a timed-out job is worse than a real answer.

`"executor": "auto"` (the default) uses `api` when `apiUrl` is set, else `inject`.

## Options

| | Default | |
|---|---|---|
| `broker` | — | `mqtt://host:1883`. Empty means stay offline |
| `root` | `agents` | Topic root; isolates one mesh from another |
| `agentId` | `hermes` | Your identity on the mesh |
| `executor` | `auto` | `api`, `inject`, or `auto` |
| `maxDepth` | `4` | Delegation hop limit — stops cycles |
| `askTimeoutSeconds` | `300` | How long a delegated ask waits |
| `maxJobSeconds` | `1800` | Watchdog for `inject` mode |
| `durable` | `true` | `clean_session=False` persistent session |
| `clientId` | derived | **Set this only if two Hermes instances share an `agentId`** |
| `promptVars` | `{}` | Deployment values for `${VAR}` |

## Durability

The plugin holds one persistent MQTT session: QoS 1 throughout, retained profile, last-will
presence, 5-second reconnect. **A job published while Hermes is shut down is queued by the broker
and delivered when it starts again.**

That depends entirely on the client id being stable across restarts, so it's derived from
hostname + mesh root + agent id — never a pid. With a changing id every restart is a *new*
session, the broker's queued messages stay orphaned with the dead one, and work published while
you were away is lost silently.

If you genuinely run two Hermes instances with the same `agentId`, set `clientId` on one. They'd
otherwise fight over one session and kick each other, which looks exactly like a flaky network.

## Tests

```bash
pip install "paho-mqtt>=2.1"
mosquitto -p 1883 &

python hosts/hermes/tests/test_plugin.py     # the plugin, with Hermes faked
python hosts/hermes/tests/test_interop.py    # a Hermes agent and a JS agent, together
```

Only `PluginContext` is faked. The mesh, the broker, the durable session, the job lifecycle and
every tool handler are real — so these cover the whole plugin except the one thing that needs
Hermes installed: that Hermes really calls `register(ctx)` with a context shaped like the fake.

`test_interop.py` is the one worth reading. It runs a Python Hermes agent and a Node agent against
one broker and checks that each can discover, delegate to, and answer the other, with lineage
intact across the language boundary.

## Known limits

**Not yet run against a live Hermes.** The protocol half is thoroughly tested and interoperates
with the JavaScript implementation; the Hermes-facing half is written against the
[published plugin API](https://hermes-agent.nousresearch.com/docs/user-guide/features/plugins) and
verified against a faithful fake. If your Hermes build differs, the failure will be at
`register()` and the log will say so.

**No shutdown hook.** Hermes documents no stop lifecycle event, so the connection is closed at
process exit via `atexit`. A hard kill leaves the broker to publish the last will, which is what
it's for.

**Cancellation is cooperative.** A cancelled job stops producing mesh traffic immediately and its
result is suppressed, but an agent already mid-turn keeps going internally. Treat `cancelled` as
"no further traffic will be honoured", not "the compute stopped".

---

Apache 2.0 · part of [Plexus](../../README.md) · writing one for another platform?
[docs/HOSTS.md](../../docs/HOSTS.md)
