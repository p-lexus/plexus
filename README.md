# Agent Mesh Protocol

An MQTT protocol for dispatching jobs to autonomous agents, plus a reference implementation
as an [OpenClaw](https://github.com/openclaw) plugin.

The full specification is in **[PROTOCOL.md](PROTOCOL.md)** (also available as
[PROTOCOL.pdf](PROTOCOL.pdf)).

## What it is

A human or another agent publishes a job to an MQTT topic. An agent picks it up, executes it in
an isolated subagent, and publishes progress and a terminal result back on owner-scoped topics.
The requester subscribes to their own scope and sees only their own traffic.

```
you ──invoke──▶  agents/commands/<agentId>/invoke
                        │
                        ▼  executor subagent
agents/jobs/<owner>/<jobId>/events    ◀── progress milestones
agents/jobs/<owner>/<jobId>/result    ◀── terminal payload (retained)
```

Because the transport is MQTT, the agent only ever makes an **outbound** connection — it works
from behind NAT with no inbound ports and no tunnel.

## Design principles

**Transport in the framework, logic in the agent.** The bridge moves jobs. It does not know
what any job means.

**Capabilities are data, not code.** A capability is a JSON entry in `services.json` with a
prompt template. The bridge contains no service name anywhere — not `code.review`, not
anything else. Add, change or remove capabilities at runtime over the `config` command; no
restart, no code change. The catalog in this repo is an example, not the product.

**The bridge carries jobs, not credentials.** It holds the broker login and an optional panel
token, nothing more. No GitHub, cloud or third-party auth lives in the plugin; executors
authenticate with the host's own identity. See *Credentials & scope boundary* in the spec.

**Push, not poll.** A persistent MQTT session with a stable client id delivers jobs; the
executor is spawned the instant one arrives; the panel is fed by Server-Sent Events. Nothing on
a delivery path polls.

## Install

```bash
git clone https://github.com/MoGhali/agent-mesh-protocol.git
cd agent-mesh-protocol
npm install
npm run build
```

Then point OpenClaw at it — drop the directory in `~/.openclaw/extensions/` and enable it:

```jsonc
{
  "plugins": {
    "allow": ["agent-mesh-bridge"],
    "entries": {
      "agent-mesh-bridge": {
        "enabled": true,
        "config": {
          "broker": {
            "url": "mqtt://broker.example.com:1883",
            "username": "mesh_user",
            "password": "${MQTT_PASSWORD}"    // ${ENV_VAR} is resolved at runtime
          },
          "mesh": {
            "root": "agents",
            "agentId": "my-agent"
          }
        }
      }
    }
  }
}
```

Every option is documented in `openclaw.plugin.json`. Only `broker.url` is required.

### Client id and durability

The client id defaults to a hash of hostname + install path so it is **stable across
restarts** — that is what makes `clean: false` meaningful and lets the broker queue jobs
published while the agent is offline. If you run two instances from the same directory, set
`broker.clientId` explicitly on one; otherwise they fight over the session and the bridge will
log a kick-loop warning.

## Web panel

A local control panel is served on `127.0.0.1:8765` (configurable). It lists the capability
catalog, dispatches invokes, streams live job state over SSE, and edits the catalog. Set
`web.auth` to require a bearer token, or `web.enabled: false` to turn it off.

## Status

Protocol version **1.2**. The specification and this implementation are versioned together;
`ownerPolicy` in the retained profile tells clients what a given deployment actually enforces,
so you never have to infer behaviour from the version number.

## License

Not yet licensed — all rights reserved. Open an issue if you want to use this.
