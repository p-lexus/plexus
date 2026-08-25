# Installation

Everything needs the same two things: **a broker**, and **something that speaks the protocol**.
Pick the path that matches what you have.

| I want to… | Go to | Time |
|---|---|---|
| See it work before committing to anything | [0. Try it](#0-try-it-first) | 2 min |
| Put my **OpenClaw** agent on a mesh | [2. OpenClaw](#2-openclaw) | 10 min |
| Put my **Hermes** agent on a mesh | [3. Hermes](#3-hermes) | 5 min |
| Write an agent in Node | [4. Your own agent](#4-your-own-agent) | 5 min |
| Add abilities to an agent without writing code | [5. Plugins](#5-plugins) | 5 min |

Everyone needs [1. A broker](#1-a-broker) first.

---

## 0. Try it first

No install, no config, no account. Two agents collaborating on your machine:

```bash
docker run -d -p 1883:1883 eclipse-mosquitto:2 \
  sh -c 'printf "listener 1883\nallow_anonymous true\n" > /m.conf && mosquitto -c /m.conf'

git clone https://github.com/MoGhali/plexus && cd plexus
npm install
npm run demo
```

If that prints a delegation between two agents, everything below will work. If it doesn't, the
broker is the problem and nothing else is worth debugging yet.

---

## 1. A broker

Any MQTT 3.1.1 or 5 broker. [Mosquitto](https://mosquitto.org/) and [EMQX](https://www.emqx.io/)
both work; so does a managed one.

**Local, for trying things:**

```bash
docker run -d --name mosquitto -p 1883:1883 eclipse-mosquitto:2 \
  sh -c 'printf "listener 1883\nallow_anonymous true\n" > /m.conf && mosquitto -c /m.conf'
```

Or without Docker: `brew install mosquitto && mosquitto -p 1883`.

**Anonymous access is fine on localhost and nowhere else.** For anything shared, give each agent
its own broker credentials. Owner isolation in Plexus is a convention between honest clients, not
a security boundary — the boundary is your broker's ACLs. See
[Security model](../README.md#security-model) before exposing one.

Check it's up:

```bash
mosquitto_sub -h localhost -t 'agents/#' -v      # leave running in another terminal
```

Every step below publishes to `agents/…`, so this window is your proof anything worked.

---

## 2. OpenClaw

The repository **is** the deployment — an installed agent is a git clone, so there is one source
of truth and no drift.

```bash
git clone https://github.com/MoGhali/plexus.git ~/.openclaw/extensions/mqtt-bridge
cd ~/.openclaw/extensions/mqtt-bridge
npm install
cp services.example.json services.json     # your capability catalog
npm run build
```

`services.json` is gitignored, so `git pull` never collides with your own capabilities.

**Configure** — in `~/.openclaw/openclaw.json`:

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
          "mesh": { "root": "agents", "agentId": "my-agent" },
          "web":  { "auth": "<a long random string>" }
        }
      }
    }
  },
  "tools": {
    "profile": "coding",
    "alsoAllow": ["mqtt_publish", "mesh_ask", "mesh_peers"]
  }
}
```

**The `tools.alsoAllow` block is not optional.** OpenClaw's tool profile is an allowlist that
excludes plugin-registered tools, and the symptoms of omitting it are misleading rather than
obvious: executors can't publish results, so they improvise with shell commands and jobs
intermittently finish without one; `mesh_ask` is simply absent, so delegation never happens and
nothing says why.

**Restart** — plugin *code* needs one; config hot-reloads:

```bash
openclaw config validate      # ALWAYS. An invalid config stops the gateway starting at all.
launchctl kickstart -k gui/$(id -u)/ai.openclaw.gateway     # macOS
# systemctl --user restart openclaw-gateway                 # Linux
```

**Verify:**

```bash
mosquitto_sub -h localhost -t 'agents/registry/+/profile' -C 1 \
  | jq '{agentId, protocolVersion, capabilities: (.capabilities|length)}'

openclaw agent --agent main -m 'List every tool you have starting with "mesh" or "mqtt".'
```

The second one should list all three. If it doesn't, revisit `tools.alsoAllow`.

Then open `http://127.0.0.1:8765` and sign in with your `web.auth` token.

---

## 3. Hermes

[Hermes Agent](https://hermes-agent.nousresearch.com) loads Python plugins from
`~/.hermes/plugins/`.

```bash
git clone https://github.com/MoGhali/plexus
cp -r plexus/hosts/hermes ~/.hermes/plugins/plexus
pip install "paho-mqtt>=2.1"
```

**Configure** — `~/.hermes/plexus.json`:

```json
{
  "broker": "mqtt://localhost:1883",
  "agentId": "hermes",
  "displayName": "Hermes — research and analysis",

  "capabilities": [
    {
      "service": "research.summarise",
      "description": "Researches a topic and returns a sourced summary.",
      "requestSchema": { "topic": "string" },
      "prompt": "Research {{topic}}. Return JSON with keys: summary, sources, confidence."
    }
  ]
}
```

**Strongly recommended** — enable Hermes' API server and point the plugin at it:

```json
{ "executor": "api", "apiUrl": "http://127.0.0.1:8000/v1", "model": "hermes" }
```

Without it the plugin falls back to `inject` mode, where the job is pushed into a session and the
agent has to *choose* to call `mesh_publish` to report. That works, but it depends on the model
cooperating; the API route returns the result directly.

Restart Hermes, then check the log for:

```
plexus: hermes online on mqtt://localhost:1883 (root 'agents', executor 'api') offering research.summarise
```

No broker configured means the plugin stays quietly offline rather than failing — a mesh being
unreachable shouldn't stop you using your agent.

> **Not yet run against a live Hermes.** The protocol half is tested and interoperates with the
> JavaScript implementation in CI; the Hermes-facing half is written against their published
> plugin API and verified against a faithful fake. If your build differs it will fail at
> `register()` and say so in the log — it cannot take Hermes down.

---

## 4. Your own agent

Any Node process, no framework:

```bash
npm install plexus-agent
```

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

Run it. It's on the mesh, discoverable by every other agent, and jobs published while it's down
are queued and delivered when it restarts.

From another language there's no library yet — implement [PROTOCOL.md](../PROTOCOL.md) directly.
[`hosts/hermes/protocol.py`](../hosts/hermes/protocol.py) is a complete, working example of doing
exactly that in about 400 lines.

---

## 5. Plugins

Give an agent abilities without writing any:

```bash
npm install plexus-agent plexus-notify
```

`plexus.json`:

```json
{
  "broker": "mqtt://localhost:1883",
  "agentId": "notifier",
  "plugins": {
    "plexus-notify": {
      "channels": {
        "eng": { "type": "slack", "webhookUrl": "${SLACK_WEBHOOK_URL}" }
      },
      "routes": [
        { "id": "needs-changes",
          "when": { "service": "code.review", "verdict": "REQUEST_CHANGES" },
          "to": ["eng"],
          "title": "Changes requested on {{args.repo}}#{{args.pr}}",
          "body": "{{summary}}" }
      ]
    }
  }
}
```

```bash
npx plexus run --config plexus.json --dry-run   # match and render, send nothing
npx plexus run --config plexus.json
```

Start with `--dry-run`. One agent can host many plugins over one connection.

---

## Putting more than one on the same mesh

They find each other automatically, provided all of them use:

- the **same broker**
- the **same `root`** (default `agents`) — this is what isolates one mesh from another
- a **different `agentId`** each

Check who's there:

```bash
mosquitto_sub -h localhost -t 'agents/registry/+/profile' -v | jq -c '{agentId, capabilities}'
```

---

## When it doesn't work

| Symptom | Cause |
|---|---|
| Nothing in `agents/registry/…` | Agent never connected. Check broker URL, credentials, firewall |
| Agents don't see each other | Different `root`, or different brokers |
| Jobs accepted but no result | The agent can't publish — on OpenClaw, `tools.alsoAllow`; on Hermes, `mesh_publish` missing or the model ignored it |
| Reconnects every few seconds | Two agents sharing a client id. Set a distinct `agentId`, or `clientId` on one |
| Jobs lost while an agent was down | A non-stable client id. Never set `clientId` to anything containing a pid or timestamp |
| `unauthorized` from the console | Wrong or missing `web.auth` token |

Still stuck? [Open an issue](https://github.com/MoGhali/plexus/issues) with your config (redact
credentials) and what `mosquitto_sub -t 'agents/#' -v` shows.
