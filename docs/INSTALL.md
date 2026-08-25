# Installation

Everything needs the same two things: **a broker**, and **something that speaks the protocol**.

## The short way

macOS and Linux, and Windows under Git Bash or WSL:

```bash
git clone https://github.com/MoGhali/plexus && cd plexus
./install.sh
```

It looks for OpenClaw and Hermes, installs the right host plugin for whichever it finds, and
prints the one manual step each needs.

```
./install.sh --dry-run                      print actions, change nothing
./install.sh --target hermes                skip detection
./install.sh --broker mqtt://host:1883      point it somewhere other than localhost
./install.sh --agent-id dba                 name this agent on the mesh
```

**Re-run it to update.** It pulls, rebuilds, and compares the compiled output with what was there
before — so it restarts the gateway when the plugin's code actually changed, and leaves a running
agent alone when it didn't.

**It will not overwrite anything of yours.** An existing `services.json`, `plexus.json` or
capability catalog is kept as-is and reported as kept; the installer only ever creates what is
missing. Re-running it is safe.

On **Windows PowerShell** there is no script — the steps are short enough to do by hand and are
written out below. `install.sh` is bash, and rather than ship an untested PowerShell port, this
page gives you the commands it would have run.

---

## Where things live

Everything below differs by platform in exactly two ways: the path, and how you restart a
service. Both are in this table.

| | macOS | Linux | Windows |
|---|---|---|---|
| OpenClaw plugin | `~/.openclaw/extensions/mqtt-bridge` | same | `%USERPROFILE%\.openclaw\extensions\mqtt-bridge` |
| OpenClaw config | `~/.openclaw/openclaw.json` | same | `%USERPROFILE%\.openclaw\openclaw.json` |
| Hermes plugin | `~/.hermes/plugins/plexus` | same | `%USERPROFILE%\.hermes\plugins\plexus` |
| Hermes config | `~/.hermes/plexus.json` | same | `%USERPROFILE%\.hermes\plexus.json` |
| Restart the gateway | `openclaw gateway restart` | same | same |
| Gateway logs | `/tmp/openclaw/openclaw-<date>.log` | same | `%TEMP%\openclaw\openclaw-<date>.log` |

`openclaw gateway restart` drives launchd, systemd or Task Scheduler depending on the platform, so
it is the same command everywhere. `openclaw gateway status` prints which one it is managing, and
the exact log path.

**Prerequisites:** Node 18+ and git for OpenClaw; Python 3.9+ and pip for Hermes. On Windows,
install Node and Python from their own installers or `winget`.

---

## 1. A broker

Any MQTT 3.1.1 or 5 broker. [Mosquitto](https://mosquitto.org/) and [EMQX](https://www.emqx.io/)
both work; so does a managed one.

**Docker — identical on all three platforms:**

```bash
docker run -d --name mosquitto -p 1883:1883 eclipse-mosquitto:2 \
  sh -c 'printf "listener 1883\nallow_anonymous true\n" > /m.conf && mosquitto -c /m.conf'
```

**Without Docker:**

| | |
|---|---|
| macOS | `brew install mosquitto && mosquitto -p 1883` |
| Debian/Ubuntu | `sudo apt install mosquitto && mosquitto -p 1883` |
| Fedora | `sudo dnf install mosquitto && mosquitto -p 1883` |
| Windows | `winget install EclipseFoundation.Mosquitto`, then `mosquitto.exe -p 1883` |

**Anonymous access is fine on localhost and nowhere else.** For anything shared, give each agent
its own broker credentials. Owner isolation in Plexus is a convention between honest clients, not
a security boundary — the boundary is your broker's ACLs. See
[Security model](../README.md#security-model) before exposing one.

Check it's up — leave this running in another terminal:

```bash
mosquitto_sub -h localhost -t 'agents/#' -v
```

Every step below publishes to `agents/…`, so that window is your proof anything worked.

---

## 2. OpenClaw

The repository **is** the deployment — an installed agent is a git clone, so there is one source
of truth and no drift.

### macOS and Linux

```bash
git clone https://github.com/MoGhali/plexus.git ~/.openclaw/extensions/mqtt-bridge
cd ~/.openclaw/extensions/mqtt-bridge
npm install
cp services.example.json services.json     # your capability catalog
npm run build
```

### Windows (PowerShell)

```powershell
git clone https://github.com/MoGhali/plexus.git "$env:USERPROFILE\.openclaw\extensions\mqtt-bridge"
cd "$env:USERPROFILE\.openclaw\extensions\mqtt-bridge"
npm install
Copy-Item services.example.json services.json
npm run build
```

`services.json` is gitignored, so `git pull` never collides with your own capabilities.

### Configure — every platform

In `openclaw.json` (see the path table above). The file allows `//` comments:

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

### Restart and verify — every platform

Plugin *code* needs a restart; config hot-reloads.

```bash
openclaw config validate      # ALWAYS. An invalid config stops the gateway starting at all.
openclaw gateway restart
openclaw gateway status
```

Then confirm the agent announced itself:

```bash
mosquitto_sub -h localhost -t 'agents/registry/+/profile' -C 1
```

and that the executor really has the mesh tools — this must list all three:

```bash
openclaw agent --agent main -m 'List every tool you have starting with "mesh" or "mqtt".'
```

If it doesn't, revisit `tools.alsoAllow`. Finally, open `http://127.0.0.1:8765` and sign in with
your `web.auth` token.

---

## 3. Hermes

[Hermes Agent](https://hermes-agent.nousresearch.com) loads Python plugins from its plugins
directory.

### macOS and Linux

```bash
git clone https://github.com/MoGhali/plexus
mkdir -p ~/.hermes/plugins
cp -r plexus/hosts/hermes ~/.hermes/plugins/plexus
pip install "paho-mqtt>=2.1"
```

### Windows (PowerShell)

```powershell
git clone https://github.com/MoGhali/plexus
New-Item -ItemType Directory -Force "$env:USERPROFILE\.hermes\plugins" | Out-Null
Copy-Item -Recurse plexus\hosts\hermes "$env:USERPROFILE\.hermes\plugins\plexus"
pip install "paho-mqtt>=2.1"
```

If pip refuses because Python is externally managed (common on Linux, and on macOS via Homebrew):

```bash
pip install --user --break-system-packages "paho-mqtt>=2.1"
# or, if Hermes runs from a pipx install:
pipx inject hermes paho-mqtt
```

The plugin has to import `paho.mqtt` **from the interpreter Hermes itself runs under**, so install
it into that environment rather than a different one. `ModuleNotFoundError: paho` almost always
means it went somewhere else.

### Configure — every platform

Create `plexus.json` in your Hermes directory (see the path table):

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

Restart Hermes, then look in its log for:

```
plexus: hermes online on mqtt://localhost:1883 (root 'agents', executor 'api') offering research.summarise
```

No broker configured means the plugin stays quietly offline rather than failing — a mesh being
unreachable shouldn't stop you using your agent.

> **Not yet run against a live Hermes.** The protocol half is tested and interoperates with the
> JavaScript implementation; the Hermes-facing half is written against their published plugin API
> and verified against a faithful fake. If your build differs it will fail at `register()` and say
> so in the log — it cannot take Hermes down.

---

## 4. Your own agent

Any Node process, no framework, any platform:

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

From another language there's no packaged library yet — implement [PROTOCOL.md](../PROTOCOL.md)
directly. [`hosts/hermes/protocol.py`](../hosts/hermes/protocol.py) is a complete, working example
of doing exactly that in about 400 lines.

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

An OpenClaw agent and a Hermes agent on one machine is a normal setup: two processes, two registry
entries, one broker, and each able to delegate to the other. Check who's there:

```bash
mosquitto_sub -h localhost -t 'agents/registry/+/profile' -v
```

---

## Updating

```bash
./install.sh --yes --target openclaw
```

It pulls, rebuilds, and restarts **only if the compiled output changed** — so a docs-only update
leaves a running agent alone. By hand:

```bash
cd ~/.openclaw/extensions/mqtt-bridge && git pull && npm install && npm run build
openclaw config validate && openclaw gateway restart      # only if the code changed
```

For Hermes, re-copy the directory and restart Hermes. Your `plexus.json` lives outside the plugin
directory, so it survives.

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
| Gateway won't start after a config edit | Run `openclaw config validate` — an invalid config stops it entirely |
| `ModuleNotFoundError: paho` | Installed into a different interpreter than the one Hermes runs |

Gateway logs are at `/tmp/openclaw/openclaw-<date>.log` (`%TEMP%\openclaw\…` on Windows), and
`openclaw gateway status` prints the exact path it is using.

Still stuck? [Open an issue](https://github.com/MoGhali/plexus/issues) with your config (redact
credentials) and what `mosquitto_sub -t 'agents/#' -v` shows.
