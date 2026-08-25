<div align="center">
  <img src="../../docs/logo/plexus-mark-light.svg#gh-light-mode-only" width="56" alt="">
  <img src="../../docs/logo/plexus-mark-dark.svg#gh-dark-mode-only" width="56" alt="">
  <h1>Plexus for OpenClaw</h1>
  <p>Put an OpenClaw agent on the mesh, so it can work with agents that aren't OpenClaw.</p>
</div>

---

This is a **host plugin**: it teaches OpenClaw to speak [Plexus](../../PROTOCOL.md). Once loaded,
your gateway's agent is a peer — other agents discover what it can do and send it work, and it can
ask them for help with anything outside its own expertise.

It is the reference implementation and the one running in production. It is also the oldest, which
is why most of the hard-won notes in [docs/HOSTS.md](../../docs/HOSTS.md) are about mistakes made
here first.

**Installation: [docs/INSTALL.md § OpenClaw](../../docs/INSTALL.md#2-openclaw).**

## Why this one lives differently

The other host plugin, [`hosts/hermes/`](../hermes), is a self-contained directory you copy into
`~/.hermes/plugins/`. This one isn't, and the difference is worth understanding before you go
looking for a `package.json` in here:

**The repository *is* the OpenClaw deployment.** An installed agent is a git clone of the repo
root, and OpenClaw reads the root `package.json`, `openclaw.plugin.json` and `dist/`. So the
manifest, the build output and the capability catalog stay at the top level; only the TypeScript
sources live here.

```
  repo root/
  ├── package.json            ← OpenClaw reads this ("extensions": ["./dist/index.js"])
  ├── openclaw.plugin.json    ← config schema the gateway validates against
  ├── services.json           ← your capability catalog (gitignored)
  ├── dist/                   ← build output, what actually gets loaded
  └── hosts/openclaw/src/     ← you are here
```

`npm run build` at the root compiles `hosts/openclaw/src/` into `dist/`. The output is unchanged
from when these files sat at `src/` — byte for byte — so the move that put them here was invisible
to running gateways.

## Layout

| | |
|---|---|
| `src/index.ts` | Plugin entry: registration guards, wiring, reload takeover |
| `src/config.ts` | Every default, in one place |
| `src/mesh/topics.ts` | The whole address space — **pure**, keep it that way |
| `src/mesh/payload.ts` | Result normalisation and prompt rendering — **pure** |
| `src/mesh/transport.ts` | The durable session, stable client id, collision recovery |
| `src/mesh/dispatch.ts` | Dispatch, delegation, cancel, watchdog |
| `src/mesh/catalog.ts` | `services.json` and its file watch |
| `src/mesh/vars.ts` | `${VAR}` layering and the `0600` store |
| `src/mesh/registry.ts` | The retained profile, and config actions |
| `src/http/` | The console's API, auth and event stream |

The console itself is [`web/index.html`](../../web/index.html) at the root — one file, no build
step, no external assets.

## What this plugin does that the protocol doesn't

Three things are specific to running inside a gateway, and each exists because its absence caused
a real failure:

**Registration runs per agent session, not once per process.** Dispatching a job re-registers the
plugin, so only a genuinely *rebuilt* module may take over the transport. Reconnecting on every
registration means reconnecting the broker on every job.

**Tools must be registered outside the transport guard.** OpenClaw's `tools.profile` is an
allowlist that excludes plugin-registered tools, and asking for the plugin's tool list happens in
a different registration mode than running it. Get the ordering wrong and executors run with no
mesh tools at all — producing plausible output and publishing nothing.

**The watchdog's liveness signal is run settlement, not chattiness.** A job that has been silent
for five minutes is usually a large job thinking, not a dead one. Re-dispatching on silence alone
duplicates exactly your most expensive work.

## Tests

Run from the repository root:

```bash
npm test        # unit tests, the console render check, package and end-to-end tests
```

`test/unit.mjs` imports the **built** output, so a missing `.js` extension in an ESM import fails
there rather than at gateway start.

---

Apache 2.0 · part of [Plexus](../../README.md) · writing one for another platform?
[docs/HOSTS.md](../../docs/HOSTS.md)
