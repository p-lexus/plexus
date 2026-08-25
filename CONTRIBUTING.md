# Contributing

Thanks for taking a look. This project is small and opinionated; the notes below are less about
process and more about the invariants that are easy to break by accident.

## Getting set up

```bash
npm install
cp services.example.json services.json
npm test                 # build, unit tests, console render check, package tests

# the Python host plugin, if you touch it
pip install "paho-mqtt>=2.1"
python hosts/hermes/tests/test_plugin.py
```

The tests run without a broker — the end-to-end ones skip themselves. Start `mosquitto -p 1883`
to get them, and note that they are the only tests that exercise durability, delegation across a
real session, and interoperability between implementations.

## How this repo relates to a running agent

The repository **is** the deployment. An installed Plexus agent is a git clone of this repo, so
there is exactly one source of truth and no drift between what you read and what runs.

```
  this repo  ──git pull──▶  ~/.openclaw/extensions/mqtt-bridge  ──▶  running agent
                                   (a clone, not a copy)
```

Changes reach a running agent **through git**, never by editing it in place:

```bash
# in your working copy
npm test && git commit && git push

# on the machine running the agent
cd ~/.openclaw/extensions/mqtt-bridge
git pull && npm run build
launchctl kickstart -k gui/$(id -u)/ai.openclaw.gateway   # code needs a restart
```

Editing the installed copy directly means the next `git pull` either conflicts or silently
discards your work, and the repo stops describing what is actually deployed. It also means a bad
change can't be undone with `git checkout`.

**What legitimately differs per install**, and is therefore gitignored:

| Path | Why it is local |
|---|---|
| `services.json` | Your capability catalog. `services.example.json` is what ships |
| `mesh.local.json` | Deployment variables, `0600`, never committed |
| `dist/`, `node_modules/` | Build output |
| `~/.openclaw/openclaw.json` | Host config — outside the repo entirely |

Everything else is tracked, so `git status` in an installed agent should be clean. If it isn't,
something was edited in place.

## Layout

| Path | What lives there |
|---|---|
| `PROTOCOL.md` | The specification. Behaviour changes belong here too |
| `packages/agent/` | `plexus-agent` — the client library, the plugin contract, the host |
| `packages/notify/` | `plexus-notify` — a plugin, and the worked example of one |
| `hosts/openclaw/src/mesh/` | Protocol behaviour: topics, dispatch, transport, catalog, variables |
| `hosts/openclaw/src/http/` | The console's API, auth and event stream |
| `web/index.html` | The console — one file, no build step, no external assets |
| `hosts/hermes/` | Host plugin for Hermes Agent — Python, an independent implementation |
| `test/` | Unit tests, the console render check, and package/end-to-end tests |
| `docs/HOSTS.md` | How to put another agent platform on the mesh |
| `docs/INSTALL.md` | Installation for every path. Update it when a step changes |

The repository is an npm workspace: `npm install` links `plexus-agent` and `plexus-notify` into
`node_modules`, so the tests and examples import them by package name exactly as a user would.

**`hosts/openclaw/` is a host plugin, not the runtime.** It is one participant on the mesh, and
so is `hosts/hermes/`. If a change only makes sense for one platform it belongs in that host; if
it is protocol behaviour it belongs in `PROTOCOL.md` and in every implementation.

`hosts/openclaw/src/mesh/topics.ts` and `.../payload.ts` are pure. Keep them that way — they are the
easiest parts of the system to reason about, and the most load-bearing.

## Invariants worth knowing

These each exist because breaking them caused a real failure:

**The client id must be stable across restarts.** It is what makes `clean: false` meaningful. A
pid or timestamp in the id means every restart is a new MQTT session, the broker's queued
messages stay orphaned with the dead one, and jobs published while the agent was down are lost
silently.

**`${VAR}` expands before `{{args}}`.** Reversed, a caller passes `"${SOME_SECRET}"` as an
argument value and the bridge expands it — turning every invoke into an environment read.

**Deployment values are resolved at dispatch, never in the catalog.** The catalog is published
to a *retained* topic. Resolving there broadcasts every deployment secret to anyone subscribed
to the registry.

**Job topics are always owner-scoped.** There is no unscoped `jobs/<jobId>/…` form. A result
belongs to exactly one owner and is published to exactly one topic.

**`register()` runs per agent session, not once per process.** Dispatching a job re-registers
the plugin. Only a *rebuilt module* should take over the transport; anything else reconnects the
broker on every job.

**An alert nobody can see is not an alert.** Some deployments capture only info-level plugin
output, which is why `logger.alert` mirrors to info.

## Before you push

There is no CI. This project is deployed by pulling into a running gateway, so a broken push is
discovered on the machine doing real work — which makes the checklist below the only thing
standing between a bad commit and a broken agent.

```bash
mosquitto -p 1883 &        # FIRST — see below
npm test
python hosts/hermes/tests/test_plugin.py     # if you touched hosts/hermes
python hosts/hermes/tests/test_interop.py    # if you touched anything on the wire
```

**Start the broker first.** Every end-to-end test skips itself when none is reachable, so
`npm test` will happily print `48 passed` having quietly not run durability, delegation,
retained-replay suppression, or cross-language interop. A green suite that skipped the
interesting half is worse than a red one.

## Testing

`npm test` builds first and the tests import the **built** output, so a missing `.js` extension
in an ESM import fails here rather than at gateway start.

The console check executes every view function against fixtures in a stubbed DOM. Fetching the
HTML and grepping it proves the file is served but never runs a line of it — which is how a
`ReferenceError` in a view once reached production. If you add a view, add it to
`test/render-check.mjs`.

Fixtures in `test/fixtures/` are synthetic and use a fixed epoch. Keep them that way: no live
topics, ids or repository names, and no dependence on the wall clock.

## Generated artifacts

Three things in the repository are generated, and all three go stale silently:

| | Regenerate with | When |
|---|---|---|
| `docs/screenshots/*.png` | `node tools/screenshots.mjs` | the console's appearance changes |
| `docs/demo*.svg` | `node tools/record-demo.mjs examples/with-plugins.mjs docs/demo.svg` | an example changes |
| `PROTOCOL.pdf` | `npm install --no-save marked mermaid && npm run pdf` | `PROTOCOL.md` changes |

Screenshots and demos are produced from **real runs** with the network stubbed and the clock
frozen — so they cannot drift from what the code does, but they also will not update themselves.
Commit the result.

`marked` and `mermaid` are deliberately not dependencies: mermaid alone is 3.6 MB and this
repository is cloned onto machines that run agents.

## Changing the protocol

Anything observable on the wire — a topic, a payload field, a guarantee — is a protocol change:

1. Update `PROTOCOL.md`, including the version-history section.
2. Say what a client that does not know about the change will see.
3. Add a test if the change has a testable invariant.
4. **Update every implementation, or say why not.** There are three, sharing no code:
   `hosts/openclaw/` (TypeScript), `packages/agent` (JavaScript) and `hosts/hermes/` (Python). A change
   landing in one of them is how a specification quietly becomes a description of one program.
5. Regenerate the PDF: `npm install --no-save marked mermaid && npm run pdf`.

`hosts/hermes/tests/test_interop.py` runs a Python agent and a Node agent against one broker and
checks they can still work together. If a protocol change breaks that, it has broken
interoperability — which is the only thing this project actually sells.

`ownerPolicy` in the retained profile exists so clients can read what a deployment *actually*
enforces rather than inferring it from a version number. Prefer advertising a capability over
implying it.

## Commit messages

Explain **why**, not just what. A commit that says "fix watchdog" is much less useful in six
months than one that says the watchdog treated a settled run as a failed one, so long silent
jobs were duplicated.
