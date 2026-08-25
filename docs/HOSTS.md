# Putting a platform on the mesh

How to write a Plexus plugin for an agent platform — the role [`hosts/openclaw/`](../hosts/openclaw)
plays for OpenClaw, and [`hosts/hermes/`](../hosts/hermes) plays for Hermes Agent.

This is the document to read when you want *your* platform's agents to delegate to, and receive
work from, agents running on someone else's.

```
                        ┌──────────── the mesh ────────────┐
   OpenClaw agent ──────┤                                  ├────── your platform's agent
  via hosts/openclaw/   │      MQTT broker · Plexus        │        via your plugin
                        └──────────────────────────────────┘
```

Both sides speak [PROTOCOL.md](../PROTOCOL.md). Neither knows what the other is running, and
that is the entire point — a plugin is a **translation layer**, not a runtime.

---

## What a host plugin actually does

Four jobs, and no more than four:

| | The plugin's job | In OpenClaw |
|---|---|---|
| **1. Register** | Get loaded by the platform and stay loaded | `definePluginEntry` + `api.registerTool` |
| **2. Connect** | Hold one durable MQTT session for the process lifetime | `hosts/openclaw/src/mesh/transport.ts` |
| **3. Execute** | Turn an arriving `invoke` into a real agent turn, and get the result | `api.runtime.subagent` |
| **4. Report** | Publish milestones and one terminal result, owner-scoped | `hosts/openclaw/src/mesh/dispatch.ts` |

Everything else — discovery, delegation, lineage, hop limits, cancellation, retained profiles —
is protocol behaviour that [`plexus-agent`](../packages/agent) already implements. **Don't
reimplement it.** A host plugin should be mostly adapter code, and if yours isn't, something that
belongs in the library has leaked into it.

## The three questions to answer about your platform

Before writing anything, find these. They determine the whole shape of the plugin, and two of the
three caused real bugs in the OpenClaw one.

### 1. How does a plugin start something long-lived?

Plexus needs one MQTT connection held open for the life of the process. Some platforms give you a
clean lifecycle hook; some only call you per-request, in which case you need a module-level
singleton and a way to detect reload.

> **The OpenClaw trap.** Its plugin `register()` runs **once per agent session**, not once per
> process — so dispatching a job re-registers the plugin. Naively reconnecting on registration
> meant reconnecting the broker on every single job. The fix is a module-scoped instance marker,
> so only a genuinely *rebuilt* module takes over the transport.

Ask: is my entry point called once, or many times? What tells me a reload happened?

### 2. How do I run an agent turn and get the result?

This is the one thing the library cannot do for you, because it is entirely platform-specific.
You need to take a rendered prompt, run it as an agent turn in isolation, and know when it
finished.

OpenClaw exposes `api.runtime.subagent` with `run` / `waitForRun` / `getSessionMessages` /
`getSession` / `deleteSession`. Note what is **missing**: there is no abort primitive, which is
why Plexus cancellation is documented as cooperative — a cancelled job stops producing mesh
traffic, but the compute may run to completion internally.

Ask: can I run a turn without hijacking the user's session? Can I tell when it settled — as
distinct from when it last said something? Can I stop it?

> **The liveness trap.** A watchdog that re-dispatches jobs that have been *silent* for N minutes
> will duplicate exactly your most expensive work, because a big job thinks quietly. Use "the run
> has settled without publishing a result" as the signal, and require silence *as well*, not
> instead.

### 3. Does the executor have the tools it needs?

An agent asked to review a repository needs to read the repository. Plexus carries jobs, not
credentials — the executor authenticates with the host's own identity.

> **The trap that cost the most time.** In OpenClaw, the plugin's tools never reached executor
> sessions because a `registrationMode` guard returned early, before `api.registerTool`. The
> agents ran, produced plausible output, and quietly had no ability to publish anything. Verify
> end to end that an executor can actually call your tools — don't infer it from the absence of
> errors.

## The shape

With `plexus-agent` doing the protocol, a host plugin is roughly this:

```js
import { connect } from "plexus-agent";

export async function start(platform, config) {
  const agent = await connect({
    broker: config.broker,
    agentId: config.agentId,
    // Stable across restarts, or `clean:false` buys nothing and jobs published
    // while you were down are lost silently.
    clientId: config.clientId,
  });

  for (const capability of config.capabilities) {
    agent.serve(capability.service, async (job, ctx) => {
      const prompt = render(capability.prompt, job.args);

      // ── the only platform-specific part ──
      const run = await platform.runAgentTurn({ prompt, isolated: true });
      ctx.progress("running");
      const output = await run.result();
      // ─────────────────────────────────────

      return parse(output);
    }, { description: capability.description });
  }

  return { stop: () => agent.close() };
}
```

The capability list is **data** — names, argument schemas, prompt templates. A host plugin should
contain no service name anywhere. If yours mentions `code.review`, it has stopped being a bridge
and started being an agent.

## Rules that are not optional

Break any of these and you'll interoperate *almost* correctly, which is worse than failing:

**The client id must be stable across restarts.** Never derive it from a pid or timestamp.

**`${VAR}` expands before `{{args}}`.** Reversed, a caller passes `"${SOME_SECRET}"` as an
argument value and your bridge expands it — turning every invoke into an environment read.

**Deployment values resolve at dispatch, never in the catalog.** The catalog is published to a
*retained* topic; resolving there broadcasts every deployment secret to the whole registry.

**Job topics are always owner-scoped.** There is no unscoped `jobs/<jobId>/…` form.

**Every path publishes a terminal result** — including rejection, timeout, cancellation and your
own crashes. A client must never be left waiting on a job that quietly stopped existing.

**Retain results, don't retain events.** A late subscriber gets the answer, not the history.

## Proving it works

Run your plugin's agent against a reference agent and check they interoperate both ways:

```bash
mosquitto -p 1883 &
node examples/demo.mjs           # two reference agents, delegating
```

Then, with your platform's agent on the same broker and root:

- your agent appears in `agent.peers()` from a reference agent, with its capabilities;
- a reference agent can `invoke` yours and gets a terminal result;
- your agent can `ctx.askAny(...)` a reference agent and use the answer;
- killing your process marks it offline via the broker's last will, with no husk left behind;
- a job published *while your agent is stopped* is delivered when it restarts. **This is the one
  people skip, and it is the one that proves the client id is right.**

The end-to-end tests in [`test/packages.mjs`](../test/packages.mjs) are a working template for all
five.

## Two platforms, one agent

If your platform already has a plugin system, you have a choice about layering. The plugin can be
thin — just the bridge — and let capabilities come from config. Or it can host
[Plexus plugins](../packages/agent#plugins) itself, so that `plexus-notify` and anything else
written against the frame run inside your platform's process without modification.

The second costs about thirty lines more and means the ecosystem is shared rather than
per-platform. Prefer it.

---

Questions, or building one? [Open an issue](https://github.com/MoGhali/plexus/issues) — a second
independent host implementation is worth more to this project than any feature on the roadmap.
