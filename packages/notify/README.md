<div align="center">
  <img src="../../docs/logo/plexus-mark-light.svg#gh-light-mode-only" width="56" alt="">
  <img src="../../docs/logo/plexus-mark-dark.svg#gh-dark-mode-only" width="56" alt="">
  <h1>plexus-notify</h1>
  <p>A Plexus plugin. Your agents finish something — this tells people about it.</p>
</div>

---

The obvious way to notify is to give every agent a Slack token and a `postMessage` call. Then you
do it again for the next agent, and again when you add a pull-request comment, and by the fourth
one you're maintaining four slightly different formatters and four copies of a credential.
Delivery is not what those agents are for.

This is one plugin that does it for all of them. It watches the mesh, matches results against
routes, renders a message, and sends it. Agents don't know it's there, and adding a channel
doesn't touch their code.

## Load it

It's a plugin, not a process — it joins an agent you already run, over that agent's existing
connection:

```json
{
  "broker": "mqtt://localhost:1883",
  "agentId": "conan",
  "plugins": {
    "plexus-notify": {
      "channels": {
        "eng": { "type": "slack", "webhookUrl": "${SLACK_WEBHOOK_URL}" },
        "pr":  { "type": "github", "token": "${GITHUB_TOKEN}",
                 "repo": "{{args.repo}}", "issue": "{{args.pr}}" }
      },
      "routes": [
        { "id": "needs-changes",
          "when": { "service": "code.review", "verdict": "REQUEST_CHANGES" },
          "to": ["eng", "pr"],
          "title": "Changes requested on {{args.repo}}#{{args.pr}}",
          "body": "{{summary}}" }
      ]
    }
  }
}
```

```bash
npm install plexus-agent plexus-notify
npx plexus run --config plexus.json
```

`${VAR}` reads the environment, so no token ever lives in the file.

See it work:

```bash
node examples/with-plugins.mjs
```

```
 1.2s  notify    Changes requested on acme/web-app#42
                 Application logic is fine. Blocking on the migration: ALTER
                 on users without CONCURRENTLY — locks writes for the rebuild
                 reviewed for alice · job job-050260ddb366
```

That message names the repository and PR, which came from the **request**, and the finding, which
came from the **result** — produced by a different agent than the one that was asked.

## Channels

| Type | Sends to | Needs |
|---|---|---|
| `console` | stdout | nothing |
| `file` | a JSONL file | `path` |
| `slack` | a Slack incoming webhook | `webhookUrl` |
| `github` | a PR or issue comment | `token`, `repo`, `issue` |
| `webhook` | any HTTP endpoint | `url`, optional `headers` |

Adding one is adding a key to [`channels.js`](channels.js) with a `send`. There is no loader to
satisfy.

## Routes

`when` is an AND across its keys. A bare value means equality; the rest are explicit:

```jsonc
{ "verdict": "REQUEST_CHANGES" }            // equals
{ "type": ["error", "timeout"] }            // any of
{ "risk": { "$in": ["high", "critical"] } } // any of, explicitly
{ "verdict": { "$ne": "APPROVE" } }         // not equal
{ "error": { "$exists": true } }            // present
{ "summary": { "$re": "^BLOCK" } }          // regex, case-insensitive
{ "summary": { "$contains": "migration" } } // substring, case-insensitive
{ "duration": { "$gt": 300 } }              // numeric
```

An empty `when` matches everything — that's your audit route. An **unknown operator matches
nothing**, so a typo goes quiet rather than paging everyone.

Every matching route fires. `"stop": true` ends evaluation there, for when the urgent version
should suppress the cheerful one.

### What templates can see

| | From | Example |
|---|---|---|
| `{{jobId}}` `{{owner}}` `{{type}}` `{{ts}}` | the envelope | `job-050…`, `alice`, `result` |
| `{{service}}` `{{args.*}}` | the **request** | `code.review`, `{{args.repo}}` |
| `{{verdict}}` `{{summary}}` … | the **result** | whatever the agent returned |
| `{{agent}}` | who was asked | `dba` |
| `{{result}}` | the whole result, as JSON | |

A missing path renders empty. A notification with a gap reads fine; one with `{{result.verdict}}`
in the middle looks broken.

Why `args` needs explaining: a result carries the answer, not the question, so `{{args.repo}}`
cannot come from it. The plugin observes invoke traffic as well and keeps the two together.
That's what makes "changes requested on acme/web-app#42" possible at all.

## Agents can ask it directly

Loading the plugin adds a `notify.send` capability to its host agent, so any agent on the mesh can
ask for delivery mid-job rather than waiting to finish:

```js
await ctx.askAny("notify.send", {
  title: "Deploy blocked",
  body:  "Migration 0042 locks writes. Holding the release.",
  level: "error",
  to:    ["eng"],
});
```

Nothing about this is special — it's an ordinary directed invoke, the same as asking any other
specialist.

## Why it won't spam you

**Results are retained.** Every subscriber receives the full backlog the moment it connects. A
naive notifier re-pages the whole team about last Tuesday's work on every restart — the failure
mode that makes people turn notifications off.

This handles it twice over: it notes the retained flush on startup without delivering it, and it
remembers what it has already sent in `notify.state.json`, so the suppression survives restarts
too. Deliveries are keyed per route *and* per channel — if Slack succeeds and GitHub is
rate-limited, only the GitHub comment is retried.

Delivery is at-least-once, like everything else in Plexus. A failed channel retries with backoff;
a success whose acknowledgement is lost may send twice. That trade is deliberate — a duplicate
Slack message beats a missed page. **If you route to something that acts rather than informs,
make it idempotent.**

Set `"replay": "all"` to deliver the backlog on purpose.

## Options

| | Default | |
|---|---|---|
| `state` | `./notify.state.json` | Where the delivered-set is kept |
| `replay` | `skip` | `all` also delivers what's already retained |
| `replayGraceMs` | `3000` | How long the retained flush is assumed to last |
| `retries` | `[1000, 5000, 20000]` | Backoff between delivery attempts |
| `dryRun` | `false` | Match and render, deliver nothing |

`npx plexus run --dry-run` sets `dryRun` on every plugin.

## Using the matching on its own

It's pure, and separately importable:

```js
import { plan, matches, render } from "plexus-notify/routes";
```

---

Apache 2.0 · part of [Plexus](../../README.md)
