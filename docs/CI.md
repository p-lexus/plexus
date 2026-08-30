# Asking the mesh from CI

A pipeline publishes a job, waits for the answer, and exits on it. No endpoint to host, no callback
URL that has to be reachable from wherever the agent happens to be, and nothing to keep alive
between the ask and the answer.

This is the **any-broker** guide: an MQTT client, a credential, and the topic map. It assumes
protocol **v1.4**, where the owner is a topic segment rather than a payload field — which is what
lets a broker rule decide who may ask as whom.

If you run the Plexus box, none of this is necessary: `plexus ask code.review --arg pr:=42 --wait`
does the whole of it and sets the exit code. This document is for a mesh on a broker you brought
yourself.

---

## What a pipeline needs

| | |
|---|---|
| Broker URL | `mqtts://broker.internal:8883` — TLS, if it leaves the machine |
| A credential | Username and password for the pipeline's **own** identity, e.g. `ci` |
| The mesh root | `agents`, `acme/agents`, whatever your deployment uses |
| An agent and a capability | `reviewer` and `code.review` — read them from the registry |

The credential's username **is** the owner scope. A pipeline called `ci` asks as `ci`, its answers
land under `jobs/ci/`, and it can read nothing else.

## The two topics

```bash
# ask   — the owner is in the topic, where a broker can check it
<root>/commands/<agent>/invoke/<owner>

# collect — retained, so it can be read long after the job finished
<root>/jobs/<owner>/<jobId>/result
```

`<owner>` must already be owner-scoped: lowercase, `[a-z0-9_-]`, no spaces. The agent does **not**
normalise it — `Team.CI!` is refused with an error telling you to use `team-ci`. Normalising would
make one identity two spellings, and a broker ACL matches only one of them, so the agent would be
accepting what the broker did not.

## The whole thing, in nine lines

```bash
#!/usr/bin/env bash
set -euo pipefail

ROOT=agents  AGENT=reviewer  OWNER=ci
JOB="ci-${GITHUB_RUN_ID:-$(date +%s)}-${GITHUB_RUN_ATTEMPT:-1}"

# Subscribe FIRST, in the background. A result published before you are
# listening is still retained — but a job that finishes in 200ms and a
# subscription that starts in 400ms is a race you do not want to think about.
mosquitto_sub -h "$BROKER" -u "$OWNER" -P "$MQTT_PASSWORD" \
  -t "$ROOT/jobs/$OWNER/$JOB/result" -C 1 -W 900 > result.json &
SUB=$!

mosquitto_pub -h "$BROKER" -u "$OWNER" -P "$MQTT_PASSWORD" \
  -t "$ROOT/commands/$AGENT/invoke/$OWNER" \
  -m "$(jq -nc --arg job "$JOB" --arg repo "$GITHUB_REPOSITORY" --argjson pr "$PR_NUMBER" \
        '{jobId:$job, service:"code.review", args:{repo:$repo, pr:$pr}}')"

wait "$SUB"      # -W 900 gives up after fifteen minutes
```

Then decide what the build does about the answer:

```bash
TYPE=$(jq -r '.type // "unknown"' result.json)
case "$TYPE" in
  review|done) : ;;                                   # the work happened
  error|timeout|rejected|duplicate|cancelled)
    echo "::error::job $JOB ended as $TYPE — $(jq -r '.error // .note // ""' result.json)"
    exit 1 ;;
  *) echo "::error::unrecognised result type $TYPE"; exit 1 ;;
esac

# and only then, the verdict the capability returned
[ "$(jq -r '.verdict // "APPROVE"' result.json)" = "REQUEST_CHANGES" ] && exit 1
exit 0
```

**Two failures, kept apart.** *The job did not complete* (`error`, `timeout`) is a pipeline
problem — retry it, alert on it. *The job completed and the answer was no* (`verdict:
REQUEST_CHANGES`) is a review outcome — that is the build doing its job. Collapsing them into one
non-zero exit makes a broken mesh look like a failing test, and gets both ignored.

## Use a fresh jobId every run

Results are **retained**: the last publish for a topic stays there until it is overwritten or
cleared. So a `jobId` that repeats does two bad things at once — your subscription gets the
*previous* run's answer instantly and the pipeline believes it, and if the job does run, its answer
overwrites the old one.

Derive it from something the CI system guarantees is unique:

| System | |
|---|---|
| GitHub Actions | `ci-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT` |
| GitLab CI | `ci-$CI_PIPELINE_ID-$CI_JOB_ID` |
| Jenkins | `ci-$BUILD_TAG` |
| Anything else | a UUID |

An agent refuses a `jobId` that is currently **active** with a terminal `duplicate`. It does not
refuse one that finished an hour ago — the id is yours to keep unique.

## The rules the pipeline's credential needs

Generated from the topic map rather than written by hand:

```js
import { aclFor } from "plexus-agent/acl";
aclFor({ root: "agents", role: "requester", id: "ci", ownerInTopic: true });
```

```
publish     agents/commands/+/invoke/ci      ← as itself, and as nobody else
publish     agents/commands/+/cancel
subscribe   agents/jobs/ci/#                 ← its own answers, and no others
subscribe   agents/registry/+/profile
subscribe   agents/registry/+/status
```

The first line is the one that matters. `commands/+/invoke/ci` lets this credential ask any agent
on the mesh, as `ci`, and refuses `invoke/someone-else` — which is what "the owner is in the topic"
buys. Grant `commands/+/invoke` instead and you are back to v1.3, where anyone can claim to be
anyone, because no broker can police a field inside a payload.

**Without those rules nothing above fails loudly.** A refused publish is acknowledged at QoS 1 and a
refused subscription returns a code most clients swallow, so the pipeline hangs until `-W` fires and
reports a timeout that looks like a slow agent. If a job never arrives, read the broker's log before
you read anything else.

## GitHub Actions

```yaml
- name: Ask the mesh for a review
  env:
    BROKER: ${{ vars.PLEXUS_BROKER }}
    MQTT_PASSWORD: ${{ secrets.PLEXUS_CI_PASSWORD }}
    PR_NUMBER: ${{ github.event.pull_request.number }}
  run: |
    sudo apt-get install -y mosquitto-clients jq
    ./scripts/ask-the-mesh.sh
```

## GitLab CI

```yaml
review:
  image: alpine:3
  variables:
    OWNER: ci
  before_script: [ "apk add --no-cache mosquitto-clients jq bash" ]
  script: [ "./scripts/ask-the-mesh.sh" ]
```

`PLEXUS_CI_PASSWORD` is a masked variable in both. It is a broker credential scoped to one owner —
not a token that reads the mesh — but it is still a credential, and rotating it is one command
against the broker.

## Cancelling

```bash
mosquitto_pub -h "$BROKER" -u "$OWNER" -P "$MQTT_PASSWORD" \
  -t "$ROOT/commands/$AGENT/cancel" \
  -m "{\"jobId\":\"$JOB\",\"requestedBy\":\"$OWNER\"}"
```

Cancel has no owner-in-topic form: it carries `requestedBy` in the payload. Cancellation is also
**cooperative** — the mesh publishes a terminal result immediately and honours nothing further for
that job, but work already running inside an executor may finish anyway. A cancelled pipeline is
not a stopped agent.

## Things that will catch you

**Subscribe before you publish.** Retained results make this survivable rather than mandatory, but
only if the job actually reaches a terminal result. Ordering the two correctly costs nothing.

**Events are not retained; results are.** Progress milestones are gone the moment they are
delivered, so a pipeline that starts listening late sees the answer and none of the story. If you
want the timeline in your build log, subscribe to `jobs/$OWNER/$JOB/events` too — before publishing.

**`-W` is your only timeout.** Nothing else gives up. Set it to something longer than the
capability's own worst case — `avgLatency` in the registry profile is the author's estimate — and
treat firing it as an incident rather than a retry.

**Never put credentials in `args`.** Payloads are readable by every subscriber to that topic, and
results are retained, so a secret published once persists until something overwrites it.

**A pipeline that only publishes and exits is fine.** Fire-and-forget is a legitimate shape: publish
the invoke, exit 0, and let the answer land on the retained topic for something else to collect. You
only need the subscribe-and-wait dance if the build's outcome depends on the answer.

## Reading the mesh instead of hardcoding it

Every agent's capabilities are on a retained topic, so a pipeline can discover rather than assume:

```bash
mosquitto_sub -h "$BROKER" -u "$OWNER" -P "$MQTT_PASSWORD" \
  -t "$ROOT/registry/+/profile" -W 2 \
  | jq -r '.agentId as $a | .capabilities[] | "\($a)\t\(.service)"'
```

That is also how you find out an agent has stopped offering something before the build discovers it
the expensive way.
