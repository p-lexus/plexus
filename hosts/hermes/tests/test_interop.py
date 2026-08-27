"""Cross-host interoperability: a Hermes agent and a JavaScript agent.

This is the project's central claim under test. Two agents written in different
languages, by different code paths, with nothing in common but PROTOCOL.md:

  - the Hermes plugin (Python, paho-mqtt, ``register(ctx)``)
  - a plexus-agent client (Node, mqtt.js)

Each discovers the other through the retained registry, sends it work, and uses
the answer. If this passes, the specification is a specification. If it fails,
Plexus is a description of one program and the second implementation found the
gap — which is exactly what a second implementation is for.

    PLEXUS_TEST_BROKER=mqtt://localhost:1883 python hosts/hermes/tests/test_interop.py
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "hosts"))

BROKER = os.environ.get("PLEXUS_TEST_BROKER", "mqtt://localhost:1883")
ROOT = f"plexus-interop-{os.getpid()}"

# The import is absolute because this file is written to a temp directory, so a
# relative specifier would resolve from there rather than from the repo.
JS_AGENT = r"""
import { connect } from "__AGENT_PATH__";

const [, , broker, root] = process.argv;
const js = await connect({ broker, root, agentId: "js-reviewer", displayName: "JS reviewer" });

// A capability the Hermes agent will delegate to.
js.serve("schema.review", (job) => {
  console.log(JSON.stringify({ event: "js-served", depth: job.depth, parent: job.parentJobId }));
  return { risk: "high", finding: "ALTER without CONCURRENTLY locks writes" };
}, { description: "Reviews a migration for lock risk." });

console.log(JSON.stringify({ event: "js-ready" }));

// And a request going the other way, into Hermes.
const peer = await js.waitForPeer("research.summarise", 20000);
const answer = await js.invoke(peer, "research.summarise", { topic: "row locks" }, { timeoutMs: 25000 });
console.log(JSON.stringify({ event: "js-got-answer", peer, answer }));

await new Promise((r) => setTimeout(r, 1500));
await js.close();
"""


def reachable() -> bool:
    host, _, port = BROKER.replace("mqtt://", "").partition(":")
    try:
        with socket.create_connection((host or "localhost", int(port or 1883)), timeout=1.5):
            return True
    except OSError:
        return False


def main() -> int:
    if not reachable():
        print(f"⚠️  no broker at {BROKER} — skipping interop test")
        return 0

    import hermes
    from hermes.protocol import DEFER

    workdir = Path(tempfile.mkdtemp())
    agent_path = (REPO / "packages" / "agent" / "index.js").as_uri()
    (workdir / "js_agent.mjs").write_text(JS_AGENT.replace("__AGENT_PATH__", agent_path))

    config = workdir / "plexus.json"
    config.write_text(json.dumps({
        "broker": BROKER, "root": ROOT, "agentId": "hermes-agent",
        "displayName": "Hermes agent", "executor": "inject", "maxJobSeconds": 30,
        "capabilities": [{
            "service": "research.summarise",
            "description": "Researches a topic.",
            "requestSchema": {"topic": "string"},
            "prompt": "Research {{topic}}.",
        }],
    }))
    # A spy on the wire. The point of v1.4 is which TOPIC an invoke is published
    # to, and no amount of correct answers proves that — an implementation that
    # silently kept using the v1.3 form would pass every other assertion here.
    import paho.mqtt.client as _mqtt

    invoke_topics: list[str] = []
    spy = _mqtt.Client(_mqtt.CallbackAPIVersion.VERSION2, client_id="interop-spy")
    spy.on_message = lambda _c, _u, m: invoke_topics.append(m.topic)
    spy_host, _, spy_port = BROKER.replace("mqtt://", "").partition(":")
    spy.connect(spy_host, int(spy_port or 1883), 30)
    spy.subscribe(f"{ROOT}/commands/+/invoke")
    spy.subscribe(f"{ROOT}/commands/+/invoke/+")
    spy.loop_start()

    os.environ["PLEXUS_CONFIG"] = str(config)
    hermes._agent = None

    class FakeHermes:
        def __init__(self):
            self.tools, self.injected = {}, []

        def register_tool(self, name, toolset, schema, handler):
            self.tools[name] = handler

        def register_hook(self, *_a, **_k):
            pass

        def inject_message(self, content, role="user", session_key=None):
            self.injected.append(content)

    ctx = FakeHermes()
    hermes.register(ctx)
    if hermes._agent is None:
        print("❌ the Hermes plugin failed to connect")
        return 1
    print("  hermes plugin online, offering research.summarise")

    js = subprocess.Popen(
        [os.environ.get("NODE", "node"), str(workdir / "js_agent.mjs"), BROKER, ROOT],
        cwd=REPO, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1,
    )
    seen: dict[str, dict] = {}

    def pump():
        for line in js.stdout:
            line = line.strip()
            if not line.startswith("{"):
                if line:
                    print(f"  [js] {line}")
                continue
            try:
                message = json.loads(line)
            except ValueError:
                continue
            seen[message.get("event", "?")] = message
            print(f"  [js] {message.get('event')}")

    threading.Thread(target=pump, daemon=True).start()

    failures = []
    try:
        deadline = time.monotonic() + 20
        while "js-ready" not in seen and time.monotonic() < deadline:
            time.sleep(0.05)
        if "js-ready" not in seen:
            print("❌ the JavaScript agent never came online")
            return 1

        # ── 1. JS -> Hermes ────────────────────────────────────────────────
        deadline = time.monotonic() + 20
        while not ctx.injected and time.monotonic() < deadline:
            time.sleep(0.05)
        if not ctx.injected:
            failures.append("the JS agent's request never reached the Hermes agent")
        else:
            briefing = ctx.injected[0]
            job_id = briefing.split("job `")[1].split("`")[0]
            print(f"  hermes received job {job_id} from the JS agent")

            # ── 2. Hermes -> JS, mid-job, via the mesh_ask tool ────────────
            asked = json.loads(ctx.tools["mesh_ask"]({
                "service": "schema.review", "args": {"migration": "0042.sql"}, "jobId": job_id,
            }))
            if not asked.get("success"):
                failures.append(f"the Hermes agent could not delegate to the JS agent: {asked}")
            else:
                answer = asked["answer"]
                if answer.get("risk") != "high":
                    failures.append(f"unexpected answer from the JS agent: {answer}")
                else:
                    print(f"  hermes delegated to {asked['agentId']} and got risk={answer['risk']}")

                served = seen.get("js-served", {})
                if served.get("depth") != 1:
                    failures.append(f"delegated job should be depth 1, was {served.get('depth')}")
                if served.get("parent") != job_id:
                    failures.append(f"parentJobId should be {job_id}, was {served.get('parent')}")
                if not failures:
                    print("  lineage intact across the language boundary: depth 1, parent linked")

            # ── 3. Hermes answers, folding in what it learned ──────────────
            json.loads(ctx.tools["mesh_publish"]({
                "jobId": job_id, "kind": "result",
                "payload": {"type": "research", "summary": "row locks, mostly", "contributedBy": ["js-reviewer"]},
            }))

        deadline = time.monotonic() + 20
        while "js-got-answer" not in seen and time.monotonic() < deadline:
            time.sleep(0.05)
        got = seen.get("js-got-answer", {}).get("answer", {})
        if got.get("summary") != "row locks, mostly":
            failures.append(f"the JS agent did not receive the Hermes result: {got}")
        else:
            print("  the JS agent received one combined answer from the Hermes agent")
    finally:
        js.terminate()
        hermes._shutdown()

    print()
    for failure in failures:
        print(f"❌ {failure}")
    if failures:
        return 1
    spy.loop_stop()
    spy.disconnect()

    # Both implementations advertise `topic: "accept"`, so both should have
    # published the owner-scoped form to each other.
    v14 = [t for t in invoke_topics if t.count("/") == 4]
    v13 = [t for t in invoke_topics if t.count("/") == 3]
    if not v14:
        failures.append(
            f"no invoke used the v1.4 owner-scoped topic — saw {invoke_topics}")
    if v13:
        failures.append(
            f"an invoke fell back to the v1.3 form despite the peer advertising support: {v13}")
    if failures:
        print()
        for f in failures:
            print(f"❌ {f}")
        print(f"\n❌ interop FAILED ({len(failures)} problem(s))")
        return 1
    print(f"  both directions used the v1.4 topic form: {sorted(set(v14))}")
    print("✅ a Hermes agent and a JavaScript agent interoperate, both directions, lineage intact")
    return 0


if __name__ == "__main__":
    sys.exit(main())
