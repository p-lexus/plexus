"""Tests for the Hermes host plugin.

The pure parts run anywhere. The end-to-end block needs a broker and is skipped
without one.

What is faked and what is not matters here. **Only Hermes itself is faked** — a
``PluginContext`` that records tool registrations and injected messages. The
mesh, the broker, the durable session, the job lifecycle and every tool handler
are real. So these tests prove the whole plugin except the one thing that
cannot be tested without Hermes installed: that Hermes really calls
``register(ctx)`` with a context shaped like this one.

    python -m pytest hosts/hermes/tests -q
    PLEXUS_TEST_BROKER=mqtt://localhost:1883 python hosts/hermes/tests/test_plugin.py
"""

from __future__ import annotations

import json
import os
import socket
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from hermes import config as config_module  # noqa: E402
from hermes import topics  # noqa: E402
from hermes.executor import parse_output  # noqa: E402
from hermes.prompts import missing_arguments, render_prompt  # noqa: E402
from hermes.protocol import (  # noqa: E402
    DEFER, REMEMBERED_JOBS, MeshAgent, derive_client_id,
)

PASS, FAIL = 0, 0
_QUEUE = []


def t(name):
    def decorate(fn):
        _QUEUE.append((name, fn))
        return fn
    return decorate


# ── topics ──────────────────────────────────────────────────────────────────

@t("owner scoping matches the JS and TypeScript implementations exactly")
def _():
    assert topics.owner_scope("Mohanad.Q!") == "mohanad-q"
    assert topics.owner_scope("--ci--") == "ci"
    assert topics.owner_scope("") == "public"
    assert topics.owner_scope(None) == "public"


@t("topics build the documented address space")
def _():
    assert topics.invoke("agents", "dba") == "agents/commands/dba/invoke"
    assert topics.result("agents", "alice", "j1") == "agents/jobs/alice/j1/result"
    assert topics.profile("agents", "dba") == "agents/registry/dba/profile"


@t("v1.4: the invoke topic carries the owner, and reads back unchanged")
def _():
    assert topics.invoke_as("agents", "dba", "ci") == "agents/commands/dba/invoke/ci"
    assert topics.invoke_filter("agents", "dba") == "agents/commands/dba/invoke/+"
    assert topics.invoke_topic_owner("agents", "dba", "agents/commands/dba/invoke/ci") == "ci"
    # Unnormalised on purpose: the string in the topic is the one a broker ACL
    # matched, and lower-casing it here would accept what the broker refused.
    assert topics.invoke_topic_owner("agents", "dba", "agents/commands/dba/invoke/Mohanad.Q!") == "Mohanad.Q!"


@t("v1.4: what is not an owner-scoped invoke topic is not read as one")
def _():
    assert topics.invoke_topic_owner("agents", "dba", "agents/commands/dba/invoke") is None
    assert topics.invoke_topic_owner("agents", "dba", "agents/commands/dba/invoke/ci/more") is None
    assert topics.invoke_topic_owner("agents", "dba", "agents/commands/other/invoke/ci") is None
    assert topics.invoke_topic_owner("agents", "dba", "agents/commands/dba/cancel") is None


@t("the three implementations agree on the protocol version")
def _():
    # A version that differs between implementations is a mesh whose peers
    # disagree about what they can speak.
    root = Path(__file__).resolve().parents[3]
    js = (root / "packages" / "agent" / "index.js").read_text()
    ts = (root / "hosts" / "openclaw" / "src" / "types.ts").read_text()
    assert f'PROTOCOL_VERSION = "{topics.PROTOCOL_VERSION}"' in js, "JS disagrees"
    assert f'PROTOCOL_VERSION = "{topics.PROTOCOL_VERSION}"' in ts, "TypeScript disagrees"


@t("v1.5: a feedback topic carries its owner, and nothing else is one")
def _():
    R = "acme/agents"
    assert topics.feedback(R, "conan", "ci") == f"{R}/commands/conan/feedback/ci"
    assert topics.feedback_filter(R, "conan") == f"{R}/commands/conan/feedback/+"
    assert topics.feedback_topic_owner(R, "conan", f"{R}/commands/conan/feedback/ci") == "ci"
    # An invoke is not a verdict, and another agent's verdict is not ours.
    assert topics.feedback_topic_owner(R, "conan", f"{R}/commands/conan/invoke/ci") is None
    assert topics.feedback_topic_owner(R, "conan", f"{R}/commands/dba/feedback/ci") is None
    # Unscoped and over-deep are refused rather than guessed at.
    assert topics.feedback_topic_owner(R, "conan", f"{R}/commands/conan/feedback") is None
    assert topics.feedback_topic_owner(R, "conan", f"{R}/commands/conan/feedback/ci/x") is None


def _judged(*, served=("j1", "ci")):
    """A mesh agent that publishes into a list instead of onto a broker."""
    agent = MeshAgent("mqtt://localhost:1883", "reviewer", root="acme/agents")
    sent = []
    agent._publish = lambda topic, payload, retain=False: sent.append((topic, payload))
    if served:
        agent._remember(*served)
    return agent, sent


@t("v1.5: a verdict on somebody else's job is refused, not reattributed")
def _():
    agent, sent = _judged()
    agent._on_feedback("mallory", json.dumps({"jobId": "j1", "verdict": "bad"}))
    assert len(sent) == 1, "the refusal must be published, not swallowed"
    topic, body = sent[0]
    assert topic == "acme/agents/jobs/mallory/j1/events", "told in the scope it published from"
    assert body["type"] == "feedback_refused"
    assert "requested by ci" in body["error"]
    assert "refused rather than reattributed" in body["error"]


@t("v1.5: an accepted verdict lands on the timeline and reaches the listener")
def _():
    agent, sent = _judged()
    heard = []
    agent.on_feedback(heard.append)
    agent._on_feedback("ci", json.dumps({"jobId": "j1", "verdict": "bad", "reason": "wrong file"}))
    assert [b["type"] for _, b in sent] == ["feedback"]
    assert sent[0][1]["verdict"] == "bad"
    assert sent[0][1]["note"] == "wrong file"
    assert heard == [{"jobId": "j1", "verdict": "bad", "by": "ci", "ts": heard[0]["ts"], "reason": "wrong file"}]


@t("v1.5: an unknown job is a different answer from an unauthorised one")
def _():
    agent, sent = _judged(served=None)
    agent._on_feedback("ci", json.dumps({"jobId": "gone", "verdict": "bad"}))
    error = sent[0][1]["error"]
    assert "fallen off the end" in error
    assert "reattributed" not in error, "a blameless miss must not read as a rejection"


@t("v1.5: only the three verdicts are verdicts")
def _():
    for good in ("good", "bad", "unusable", "GOOD", " bad "):
        agent, sent = _judged()
        agent._on_feedback("ci", json.dumps({"jobId": "j1", "verdict": good}))
        assert sent[0][1]["type"] == "feedback", good
    agent, sent = _judged()
    agent._on_feedback("ci", json.dumps({"jobId": "j1", "verdict": 4}))
    assert "unknown verdict 4" in sent[0][1]["error"]


@t("v1.5: the record of who asked is bounded, and drops the oldest first")
def _():
    agent, _ = _judged(served=None)
    for i in range(REMEMBERED_JOBS + 5):
        agent._remember(f"j{i}", "ci")
    assert len(agent._served) == REMEMBERED_JOBS
    assert "j0" not in agent._served, "the oldest goes"
    assert f"j{REMEMBERED_JOBS + 4}" in agent._served


@t("v1.5: a verdict is sent as this agent and nobody else")
def _():
    agent, sent = _judged(served=None)
    agent.feedback("dba", "ask-1", "good", "clear answer")
    topic, body = sent[0]
    assert topic == "acme/agents/feedback/reviewer/dba/ask-1", \
        "filed with the recorder, under this agent's own scope — never handed to the peer"
    assert "/commands/" not in topic, "there is no path from here to a peer's command topic"
    assert body["verdict"] == "good" and body["reason"] == "clear answer"
    try:
        agent.feedback("dba", "ask-1", "excellent")
    except ValueError:
        pass
    else:
        raise AssertionError("a verdict outside the vocabulary must be refused before it is sent")


@t("Hermes addresses the same mesh plexus-agent does")
def _():
    fx = json.loads((Path(__file__).resolve().parents[3] / "test" / "fixtures" / "topics.json").read_text())
    root, agent, owner, job, service = fx["root"], fx["agentId"], fx["owner"], fx["jobId"], fx["service"]

    mine = {
        "profile": topics.profile(root, agent),
        "status": topics.status(root, agent),
        "invoke": topics.invoke(root, agent),
        "invokeAs": topics.invoke_as(root, agent, owner),
        "invokeFilter": topics.invoke_filter(root, agent),
        "cancel": topics.cancel(root, agent),
        "query": topics.query(root, agent),
        "config": topics.config(root, agent),
        "events": topics.events(root, owner, job),
        "result": topics.result(root, owner, job),
        "postmortem": topics.postmortem(root, owner, job),
        "feedback": topics.feedback(root, agent, owner),
        "feedbackFilter": topics.feedback_filter(root, agent),
        "feedbackFile": topics.feedback_file(root, owner, agent, job),
        "feedbackFileFilter": topics.feedback_file_filter(root),
        "memory": topics.memory(root, service),
        "memoryFilter": topics.memory_filter(root),
    }
    for name, expected in fx["built"].items():
        assert mine[name] == expected, f"{name}: {mine[name]!r} != {expected!r}"
    # A topic only one implementation knows about is one the others ignore.
    assert sorted(mine) == sorted(fx["built"])


@t("Hermes reads a job topic the way the fixture says")
def _():
    fx = json.loads((Path(__file__).resolve().parents[3] / "test" / "fixtures" / "topics.json").read_text())
    pattern = topics.job_pattern(fx["root"])

    for case in fx["jobTopics"]:
        m = pattern.match(case["topic"])
        if case.get("match") is False:
            assert m is None, f"{case['topic']} must not parse as job traffic"
            continue
        assert m is not None, case["topic"]
        assert m.group(1) == case["owner"] and m.group(2) == case["jobId"] and m.group(3) == case["kind"]

    for case in fx["invokeOwners"]:
        assert topics.invoke_topic_owner(fx["root"], fx["agentId"], case["topic"]) == case["owner"], case["topic"]
    for case in fx["ownerScopes"]:
        assert topics.owner_scope(case["from"]) == case["actual"], case["from"]


@t("the job pattern refuses the unscoped form")
def _():
    pattern = topics.job_pattern("agents")
    assert pattern.match("agents/jobs/alice/j1/result")
    assert not pattern.match("agents/jobs/j1/result"), "unscoped job topics must never match"


@t("client id is stable, and never contains the pid")
def _():
    assert derive_client_id("dba", "agents") == derive_client_id("dba", "agents")
    assert derive_client_id("dba", "agents") != derive_client_id("dba", "staging")
    assert str(os.getpid()) not in derive_client_id("dba", "agents")


# ── prompts ─────────────────────────────────────────────────────────────────

@t("environment values expand BEFORE caller arguments")
def _():
    # The security property: reversed, a caller passes "${SECRET}" as an
    # argument and the bridge expands it on the next pass, turning every invoke
    # into an arbitrary environment read.
    out = render_prompt("say {{phrase}}", {"phrase": "${LEAKED}"}, "j1", "alice", {"LEAKED": "hunter2"})
    assert out == "say ${LEAKED}", f"argument values must never be expanded: {out!r}"


@t("deployment values, job identity and arguments all render")
def _():
    out = render_prompt(
        "Review {{repo}} for {{requestedBy}} (job {{jobId}}), tell ${CHANNEL}",
        {"repo": "acme/web"}, "j1", "alice", {"CHANNEL": "#eng"},
    )
    assert out == "Review acme/web for alice (job j1), tell #eng"


@t("config beats the environment for prompt variables")
def _():
    os.environ["PLEXUS_TEST_VAR"] = "from-env"
    assert render_prompt("${PLEXUS_TEST_VAR}", {}, "j", "a", {"PLEXUS_TEST_VAR": "from-config"}) == "from-config"
    assert render_prompt("${PLEXUS_TEST_VAR}", {}, "j", "a", {}) == "from-env"


@t("an unknown argument renders empty rather than leaving a placeholder")
def _():
    assert render_prompt("[{{nope}}]", {}, "j", "a", {}) == "[]"


@t("missing_arguments catches a prompt the schema cannot fill")
def _():
    assert missing_arguments("Review {{repo}} #{{pr}}", {"repo": "string"}) == ["pr"]
    assert missing_arguments("Job {{jobId}} for {{requestedBy}}", {}) == []


# ── executor output parsing ─────────────────────────────────────────────────

@t("agent output is parsed from bare JSON, fenced JSON, or prose")
def _():
    assert parse_output('{"verdict":"APPROVE"}')["verdict"] == "APPROVE"
    assert parse_output('Here you go:\n```json\n{"verdict":"DENY"}\n```\nhope that helps')["verdict"] == "DENY"
    fallback = parse_output("I could not do it.")
    assert fallback["type"] == "result" and "could not" in fallback["output"]


@t("parsed output always carries a type")
def _():
    assert parse_output('{"summary":"x"}')["type"] == "result"
    assert parse_output('{"type":"review","summary":"x"}')["type"] == "review"


# ── config ──────────────────────────────────────────────────────────────────

def _write_config(payload) -> Path:
    path = Path(tempfile.mkdtemp()) / "plexus.json"
    path.write_text(json.dumps(payload))
    return path


@t("a prompt referencing an undeclared argument is refused at load")
def _():
    path = _write_config({"broker": "mqtt://x", "capabilities": [
        {"service": "a.b", "prompt": "Review {{repo}}", "requestSchema": {}},
    ]})
    try:
        config_module.load(path)
    except ValueError as err:
        assert "{{repo}}" in str(err)
    else:
        raise AssertionError("should have refused a prompt the schema cannot fill")


@t("duplicate services are refused at load")
def _():
    path = _write_config({"broker": "mqtt://x", "capabilities": [
        {"service": "a.b", "prompt": "x", "requestSchema": {}},
        {"service": "a.b", "prompt": "y", "requestSchema": {}},
    ]})
    try:
        config_module.load(path)
    except ValueError as err:
        assert "duplicate" in str(err)
    else:
        raise AssertionError("should have refused a duplicate service")


@t("capability prompts keep their ${VAR}s unresolved in the catalog")
def _():
    # The catalog is published to a *retained* topic. Resolving deployment
    # values before publishing would broadcast every secret to the registry.
    os.environ["PLEXUS_SECRET_TEST"] = "s3cret"
    path = _write_config({
        "broker": "mqtt://x",
        "capabilities": [{"service": "a.b", "prompt": "tell ${PLEXUS_SECRET_TEST}", "requestSchema": {}}],
    })
    loaded = config_module.load(path)
    assert "s3cret" not in loaded["capabilities"][0]["prompt"]
    assert "${PLEXUS_SECRET_TEST}" in loaded["capabilities"][0]["prompt"]


@t("an invalid executor mode is refused")
def _():
    path = _write_config({"broker": "mqtt://x", "executor": "telepathy"})
    try:
        config_module.load(path)
    except ValueError as err:
        assert "executor" in str(err)
    else:
        raise AssertionError("should have refused an unknown executor mode")


# ── end to end, if a broker is reachable ────────────────────────────────────

BROKER = os.environ.get("PLEXUS_TEST_BROKER", "mqtt://localhost:1883")


def _reachable() -> bool:
    host, _, port = BROKER.replace("mqtt://", "").partition(":")
    try:
        with socket.create_connection((host or "localhost", int(port or 1883)), timeout=1.5):
            return True
    except OSError:
        return False


class FakeHermes:
    """A stand-in PluginContext.

    Records what the plugin registers and what it injects — which is exactly
    the surface Hermes provides, and nothing more.
    """

    def __init__(self):
        self.tools = {}
        self.hooks = []
        self.injected = []

    def register_tool(self, name, toolset, schema, handler):
        assert schema["name"] == name, "schema name must match the registered tool name"
        assert "parameters" in schema, "Hermes requires a JSON Schema parameters block"
        self.tools[name] = handler

    def register_hook(self, event, callback):
        self.hooks.append((event, callback))

    def inject_message(self, content, role="user", session_key=None):
        self.injected.append({"content": content, "role": role, "session_key": session_key})


if _reachable():
    ROOT = f"plexus-hermes-test-{os.getpid()}"

    @t("end to end: a job injected into Hermes is settled by the agent's own tool call")
    def _():
        import hermes

        path = _write_config({
            "broker": BROKER, "root": ROOT, "agentId": "hermes-under-test",
            "executor": "inject", "maxJobSeconds": 20,
            "capabilities": [{
                "service": "research.summarise",
                "description": "Researches a topic.",
                "requestSchema": {"topic": "string"},
                "prompt": "Research {{topic}} and report findings.",
            }],
        })
        os.environ["PLEXUS_CONFIG"] = str(path)
        hermes._agent = None                      # a fresh module state per test

        ctx = FakeHermes()
        hermes.register(ctx)
        assert hermes._agent is not None, "the plugin should have connected"
        for expected in ("mesh_publish", "mesh_peers", "mesh_ask", "mesh_status"):
            assert expected in ctx.tools, f"{expected} was not registered"

        requester = MeshAgent(BROKER, "alice", root=ROOT, durable=False).start()
        try:
            requester.wait_for_peer("research.summarise", timeout=10)

            answer: dict = {}
            done = threading.Event()

            def ask():
                answer.update(requester.ask("research.summarise", {"topic": "lock contention"}, timeout=20))
                done.set()

            threading.Thread(target=ask, daemon=True).start()

            # The agent is asked, in its own words, via inject_message.
            deadline = time.monotonic() + 10
            while not ctx.injected and time.monotonic() < deadline:
                time.sleep(0.05)
            assert ctx.injected, "the job should have been injected into a Hermes session"

            briefing = ctx.injected[0]["content"]
            assert "lock contention" in briefing, "the rendered prompt should reach the agent"
            assert "mesh_publish" in briefing, "the agent must be told how to report"

            job_id = briefing.split("job `")[1].split("`")[0]
            assert ctx.injected[0]["session_key"] == f"plexus:{job_id}"

            # Now the agent does what the briefing told it to.
            out = json.loads(ctx.tools["mesh_publish"]({
                "jobId": job_id, "kind": "events", "payload": {"message": "reading"},
            }))
            assert out["success"], out

            out = json.loads(ctx.tools["mesh_publish"]({
                "jobId": job_id, "kind": "result",
                "payload": {"type": "research", "summary": "row locks, mostly"},
            }))
            assert out["success"], out

            assert done.wait(10), "the requester never received a result"
            assert answer["summary"] == "row locks, mostly"
            assert answer["type"] == "research"
            assert answer["owner"] == "alice", "results must be owner-scoped"
        finally:
            requester.stop()
            hermes._shutdown()

    @t("end to end: a job nobody reports on still reaches a terminal state")
    def _():
        # The weakness of inject mode is that it depends on the agent choosing
        # to call the tool. The watchdog is what stops that becoming a caller
        # waiting forever on a job that quietly stopped existing.
        import hermes

        path = _write_config({
            "broker": BROKER, "root": f"{ROOT}-wd", "agentId": "forgetful",
            "executor": "inject", "maxJobSeconds": 2,
            "capabilities": [{"service": "forget.it", "prompt": "Do a thing.", "requestSchema": {}}],
        })
        os.environ["PLEXUS_CONFIG"] = str(path)
        hermes._agent = None

        ctx = FakeHermes()
        hermes.register(ctx)
        requester = MeshAgent(BROKER, "bob", root=f"{ROOT}-wd", durable=False).start()
        try:
            requester.wait_for_peer("forget.it", timeout=10)
            result = requester.ask("forget.it", {}, timeout=15)   # agent never publishes
            assert result["type"] == "error"
            assert "did not publish a result" in result["error"]
        finally:
            requester.stop()
            hermes._shutdown()

    @t("end to end: mesh_publish refuses a job that is not this agent's")
    def _():
        import hermes

        path = _write_config({
            "broker": BROKER, "root": f"{ROOT}-guard", "agentId": "guard",
            "executor": "inject",
            "capabilities": [{"service": "guard.it", "prompt": "x", "requestSchema": {}}],
        })
        os.environ["PLEXUS_CONFIG"] = str(path)
        hermes._agent = None

        ctx = FakeHermes()
        hermes.register(ctx)
        try:
            out = json.loads(ctx.tools["mesh_publish"]({
                "jobId": "not-a-real-job", "kind": "result", "payload": {"x": 1},
            }))
            assert not out["success"]
            assert "not active" in out["error"]
        finally:
            hermes._shutdown()

    @t("end to end: a reload reuses the session rather than opening a second one")
    def _():
        # Two clients sharing a client id fight over the session and the broker
        # kicks each in turn — which looks exactly like flaky networking.
        import hermes

        path = _write_config({
            "broker": BROKER, "root": f"{ROOT}-reload", "agentId": "reloaded",
            "executor": "inject",
            "capabilities": [{"service": "r.r", "prompt": "x", "requestSchema": {}}],
        })
        os.environ["PLEXUS_CONFIG"] = str(path)
        hermes._agent = None

        first_ctx = FakeHermes()
        hermes.register(first_ctx)
        first = hermes._agent
        try:
            second_ctx = FakeHermes()
            hermes.register(second_ctx)                 # Hermes reloads plugins
            assert hermes._agent is first, "a reload must not open a second connection"
            assert "mesh_publish" in second_ctx.tools, "tools should still be registered on reload"
        finally:
            hermes._shutdown()
else:
    print(f"\n⚠️  no broker at {BROKER} — skipping end-to-end tests")
    print("   start one:  mosquitto -p 1883\n")


if __name__ == "__main__":
    for name, fn in _QUEUE:
        try:
            fn()
            print(f"✅ {name}")
            PASS += 1
        except Exception as err:  # noqa: BLE001
            print(f"❌ {name}\n     {type(err).__name__}: {err}")
            FAIL += 1
    print(f"\n{PASS} passed, {FAIL} failed")
    sys.exit(1 if FAIL else 0)
