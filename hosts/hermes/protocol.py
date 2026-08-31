"""The mesh client: one durable MQTT session, and the protocol on top of it.

This is the Python counterpart to ``packages/agent/index.js``. It exists
because Hermes plugins are Python, so the Node client cannot be reused — which
makes this the second independent implementation of the protocol, and the best
evidence that the specification is a specification rather than a description of
one program.

Threading model: paho's ``loop_start()`` runs the network loop on its own
daemon thread and delivers callbacks there. Hermes gives a plugin no startup or
shutdown lifecycle event, so that thread's lifetime is the process's. Job
handlers therefore run on a small worker pool rather than on the network
thread — a handler that blocks the network thread stops keepalives and the
broker eventually drops the session, which looks exactly like a network fault
and is not one.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import secrets
import socket
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Callable

import paho.mqtt.client as mqtt

from . import topics
from .topics import PROTOCOL_VERSION, owner_scope

log = logging.getLogger("plexus")

#: Terminal result types. An unrecognised type is also terminal — treat it so.
TERMINAL = {"result", "error", "cancelled", "duplicate", "rejected", "timeout"}

#: The verdicts a requester may return (v1.5).
VERDICTS = ("good", "bad", "unusable")

#: Long enough for a paragraph of why, short enough not to be a payload.
MAX_FEEDBACK_REASON = 500

#: How many finished jobs an agent remembers the owner of, so a verdict arriving
#: long after the work can be attributed — or refused — rather than guessed at.
REMEMBERED_JOBS = 200


class _Defer:
    """Returned by a handler that will publish its own result later.

    Hermes has no way to run an agent turn and hand back its output — you push
    a message in and the agent replies in its own time, through whatever tools
    it has. So a handler dispatches the work and returns DEFER, leaving the job
    open until the agent calls ``mesh_publish``, or the watchdog gives up.

    Every deferred job is still guaranteed to reach a terminal state: the
    watchdog publishes an ``error`` if nothing arrives in time. A caller is
    never left waiting on a job that quietly stopped existing.
    """

    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover — debugging aid
        return "DEFER"


#: Sentinel: "this job is still running; something else will settle it."
DEFER = _Defer()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _job_id(prefix: str = "job") -> str:
    return f"{prefix}-{secrets.token_hex(6)}"


def derive_client_id(agent_id: str, root: str) -> str:
    """Stable per-deployment identity — the single thing durability rests on.

    Never derive this from a pid or a timestamp. With a changing id every
    restart is a *new* MQTT session, so ``clean_session=False`` buys nothing:
    the broker's queued QoS-1 messages stay orphaned with the dead session, and
    any invoke published while the agent was down is lost silently rather than
    delivered on reconnect.
    """
    digest = hashlib.sha1(f"{socket.gethostname()}::{root}::{agent_id}".encode()).hexdigest()
    return f"plexus-{agent_id}-{digest[:10]}"


class Ask:
    """A delegated request awaiting its terminal result."""

    __slots__ = ("event", "value")

    def __init__(self) -> None:
        self.event = threading.Event()
        self.value: dict[str, Any] | None = None


class MeshAgent:
    """An agent on the mesh.

    Handlers are ``(job: dict, ctx: JobContext) -> Any``. Whatever they return
    is published as the job's terminal result; whatever they raise becomes a
    terminal ``error``. There is no path on which a caller is left waiting.
    """

    def __init__(
        self,
        broker: str,
        agent_id: str,
        *,
        root: str = "agents",
        display_name: str | None = None,
        username: str | None = None,
        password: str | None = None,
        client_id: str | None = None,
        durable: bool = True,
        max_depth: int = 4,
        ask_timeout: float = 300.0,
        require_owner: bool = True,
        owner_in_topic: str = "accept",
        owner_enforced: bool = False,
        keepalive: int = 30,
        max_workers: int = 4,
        max_job_seconds: float = 1800.0,
    ) -> None:
        self.broker = broker
        self.agent_id = agent_id
        self.root = root
        self.display_name = display_name or agent_id
        self.max_depth = max_depth
        self.ask_timeout = ask_timeout
        self.require_owner = require_owner
        # v1.4: "off" serves the v1.3 form only, "accept" serves both. There is
        # no mode that refuses the old form — refusing is enforcement, and
        # enforcement belongs to the broker.
        self.owner_in_topic = "off" if owner_in_topic == "off" else "accept"
        # Whether the broker enforces who a requester may claim to be. Stated by
        # whoever applied the rules; this agent cannot observe it.
        self.owner_enforced = bool(owner_enforced)
        self.keepalive = keepalive
        self.max_job_seconds = max_job_seconds

        self._capabilities: list[dict[str, Any]] = []
        self._handlers: dict[str, Callable[..., Any]] = {}
        self._peers: dict[str, dict[str, Any]] = {}
        self._pending: dict[str, Ask] = {}
        self._active: dict[str, dict[str, Any]] = {}
        # Who asked for each job this agent served, kept after the job settles.
        # A verdict arrives long after the work, by which time ``_active`` has
        # forgotten the job — and without knowing who asked, the one rule that
        # matters cannot be applied. Bounded and insertion-ordered.
        self._served: "OrderedDict[str, str]" = OrderedDict()
        self._feedback_listeners: list[Callable[[dict[str, Any]], None]] = []
        self._lock = threading.RLock()
        self._ready = threading.Event()
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="plexus-job")

        self._job_re = topics.job_pattern(root)
        self._reg_re = topics.registry_pattern(root)
        self._cmd_re = topics.command_pattern(root)
        self._self_scope = owner_scope(agent_id)

        host, _, port = broker.replace("mqtt://", "").replace("tcp://", "").partition(":")
        self._host = host or "localhost"
        self._port = int(port or 1883)

        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id or derive_client_id(agent_id, root),
            clean_session=not durable,
        )
        if username:
            self.client.username_pw_set(username, password)
        # Presence, published by the broker itself if this process dies without
        # saying goodbye. No heartbeat service required.
        self.client.will_set(
            topics.status(root, agent_id),
            json.dumps({"status": "offline", "reason": "unexpected-disconnect", "ts": _now()}),
            qos=1,
            retain=True,
        )
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

    # ── lifecycle ────────────────────────────────────────────────────────────

    def start(self, timeout: float = 10.0) -> "MeshAgent":
        """Connect and advertise. Returns once the agent is discoverable."""
        self.client.connect_async(self._host, self._port, keepalive=self.keepalive)
        self.client.loop_start()
        if not self._ready.wait(timeout):
            raise TimeoutError(f"could not reach broker at {self.broker} within {timeout}s")
        return self

    def stop(self) -> None:
        """Withdraw from the registry and disconnect cleanly.

        The empty retained payload is what says "this agent has left". Without
        it the profile outlives the process and peers keep asking it for work
        nobody is there to do.
        """
        try:
            self._publish(topics.profile(self.root, self.agent_id), "", retain=True)
            self._publish(topics.status(self.root, self.agent_id), "", retain=True)
            time.sleep(0.15)
        finally:
            self.client.loop_stop()
            self.client.disconnect()
            self._pool.shutdown(wait=False)

    # ── advertising ──────────────────────────────────────────────────────────

    def serve(self, service: str, handler: Callable[..., Any], **meta: Any) -> "MeshAgent":
        """Offer a capability. Registering also advertises it."""
        with self._lock:
            self._handlers[service] = handler
            if not any(c["service"] == service for c in self._capabilities):
                self._capabilities.append({"service": service, **meta})
        if self._ready.is_set():
            self._advertise()
        return self

    def capabilities(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(c) for c in self._capabilities]

    def peers(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._peers.values())

    def find(self, service: str) -> str | None:
        """The id of a peer offering ``service``, or None."""
        with self._lock:
            for peer_id, profile in self._peers.items():
                if any(c.get("service") == service for c in profile.get("capabilities", [])):
                    return peer_id
        return None

    def wait_for_peer(self, service: str, timeout: float = 10.0) -> str:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            found = self.find(service)
            if found:
                return found
            time.sleep(0.05)
        raise TimeoutError(f"no peer offering {service} after {timeout}s")

    def _advertise(self) -> None:
        self._publish(
            topics.profile(self.root, self.agent_id),
            {
                "agentId": self.agent_id,
                "displayName": self.display_name,
                "status": "online",
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": self.capabilities(),
                "ownerPolicy": {
                    "required": self.require_owner,
                    "topic": self.owner_in_topic,
                    "verified": self.owner_enforced,
                },
                "ts": _now(),
            },
            retain=True,
        )
        self._publish(topics.status(self.root, self.agent_id), {"status": "online", "ts": _now()}, retain=True)

    def _publish(self, topic: str, payload: Any, retain: bool = False) -> None:
        body = payload if isinstance(payload, str) else json.dumps(payload)
        self.client.publish(topic, body, qos=1, retain=retain)

    # ── inbound ──────────────────────────────────────────────────────────────

    def _on_connect(self, client, _userdata, _flags, reason, _properties=None) -> None:
        if getattr(reason, "is_failure", False):
            log.error("broker refused the connection: %s", reason)
            return
        subscriptions = [
            (topics.invoke(self.root, self.agent_id), 1),
            (topics.cancel(self.root, self.agent_id), 1),
            # v1.5. Separate, so a broker that refuses verdicts is reported as
            # refusing verdicts rather than taking every command topic with it.
            (topics.feedback_filter(self.root, self.agent_id), 1),
            (f"{self.root}/registry/+/profile", 1),
            (f"{self.root}/registry/+/status", 1),
            (f"{self.root}/jobs/{self._self_scope}/#", 1),
        ]
        if self.owner_in_topic != "off":
            # v1.4. Subscribed separately from the v1.3 form, so a broker that
            # refuses one does not take the other with it.
            subscriptions.append((topics.invoke_filter(self.root, self.agent_id), 1))
        client.subscribe(subscriptions)
        self._advertise()
        self._ready.set()
        log.info("connected to %s as %s", self.broker, self.agent_id)

    def _on_message(self, _client, _userdata, msg) -> None:
        raw = msg.payload.decode("utf-8", "replace")

        reg = self._reg_re.match(msg.topic)
        if reg:
            return self._on_registry(reg.group(1), reg.group(2), raw)

        job = self._job_re.match(msg.topic)
        if job:
            return self._on_job_topic(job.group(1), job.group(2), job.group(3), raw)

        # v1.4: the owner is a topic segment, and it reaches the handler exactly
        # as it arrived — it is the string the broker authorised.
        topic_owner = topics.invoke_topic_owner(self.root, self.agent_id, msg.topic)
        if topic_owner is not None:
            if self.owner_in_topic == "off":
                log.info("invoke/<owner> received but owner_in_topic is off")
                return None
            return self._pool.submit(self._on_invoke, raw, topic_owner) and None

        cmd = self._cmd_re.match(msg.topic)
        if cmd and cmd.group(1) == self.agent_id:
            # Handlers run on the pool, never on the network thread: a blocking
            # handler here would stop keepalives and get the session dropped.
            return self._pool.submit(self._on_invoke, raw) and None

        judge = topics.feedback_topic_owner(self.root, self.agent_id, msg.topic)
        if judge is not None:
            return self._on_feedback(judge, raw)

        if msg.topic == topics.cancel(self.root, self.agent_id):
            return self._on_cancel(raw)

    def _remember(self, job_id: str, owner: str) -> None:
        """Record who asked, dropping the oldest once the record is full."""
        self._served[job_id] = owner
        self._served.move_to_end(job_id)
        while len(self._served) > REMEMBERED_JOBS:
            self._served.popitem(last=False)

    def _on_feedback(self, judge: str, raw: str) -> None:
        """A verdict on work this agent did (v1.5).

        The owner in the topic is the one the broker matched, so it is who this
        is from; the payload's opinion of that is not consulted. A verdict for a
        job that owner did not request is refused rather than reattributed — the
        same rule v1.4 applies to invokes, because the mesh does not quietly
        decide which of two identities somebody meant.
        """
        try:
            msg = json.loads(raw)
        except ValueError:
            log.info("feedback: unparseable payload")
            return

        job_id = str(msg.get("jobId") or "").strip()
        claimed = owner_scope(judge)

        def refuse(error: str) -> None:
            # Published where the sender can read it, in the scope it published
            # from. A verdict that vanishes silently is worse than none: the
            # requester goes on believing the mesh knows something it does not.
            log.info("feedback refused from %s: %s", judge, error)
            if job_id:
                self._publish(
                    topics.events(self.root, claimed, job_id),
                    {"jobId": job_id, "type": "feedback_refused", "error": error, "ts": _now()},
                )

        if not job_id:
            return refuse("a verdict must name the job it is about")

        verdict = str(msg.get("verdict") or "").strip().lower()
        if verdict not in VERDICTS:
            return refuse(
                f"unknown verdict {json.dumps(msg.get('verdict'))} — "
                f"expected one of {', '.join(VERDICTS)}"
            )

        with self._lock:
            owner = self._served.get(job_id)
        # Unknown is not the same as unauthorised, and the difference is worth
        # saying: the record is bounded, so a verdict on a job that scrolled off
        # the end is blameless rather than a rejection.
        if owner is None:
            return refuse(
                f"no job {job_id} here — it was never served by this agent, or it has "
                f"already fallen off the end of this agent's history"
            )
        if claimed != owner:
            return refuse(
                f"job {job_id} was requested by {owner}, and this verdict arrived as "
                f"{claimed} — refused rather than reattributed"
            )

        reason = str(msg.get("reason") or "").strip()[:MAX_FEEDBACK_REASON]
        entry = {"jobId": job_id, "verdict": verdict, "by": judge, "ts": msg.get("ts") or _now()}
        if reason:
            entry["reason"] = reason

        # On the job's own timeline too, so anything watching the mesh records
        # it without subscribing to anything new.
        body: dict[str, Any] = {
            "jobId": job_id, "owner": owner, "type": "feedback", "verdict": verdict, "ts": _now(),
        }
        if reason:
            body["note"] = reason
        self._publish(topics.events(self.root, owner, job_id), body)
        log.info("feedback: %s judged job %s %s", judge, job_id, verdict)

        for listener in list(self._feedback_listeners):
            try:
                listener(entry)
            except Exception as err:  # noqa: BLE001
                log.warning("feedback listener threw: %s", err)

    def feedback(self, peer_id: str, job_id: str, verdict: str, reason: str | None = None) -> None:
        """Say what a peer's work was worth (v1.5).

        As this agent, always: the owner segment is this agent's own scope, so a
        broker rule granting ``commands/+/feedback/<me>`` permits this and
        nothing filed under anyone else's name. An agent that delegates is a
        requester, and owes the same verdict a person does.
        """
        if verdict not in VERDICTS:
            raise ValueError(f"verdict must be one of {', '.join(VERDICTS)} — got {verdict!r}")
        body: dict[str, Any] = {"jobId": job_id, "verdict": verdict, "ts": _now()}
        if reason:
            body["reason"] = str(reason)[:MAX_FEEDBACK_REASON]
        self._publish(topics.feedback(self.root, peer_id, self._self_scope), body)

    def on_feedback(self, fn: Callable[[dict[str, Any]], None]) -> Callable[[], None]:
        """Be told when somebody judges this agent's work.

        Only verdicts that survived the owner check reach here, so a listener
        never has to wonder whether the sender was entitled to the opinion.
        """
        self._feedback_listeners.append(fn)
        return lambda: self._feedback_listeners.remove(fn)

    def _on_registry(self, peer_id: str, kind: str, raw: str) -> None:
        if peer_id == self.agent_id:
            return
        # An empty retained payload, or an offline status, both mean the peer
        # left. Deleting on either is what stops husks accumulating.
        if not raw:
            with self._lock:
                self._peers.pop(peer_id, None)
            return
        try:
            data = json.loads(raw)
        except ValueError:
            return
        if data.get("status") == "offline":
            with self._lock:
                self._peers.pop(peer_id, None)
            return
        if kind == "profile":
            with self._lock:
                self._peers[peer_id] = data

    def _on_job_topic(self, _owner: str, job_id: str, kind: str, raw: str) -> None:
        if kind != "result" or not raw:
            return
        try:
            data = json.loads(raw)
        except ValueError:
            return
        with self._lock:
            waiter = self._pending.pop(job_id, None)
        if waiter is not None:
            waiter.value = data
            waiter.event.set()

    # ── serving work ─────────────────────────────────────────────────────────

    def _on_invoke(self, raw: str, topic_owner: str | None = None) -> None:
        try:
            msg = json.loads(raw)
        except ValueError:
            log.warning("invoke: unparseable payload, dropped")
            return

        job_id = msg.get("jobId") or _job_id()
        # v1.4: the topic wins, because it is the part a broker can check. The
        # payload is not preferred over it and not ignored either — they have to
        # agree.
        owner = topic_owner if topic_owner is not None else owner_scope(msg.get("requestedBy"))

        def reject(kind: str, error: str) -> None:
            self._publish(
                topics.result(self.root, owner, job_id),
                {"jobId": job_id, "owner": owner, "type": kind, "error": error, "ts": _now()},
                retain=True,
            )
            log.info("rejected %s: %s", job_id, error)

        # Each of these refuses *before* any work starts, which is the only
        # point at which refusing is cheap.
        if topic_owner is not None:
            if owner_scope(topic_owner) != topic_owner:
                return reject(
                    "rejected",
                    f'invoke topic owner "{topic_owner}" is not owner-scoped — '
                    f'use "{owner_scope(topic_owner)}"',
                )
            claimed = msg.get("requestedBy")
            if claimed and owner_scope(claimed) != topic_owner:
                return reject(
                    "rejected",
                    f'requestedBy "{owner_scope(claimed)}" disagrees with the invoke '
                    f'topic\'s owner "{topic_owner}"',
                )
        elif self.require_owner and not msg.get("requestedBy"):
            return reject("rejected", "requestedBy is required")
        with self._lock:
            if job_id in self._active:
                return reject("duplicate", f"jobId {job_id} is already active")
        depth = int(msg.get("depth") or 0)
        if depth > self.max_depth:
            return reject("rejected", f"depth {depth} exceeds maxDepth {self.max_depth}")
        handler = self._handlers.get(msg.get("service"))
        if handler is None:
            return reject("error", f"unknown service: {msg.get('service')}")

        entry = {
            "cancelled": threading.Event(), "children": [], "owner": owner, "settled": False,
            "depth": depth, "rootJobId": msg.get("rootJobId") or job_id, "watchdog": None,
        }
        with self._lock:
            self._active[job_id] = entry
            self._remember(job_id, owner)

        def emit(payload: dict[str, Any]) -> None:
            if entry["settled"]:
                return  # suppress late output after cancel
            self._publish(topics.events(self.root, owner, job_id), {"jobId": job_id, "owner": owner, "ts": _now(), **payload})

        def settle(payload: dict[str, Any]) -> None:
            with self._lock:
                if entry["settled"]:
                    return
                entry["settled"] = True
                self._active.pop(job_id, None)
            self._publish(
                topics.result(self.root, owner, job_id),
                {"jobId": job_id, "owner": owner, "ts": _now(), **payload},
                retain=True,
            )

        entry["settle"] = settle
        emit({"type": "accepted", "service": msg.get("service")})
        ctx = JobContext(self, job_id, msg, depth, entry, emit)

        try:
            out = handler(msg, ctx)
            if entry["cancelled"].is_set():
                return
            if out is DEFER:
                # The agent will publish its own result. Arm the watchdog so the
                # job still reaches a terminal state if it never does.
                self._arm_watchdog(job_id, entry, settle)
                return
            payload = out if isinstance(out, dict) else {"value": out}
            settle({"type": payload.get("type", "result"), **payload})
        except Exception as err:  # noqa: BLE001 — a failed job must still be terminal
            if entry["cancelled"].is_set():
                return
            log.exception("job %s failed", job_id)
            settle({"type": "error", "error": f"{type(err).__name__}: {err}"})

    def _arm_watchdog(self, job_id: str, entry: dict[str, Any], settle: Callable[[dict[str, Any]], None]) -> None:
        def give_up() -> None:
            if entry["settled"]:
                return
            log.warning("job %s produced no result within %ss", job_id, self.max_job_seconds)
            settle({
                "type": "error",
                "error": f"the agent did not publish a result within {self.max_job_seconds}s. "
                         f"Check that the mesh_publish tool is available to it.",
            })

        timer = threading.Timer(self.max_job_seconds, give_up)
        timer.daemon = True
        entry["watchdog"] = timer
        timer.start()

    def publish_job(self, job_id: str, kind: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Publish to a job's topics on behalf of the agent, normalising as we go.

        Conformance is enforced here rather than trusted to the agent: ``jobId``,
        ``owner`` and ``ts`` are injected, ``type`` is defaulted, and **retain is
        forced true on result topics**. A forgotten retain flag means late
        subscribers see nothing at all, which is not a mistake worth letting an
        LLM make.
        """
        with self._lock:
            entry = self._active.get(job_id)
        if entry is None:
            return {"ok": False, "error": f"job {job_id} is not active on this agent"}

        owner = entry["owner"]
        body = {"jobId": job_id, "owner": owner, "ts": _now(), **payload}

        if kind == "result":
            body.setdefault("type", "result")
            if entry.get("watchdog") is not None:
                entry["watchdog"].cancel()
            entry["settle"](body)
            return {"ok": True, "published": "result", "jobId": job_id}

        body.setdefault("type", "progress")
        if entry["settled"]:
            return {"ok": False, "error": f"job {job_id} has already finished"}
        self._publish(topics.events(self.root, owner, job_id), body)
        return {"ok": True, "published": "events", "jobId": job_id}

    def active_jobs(self) -> list[str]:
        with self._lock:
            return list(self._active)

    # Lineage lookups, for a delegation started from a *tool call* rather than
    # from inside a handler. The agent only knows its job id, so the depth,
    # root and child-tracking have to be recovered from it — otherwise a chain
    # started by an LLM is a set of unrelated jobs, uncancellable and
    # unattributable to the request that caused it.

    def job_entry(self, job_id: str | None) -> dict[str, Any] | None:
        if not job_id:
            return None
        with self._lock:
            return self._active.get(job_id)

    def job_depth(self, job_id: str | None) -> int:
        entry = self.job_entry(job_id)
        return int(entry.get("depth", 0)) if entry else 0

    def job_root(self, job_id: str | None) -> str | None:
        entry = self.job_entry(job_id)
        return entry.get("rootJobId") or job_id if entry else job_id

    def _on_cancel(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except ValueError:
            return
        job_id = msg.get("jobId")
        with self._lock:
            entry = self._active.pop(job_id, None)
        if entry is None:
            return

        self._publish(topics.events(self.root, entry["owner"], job_id),
                      {"jobId": job_id, "type": "cancel_acknowledged", "ts": _now()})
        entry["cancelled"].set()
        entry["settled"] = True
        if entry.get("watchdog") is not None:
            entry["watchdog"].cancel()
        self._publish(
            topics.result(self.root, entry["owner"], job_id),
            {"jobId": job_id, "owner": entry["owner"], "type": "cancelled",
             "requestedBy": msg.get("requestedBy"), "ts": _now()},
            retain=True,
        )
        # Unwind the chain. Each agent cancels only its own children; those
        # agents cancel theirs in turn, so one cancel stops a chain nobody has
        # a map of.
        for child in entry["children"]:
            self._publish(topics.cancel(self.root, child["peerId"]),
                          {"jobId": child["jobId"], "requestedBy": self.agent_id})
            with self._lock:
                waiter = self._pending.pop(child["jobId"], None)
            if waiter is not None:
                waiter.value = {"type": "cancelled", "error": "cancelled by parent"}
                waiter.event.set()

    # ── delegating ───────────────────────────────────────────────────────────

    def invoke(self, peer_id: str, service: str, args: dict[str, Any] | None = None, **opts: Any) -> dict[str, Any]:
        """Ask a named peer and wait for its terminal result."""
        return self._ask(peer_id, service, args, **opts)

    def ask(self, service: str, args: dict[str, Any] | None = None, **opts: Any) -> dict[str, Any]:
        """Find a peer offering ``service`` and ask it."""
        peer_id = self.find(service)
        if peer_id is None:
            raise LookupError(f"no peer on the mesh offers {service}")
        return self._ask(peer_id, service, args, **opts)

    def _ask(
        self,
        peer_id: str,
        service: str,
        args: dict[str, Any] | None = None,
        *,
        child_depth: int = 0,
        parent_job_id: str | None = None,
        root_job_id: str | None = None,
        track: dict[str, Any] | None = None,
        timeout: float | None = None,
        id_prefix: str = "job",
    ) -> dict[str, Any]:
        # `child_depth` is the depth of the job being created, not its parent's.
        # A request entering the mesh is depth 0; a delegated ask is its
        # parent's depth plus one. Refusing here keeps a cycle off the wire.
        if child_depth > self.max_depth:
            raise RecursionError(f"ask would exceed maxDepth {self.max_depth} (depth {child_depth})")

        job_id = _job_id(id_prefix)
        waiter = Ask()
        with self._lock:
            self._pending[job_id] = waiter
        if track is not None:
            track["children"].append({"peerId": peer_id, "jobId": job_id})

        # An ask is an ordinary invoke with requestedBy set to *this* agent, so
        # the peer's result lands in our own owner scope — which we already
        # subscribe to. That is the whole return path.
        # v1.4: publish where a broker can check who we claim to be, but only to
        # a peer whose profile says it serves that form. A peer we have not
        # heard from gets the v1.3 form, which every version understands.
        with self._lock:
            peer_topic = (self._peers.get(peer_id) or {}).get("ownerPolicy", {}).get("topic")
        invoke_topic = (
            topics.invoke_as(self.root, peer_id, owner_scope(self.agent_id))
            if peer_topic in ("accept", "require")
            else topics.invoke(self.root, peer_id)
        )
        self._publish(invoke_topic, {
            "service": service,
            "args": args or {},
            "requestedBy": self.agent_id,
            "jobId": job_id,
            "parentJobId": parent_job_id,
            "rootJobId": root_job_id or parent_job_id or job_id,
            "depth": child_depth,
            "ts": _now(),
        })

        if not waiter.event.wait(timeout or self.ask_timeout):
            with self._lock:
                self._pending.pop(job_id, None)
            raise TimeoutError(f"ask {service} -> {peer_id} timed out after {timeout or self.ask_timeout}s")
        return waiter.value or {}

    def cancel(self, peer_id: str, job_id: str) -> None:
        self._publish(topics.cancel(self.root, peer_id),
                      {"jobId": job_id, "requestedBy": self.agent_id, "ts": _now()})


class JobContext:
    """Passed to every handler as its second argument."""

    def __init__(self, agent: MeshAgent, job_id: str, msg: dict[str, Any],
                 depth: int, entry: dict[str, Any], emit: Callable[[dict[str, Any]], None]) -> None:
        self._agent = agent
        self._msg = msg
        self._entry = entry
        self.job_id = job_id
        self.depth = depth
        self.emit = emit

    @property
    def cancelled(self) -> bool:
        """Check this around long work — cancellation is cooperative."""
        return self._entry["cancelled"].is_set()

    def progress(self, message: str, **extra: Any) -> None:
        """Publish a milestone. Advisory, never retained — late subscribers miss it."""
        self.emit({"type": "progress", "message": message, **extra})

    def peers(self) -> list[dict[str, Any]]:
        return self._agent.peers()

    def find(self, service: str) -> str | None:
        return self._agent.find(service)

    def ask(self, peer_id: str, service: str, args: dict[str, Any] | None = None, **opts: Any) -> dict[str, Any]:
        """Delegate to a named peer. Lineage and depth are filled in for you."""
        return self._agent._ask(
            peer_id, service, args,
            child_depth=self.depth + 1,
            parent_job_id=self.job_id,
            root_job_id=self._msg.get("rootJobId") or self.job_id,
            track=self._entry,
            id_prefix="ask",
            **opts,
        )

    def ask_any(self, service: str, args: dict[str, Any] | None = None, **opts: Any) -> dict[str, Any]:
        """Find a peer offering ``service`` and delegate to it."""
        peer_id = self._agent.find(service)
        if peer_id is None:
            raise LookupError(f"no peer on the mesh offers {service}")
        return self.ask(peer_id, service, args, **opts)
