"""Plexus for Hermes — put a Hermes agent on the agent mesh.

Drop this directory into ``~/.hermes/plugins/`` and Hermes will call
``register(ctx)`` at startup. From then on the agent is a peer on the mesh:
other agents can discover its capabilities and send it work, and it can ask
them for help with anything outside its own expertise.

The layering, so it is clear what this file is responsible for:

    protocol.py   the mesh — durable session, discovery, delegation, lineage
    executor.py   the only Hermes-specific part: running an agent turn
    tools.py      what the agent can call: mesh_publish, mesh_peers, mesh_ask
    prompts.py    capability templates
    config.py     ~/.hermes/plexus.json
    __init__.py   this file — wiring, and nothing else

Design note: ``register`` must never raise. A plugin that throws here takes
Hermes down with it, and a mesh being unreachable is not a good enough reason
to stop someone using their agent. Every failure below is logged and swallowed.
"""

from __future__ import annotations

import atexit
import logging
from typing import Any

log = logging.getLogger("plexus")

__version__ = "1.3.0"
__all__ = ["register"]

#: Module-level, because Hermes gives no shutdown hook and a reload must not
#: leave a second connection holding the same session — two clients sharing a
#: client id fight over it and the broker kicks each in turn.
_agent: Any = None


def _shutdown() -> None:
    global _agent
    if _agent is not None:
        try:
            _agent.stop()
        except Exception:  # noqa: BLE001 — best effort at exit
            pass
        _agent = None


def register(ctx: Any) -> None:
    """Hermes plugin entry point."""
    global _agent

    from . import config as config_module
    from .executor import Executor
    from .prompts import render_prompt
    from .protocol import MeshAgent
    from .tools import register_tools

    try:
        config = config_module.load()
    except Exception as err:  # noqa: BLE001
        log.error("plexus: configuration error — %s", err)
        return

    if not config.get("broker"):
        log.info(
            "plexus: no broker configured, staying offline. "
            "Set PLEXUS_BROKER or create %s to join a mesh.", config_module.config_path(),
        )
        return

    if _agent is not None:
        # A reload re-runs register(). Re-registering tools is harmless; opening
        # a second connection on the same client id is not.
        log.info("plexus: already connected as %s, reusing the session", _agent.agent_id)
        register_tools(ctx, _agent)
        return

    try:
        executor = Executor(ctx, config)
    except Exception as err:  # noqa: BLE001
        log.error("plexus: cannot set up the executor — %s", err)
        return

    agent = MeshAgent(
        config["broker"],
        config["agentId"],
        root=config["root"],
        display_name=config.get("displayName"),
        username=config.get("username"),
        password=config.get("password"),
        client_id=config.get("clientId"),
        durable=bool(config.get("durable", True)),
        max_depth=int(config.get("maxDepth", 4)),
        ask_timeout=float(config.get("askTimeoutSeconds", 300)),
        max_job_seconds=float(config.get("maxJobSeconds", 1800)),
        require_owner=bool(config.get("requireOwner", True)),
    )

    prompt_vars = config.get("promptVars") or {}

    def make_handler(capability: dict[str, Any]):
        def handler(job: dict[str, Any], mesh_ctx: Any) -> Any:
            prompt = render_prompt(
                capability["prompt"],
                job.get("args"),
                job["jobId"],
                job.get("requestedBy"),
                prompt_vars,
            )
            return executor.run(prompt, job, mesh_ctx)
        return handler

    for capability in config.get("capabilities") or []:
        agent.serve(
            capability["service"],
            make_handler(capability),
            description=capability.get("description", ""),
            requestSchema=capability.get("requestSchema") or {},
        )

    # Tools are registered before connecting, so the agent can never receive a
    # job it has no way to report on. In OpenClaw the equivalent guard was in
    # the wrong order and executors ran with no mesh tools at all — they
    # produced plausible output and published nothing.
    register_tools(ctx, agent)

    try:
        agent.start(timeout=float(config.get("connectTimeoutSeconds", 10)))
    except Exception as err:  # noqa: BLE001
        log.error(
            "plexus: could not reach the broker at %s — %s. "
            "The mesh tools are registered but will report as disconnected.",
            config["broker"], err,
        )
        return

    _agent = agent
    atexit.register(_shutdown)

    log.info(
        "plexus: %s online on %s (root %r, executor %r) offering %s",
        agent.agent_id, config["broker"], config["root"], executor.mode,
        ", ".join(c["service"] for c in agent.capabilities()) or "nothing",
    )
