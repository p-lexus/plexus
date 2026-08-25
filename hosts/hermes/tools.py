"""Tools registered into Hermes, so its agent can act on the mesh.

Four things an agent needs: report on its own job, see who else is out there,
ask one of them for help, and check the connection.

The most important is ``mesh_publish``. In ``inject`` mode it is the *only* way
a job's result reaches the requester — an agent that answers in chat has, from
the mesh's point of view, said nothing at all. That is worth being blunt about
in the tool description, because the model reads it.
"""

from __future__ import annotations

import json
import logging
from typing import Any

log = logging.getLogger("plexus.tools")


def _ok(**payload: Any) -> str:
    return json.dumps({"success": True, **payload})


def _fail(error: str, **payload: Any) -> str:
    return json.dumps({"success": False, "error": error, **payload})


def register_tools(ctx: Any, agent: Any, toolset: str = "plexus") -> None:
    """Register the mesh tools with a Hermes PluginContext."""

    # ── mesh_publish ─────────────────────────────────────────────────────────

    publish_schema = {
        "name": "mesh_publish",
        "description": (
            "Report on a Plexus mesh job. This is the ONLY way your work reaches whoever asked "
            "for it — replying in chat does not. Use kind='result' exactly once, when finished; "
            "use kind='events' as often as you like for progress on long work."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "jobId": {"type": "string", "description": "The job id given in the request."},
                "kind": {"type": "string", "enum": ["result", "events"],
                         "description": "'result' is terminal and can only be sent once."},
                "payload": {
                    "type": "object",
                    "description": (
                        "What to publish. For a result, include your findings and a 'type' "
                        "(e.g. 'review', 'error'). jobId, owner and ts are added automatically."
                    ),
                },
            },
            "required": ["jobId", "kind", "payload"],
        },
    }

    def handle_publish(params: dict[str, Any], **_kwargs: Any) -> str:
        job_id = params.get("jobId")
        kind = params.get("kind", "events")
        payload = params.get("payload") or {}
        if kind not in ("result", "events"):
            return _fail(f"kind must be 'result' or 'events', got {kind!r}")
        if isinstance(payload, str):
            # Models pass a JSON string here often enough to be worth handling.
            try:
                payload = json.loads(payload)
            except ValueError:
                payload = {"output": payload}
        if not isinstance(payload, dict):
            return _fail("payload must be an object")

        outcome = agent.publish_job(job_id, kind, payload)
        if not outcome.get("ok"):
            return _fail(outcome.get("error", "publish failed"),
                         activeJobs=agent.active_jobs())
        return _ok(**outcome)

    ctx.register_tool(name="mesh_publish", toolset=toolset, schema=publish_schema, handler=handle_publish)

    # ── mesh_peers ───────────────────────────────────────────────────────────

    peers_schema = {
        "name": "mesh_peers",
        "description": (
            "List the other agents on the Plexus mesh and what each can do. Use this before "
            "mesh_ask to find who handles something outside your own expertise."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "service": {"type": "string", "description": "Optional: only agents offering this capability."},
            },
        },
    }

    def handle_peers(params: dict[str, Any], **_kwargs: Any) -> str:
        service = params.get("service")
        peers = agent.peers()
        if service:
            peers = [p for p in peers if any(c.get("service") == service for c in p.get("capabilities", []))]
        return _ok(peers=[{
            "agentId": p.get("agentId"),
            "displayName": p.get("displayName"),
            "capabilities": [
                {"service": c.get("service"), "description": c.get("description", "")}
                for c in p.get("capabilities", [])
            ],
        } for p in peers])

    ctx.register_tool(name="mesh_peers", toolset=toolset, schema=peers_schema, handler=handle_peers)

    # ── mesh_ask ─────────────────────────────────────────────────────────────

    ask_schema = {
        "name": "mesh_ask",
        "description": (
            "Ask another agent on the mesh to do something you cannot, and wait for its answer. "
            "Use it when a request needs expertise you do not have — check mesh_peers first. "
            "The answer comes back to you; fold it into your own reply rather than telling the "
            "requester to go and ask someone else."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "agentId": {"type": "string", "description": "Who to ask. Omit to use any agent offering the service."},
                "service": {"type": "string", "description": "The capability to invoke."},
                "args": {"type": "object", "description": "Arguments, matching that capability's schema."},
                "jobId": {"type": "string", "description": "Your current job id, so the chain stays traceable."},
                "timeoutSeconds": {"type": "number"},
            },
            "required": ["service"],
        },
    }

    def handle_ask(params: dict[str, Any], **_kwargs: Any) -> str:
        service = params.get("service")
        peer_id = params.get("agentId") or agent.find(service)
        if not peer_id:
            return _fail(f"no agent on the mesh offers {service}",
                         available=[c.get("service") for p in agent.peers()
                                    for c in p.get("capabilities", [])])
        parent = params.get("jobId")
        # Lineage keeps a chain of agents attributable to the one request that
        # started it, and lets a cancel find everything to stop.
        depth = agent.job_depth(parent) if parent else 0
        try:
            answer = agent._ask(
                peer_id, service, params.get("args") or {},
                child_depth=depth + 1,
                parent_job_id=parent,
                root_job_id=agent.job_root(parent) if parent else None,
                track=agent.job_entry(parent) if parent else None,
                timeout=params.get("timeoutSeconds"),
                id_prefix="ask",
            )
        except RecursionError as err:
            return _fail(str(err))
        except TimeoutError as err:
            return _fail(str(err))
        return _ok(agentId=peer_id, service=service, answer=answer)

    ctx.register_tool(name="mesh_ask", toolset=toolset, schema=ask_schema, handler=handle_ask)

    # ── mesh_status ──────────────────────────────────────────────────────────

    status_schema = {
        "name": "mesh_status",
        "description": "Report this agent's Plexus connection: identity, capabilities, peers and active jobs.",
        "parameters": {"type": "object", "properties": {}},
    }

    def handle_status(_params: dict[str, Any], **_kwargs: Any) -> str:
        return _ok(
            agentId=agent.agent_id,
            broker=agent.broker,
            root=agent.root,
            connected=agent.client.is_connected(),
            capabilities=[c.get("service") for c in agent.capabilities()],
            peers=[p.get("agentId") for p in agent.peers()],
            activeJobs=agent.active_jobs(),
        )

    ctx.register_tool(name="mesh_status", toolset=toolset, schema=status_schema, handler=handle_status)
