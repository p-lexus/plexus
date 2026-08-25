"""Topic construction and parsing.

Pure: no I/O, no state. Everything the mesh addresses is derived here, so the
topic layout is defined in exactly one place and is directly testable.

This is a deliberate mirror of ``packages/agent/index.js`` and
``src/mesh/topics.ts``. The three must agree exactly — they are three
implementations of one wire format, and a difference here is an
interoperability bug that shows up as an agent nobody can reach.
"""

from __future__ import annotations

import re

PROTOCOL_VERSION = "1.3"

_OWNER_STRIP = re.compile(r"[^a-z0-9_-]+")
_OWNER_EDGES = re.compile(r"^-+|-+$")


def owner_scope(requested_by: str | None) -> str:
    """Owner scope for job topics.

    Lowercased, ``[a-z0-9_-]`` only, edges trimmed. Empty becomes ``public`` —
    which is why a client that omits ``requestedBy`` never sees its own results
    on its own filter.
    """
    s = _OWNER_STRIP.sub("-", str(requested_by or "").lower())
    s = _OWNER_EDGES.sub("", s)
    return s or "public"


def profile(root: str, agent_id: str) -> str:
    return f"{root}/registry/{agent_id}/profile"


def status(root: str, agent_id: str) -> str:
    return f"{root}/registry/{agent_id}/status"


def invoke(root: str, agent_id: str) -> str:
    return f"{root}/commands/{agent_id}/invoke"


def cancel(root: str, agent_id: str) -> str:
    return f"{root}/commands/{agent_id}/cancel"


def events(root: str, owner: str, job_id: str) -> str:
    return f"{root}/jobs/{owner}/{job_id}/events"


def result(root: str, owner: str, job_id: str) -> str:
    return f"{root}/jobs/{owner}/{job_id}/result"


def job_pattern(root: str) -> re.Pattern[str]:
    """Matches any owner-scoped job topic.

    Anchored, so the unscoped ``jobs/<jobId>/...`` form can never match — job
    topics are always owner-scoped.
    """
    return re.compile(rf"^{re.escape(root)}/jobs/([^/]+)/([^/]+)/(events|result)$")


def registry_pattern(root: str) -> re.Pattern[str]:
    return re.compile(rf"^{re.escape(root)}/registry/([^/]+)/(profile|status)$")


def command_pattern(root: str) -> re.Pattern[str]:
    return re.compile(rf"^{re.escape(root)}/commands/([^/]+)/invoke$")
