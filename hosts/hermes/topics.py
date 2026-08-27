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

PROTOCOL_VERSION = "1.4"

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


def invoke_as(root: str, agent_id: str, owner: str) -> str:
    """Where a peer accepts work as a particular owner (v1.4).

    The owner must already be scoped. This does not scope it, because a topic
    that normalised quietly would make one identity two spellings — and a broker
    ACL matches only one of them.
    """
    return f"{root}/commands/{agent_id}/invoke/{owner}"


def invoke_filter(root: str, agent_id: str) -> str:
    return f"{root}/commands/{agent_id}/invoke/+"


def invoke_topic_owner(root: str, agent_id: str, topic: str) -> str | None:
    """The owner an invoke topic carries, or None if this is not one.

    Returned exactly as it arrived: the string in the topic is the one the
    broker authorised, and any other string is a different identity.
    """
    prefix = f"{root}/commands/{agent_id}/invoke/"
    if not topic.startswith(prefix):
        return None
    rest = topic[len(prefix):]
    return rest if rest and "/" not in rest else None


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
