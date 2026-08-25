"""Turning a mesh job into a Hermes agent turn.

**This is the only genuinely Hermes-specific file in the plugin.** Everything
else — discovery, delegation, lineage, hop limits, cancellation, durability —
is protocol behaviour in ``protocol.py``.

Hermes offers two ways to make the agent do something, and they differ in the
one way that matters: whether you get the answer back.

``api``
    POST to the OpenAI-compatible ``/v1/chat/completions`` endpoint. The turn is
    isolated and the response comes back synchronously, so the job settles the
    moment the agent finishes. This is the closest analogue to OpenClaw's
    subagent, and the better mode when it is available. It requires the API
    server to be enabled in the user's Hermes config.

``inject``
    ``ctx.inject_message()`` pushes the job into a session. Nothing is returned:
    the agent works in its own time and reports by calling the ``mesh_publish``
    tool. The handler returns DEFER and a watchdog guarantees the job still
    reaches a terminal state if the agent never does.

    This mode depends on the agent *choosing* to call the tool, which is a real
    weakness and is why ``api`` is preferred. The prompt says so explicitly.

``auto`` (default) picks ``api`` when a base URL is configured, else ``inject``.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any
from urllib import error, request

from .protocol import DEFER

log = logging.getLogger("plexus.executor")

_FENCE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.S)
_BARE = re.compile(r"(\{.*\})", re.S)


def parse_output(text: str) -> dict[str, Any]:
    """Best-effort structured result from an agent's free text.

    Agents wrap JSON in prose and fences no matter how firmly they are asked not
    to. Falling back to the raw text keeps a job terminal and readable rather
    than failing it over formatting.
    """
    candidates = [text.strip()]
    fenced = _FENCE.search(text)
    if fenced:
        candidates.insert(0, fenced.group(1))
    else:
        bare = _BARE.search(text)
        if bare:
            candidates.append(bare.group(1))

    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except (ValueError, TypeError):
            continue
        if isinstance(parsed, dict):
            parsed.setdefault("type", "result")
            return parsed

    return {"type": "result", "output": text.strip()}


class Executor:
    """Runs a rendered prompt as a Hermes agent turn."""

    def __init__(self, ctx: Any, config: dict[str, Any]) -> None:
        self.ctx = ctx
        self.api_url = (config.get("apiUrl") or "").rstrip("/")
        self.api_key = config.get("apiKey") or ""
        self.model = config.get("model") or "hermes"
        self.timeout = float(config.get("executorTimeoutSeconds") or 900)
        mode = (config.get("executor") or "auto").lower()
        self.mode = ("api" if self.api_url else "inject") if mode == "auto" else mode

        if self.mode == "api" and not self.api_url:
            raise ValueError('executor "api" needs apiUrl, e.g. http://127.0.0.1:8000/v1')
        if self.mode == "inject" and not hasattr(ctx, "inject_message"):
            raise ValueError(
                'executor "inject" needs ctx.inject_message, which this Hermes build does not expose. '
                "Enable the API server and set apiUrl instead."
            )

    def run(self, prompt: str, job: dict[str, Any], mesh_ctx: Any) -> Any:
        if self.mode == "api":
            return self._via_api(prompt, mesh_ctx)
        return self._via_inject(prompt, job)

    # ── api: isolated turn, synchronous result ───────────────────────────────

    def _via_api(self, prompt: str, mesh_ctx: Any) -> dict[str, Any]:
        mesh_ctx.progress("running")
        body = json.dumps({
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
        }).encode()
        headers = {"content-type": "application/json"}
        if self.api_key:
            headers["authorization"] = f"Bearer {self.api_key}"

        req = request.Request(f"{self.api_url}/chat/completions", data=body, headers=headers)
        try:
            with request.urlopen(req, timeout=self.timeout) as response:
                payload = json.loads(response.read().decode())
        except error.HTTPError as err:
            detail = err.read().decode("utf-8", "replace")[:300]
            raise RuntimeError(f"Hermes API returned {err.code}: {detail}") from None
        except error.URLError as err:
            raise RuntimeError(f"cannot reach the Hermes API at {self.api_url}: {err.reason}") from None

        try:
            text = payload["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError):
            raise RuntimeError(f"unexpected response shape from the Hermes API: {str(payload)[:200]}") from None
        return parse_output(text or "")

    # ── inject: the agent reports for itself ─────────────────────────────────

    def _via_inject(self, prompt: str, job: dict[str, Any]) -> Any:
        job_id = job["jobId"]
        briefing = (
            f"{prompt}\n\n"
            f"---\n"
            f"This request came from the Plexus mesh as job `{job_id}`, "
            f"asked by `{job.get('requestedBy', 'unknown')}`.\n"
            f"When you are done you MUST report the result by calling the "
            f"`mesh_publish` tool with kind=\"result\" and jobId=\"{job_id}\". "
            f"Nothing you say in chat reaches the requester — only that tool call does.\n"
            f"Use the same tool with kind=\"events\" to report progress on long work."
        )
        kwargs: dict[str, Any] = {"role": "user"}
        # session_key keeps mesh work out of whatever the human is doing, when
        # the running Hermes supports it.
        try:
            self.ctx.inject_message(briefing, session_key=f"plexus:{job_id}", **kwargs)
        except TypeError:
            self.ctx.inject_message(briefing, **kwargs)
        log.info("job %s injected; awaiting mesh_publish", job_id)
        return DEFER
