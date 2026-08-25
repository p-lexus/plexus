"""Capability prompt rendering.

Pure. This is the whole coupling between the mesh and what an agent actually
does: a capability is a name, an argument schema and a prompt template, and
dispatching a job means rendering that template.

The bridge contains no service name anywhere. It never learns what
``code.review`` means, and it does not need to.
"""

from __future__ import annotations

import os
import re
from typing import Any, Callable

_ENV_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
_ARG_REF = re.compile(r"\{\{\s*(\w+)\s*\}\}")


def render_prompt(
    template: str,
    args: dict[str, Any] | None,
    job_id: str,
    requested_by: str | None,
    variables: Callable[[str], str] | dict[str, str] | None = None,
) -> str:
    """Render a capability's prompt template.

    Substitution order is a security property, not a style choice:

    1. ``${VAR}``  — deployment values, from config then the environment
    2. ``{{jobId}}`` / ``{{requestedBy}}`` — job identity
    3. ``{{arg}}``  — caller-supplied arguments

    **Environment expansion must happen first.** Reversed, a caller passes
    ``"${SOME_SECRET}"`` as an argument value, the bridge expands it on the next
    pass, and every invoke becomes an arbitrary environment read by anyone who
    can publish to the mesh.
    """
    if not isinstance(template, str):
        return ""

    if variables is None:
        lookup: Callable[[str], str] = lambda name: os.environ.get(name, "")
    elif isinstance(variables, dict):
        # Config first, then the environment: identifiers are versioned with the
        # deployment, secrets stay in the environment.
        lookup = lambda name: variables.get(name, os.environ.get(name, ""))
    else:
        lookup = variables

    out = _ENV_REF.sub(lambda m: str(lookup(m.group(1))), template)
    out = out.replace("{{jobId}}", job_id).replace("{{requestedBy}}", requested_by or "unknown")
    return _ARG_REF.sub(lambda m: str((args or {}).get(m.group(1), "")), out)


def missing_arguments(template: str, schema: dict[str, Any] | None) -> list[str]:
    """Placeholders in a prompt with no matching argument in the schema.

    A prompt using ``{{repo}}`` with no ``repo`` argument renders empty at
    dispatch and silently produces a bad job — so it is worth refusing to load
    rather than discovering it in a result nobody can explain.
    """
    declared = set(schema or {}) | {"jobId", "requestedBy"}
    used = set(_ARG_REF.findall(template or ""))
    return sorted(used - declared)
