"""Loading and validating the plugin's configuration.

Config comes from, in order of precedence:

1. ``~/.hermes/plexus.json`` (or ``$PLEXUS_CONFIG``)
2. ``PLEXUS_*`` environment variables
3. defaults

Secrets are referenced as ``${VAR}`` and resolved from the environment, so the
file can be committed and the broker password cannot.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .prompts import missing_arguments

DEFAULTS: dict[str, Any] = {
    "broker": "",
    "root": "agents",
    "agentId": "hermes",
    "displayName": "Hermes",
    "maxDepth": 4,
    "askTimeoutSeconds": 300,
    "maxJobSeconds": 1800,
    "requireOwner": True,
    "durable": True,
    "executor": "auto",
    "executorTimeoutSeconds": 900,
    "capabilities": [],
    "promptVars": {},
}

_ENV_KEYS = {
    "PLEXUS_BROKER": "broker",
    "PLEXUS_ROOT": "root",
    "PLEXUS_AGENT_ID": "agentId",
    "PLEXUS_DISPLAY_NAME": "displayName",
    "PLEXUS_USERNAME": "username",
    "PLEXUS_PASSWORD": "password",
    "PLEXUS_CLIENT_ID": "clientId",
    "PLEXUS_API_URL": "apiUrl",
    "PLEXUS_API_KEY": "apiKey",
    "PLEXUS_MODEL": "model",
    "PLEXUS_EXECUTOR": "executor",
}


def _expand(value: Any) -> Any:
    """Expand ``${VAR}`` from the environment, recursively."""
    if isinstance(value, str):
        out = value
        for name in {n for n in _iter_refs(value)}:
            out = out.replace("${" + name + "}", os.environ.get(name, ""))
        return out
    if isinstance(value, dict):
        return {k: _expand(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand(v) for v in value]
    return value


def _iter_refs(text: str):
    import re
    return re.findall(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", text)


def config_path() -> Path:
    return Path(os.environ.get("PLEXUS_CONFIG") or Path.home() / ".hermes" / "plexus.json")


def load(path: Path | None = None) -> dict[str, Any]:
    """Read the config. Returns defaults if there is no file."""
    path = path or config_path()
    config = dict(DEFAULTS)

    if path.exists():
        try:
            config.update(json.loads(path.read_text()))
        except ValueError as err:
            raise ValueError(f"{path} is not valid JSON — {err}") from None

    for env_key, config_key in _ENV_KEYS.items():
        if os.environ.get(env_key):
            config[config_key] = os.environ[env_key]

    # Don't expand capability prompts here: their ${VAR}s are resolved at
    # dispatch instead. The capability catalog is published to a *retained*
    # topic, and resolving deployment values before publishing would broadcast
    # every secret to anyone subscribed to the registry.
    capabilities = config.get("capabilities") or []
    config = {k: (_expand(v) if k != "capabilities" else v) for k, v in config.items()}
    config["capabilities"] = capabilities

    validate(config)
    return config


def validate(config: dict[str, Any]) -> None:
    """Fail loudly at load rather than confusingly at dispatch."""
    seen: set[str] = set()
    for index, capability in enumerate(config.get("capabilities") or []):
        where = f"capabilities[{index}]"
        service = capability.get("service")
        if not service:
            raise ValueError(f"{where} has no 'service' name")
        if service in seen:
            raise ValueError(f"{where}: duplicate service {service!r}")
        seen.add(service)
        if not capability.get("prompt"):
            raise ValueError(f"{where} ({service}) has no 'prompt'")
        # A prompt using {{repo}} with no 'repo' argument renders empty and
        # silently produces a bad job. Refuse it now.
        missing = missing_arguments(capability["prompt"], capability.get("requestSchema"))
        if missing:
            raise ValueError(
                f"{where} ({service}): prompt uses {', '.join('{{' + m + '}}' for m in missing)} "
                f"but requestSchema does not declare {'it' if len(missing) == 1 else 'them'}"
            )

    executor = (config.get("executor") or "auto").lower()
    if executor not in ("auto", "api", "inject"):
        raise ValueError(f"executor must be 'auto', 'api' or 'inject', got {executor!r}")
