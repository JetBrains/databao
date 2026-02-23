"""Parse Claude-Code-style MCP server configuration."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_SERVER_KEYS = {"command", "url"}


def parse_mcp_config(config: dict[str, Any] | str) -> list[dict[str, Any]]:
    """Parse an MCP config into a flat list of per-server dicts.

    Accepted formats
    ----------------
    1. Claude Code / Anthropic style::

           {"mcpServers": {"name": {server_cfg}, ...}}

    2. Bare servers dict (every value is a server config)::

           {"name": {server_cfg}, ...}

    3. Single server config::

           {"command": "npx", "args": [...], "env": {...}}

    *config* can also be a JSON string or a path to a ``.json`` file.

    Returns a list of server config dicts, each with a ``"name"`` key added.
    """
    if isinstance(config, str):
        config = _load_string_or_file(config)

    if not isinstance(config, dict):
        raise TypeError(f"Expected a dict or str, got {type(config).__name__}")

    if not config:
        return []

    # Format 1: {"mcpServers": {...}}
    if "mcpServers" in config:
        return _parse_servers_dict(config["mcpServers"])

    # Format 3: single server config  (has "command" or "url" at the top level)
    if _SERVER_KEYS & config.keys():
        entry = dict(config)
        entry.setdefault("name", "default")
        return [entry]

    # Format 2: bare servers dict  {"name": {server_cfg}, ...}
    if config and all(isinstance(v, dict) for v in config.values()):
        return _parse_servers_dict(config)

    raise ValueError(
        'Unrecognised MCP config format. Expected {"mcpServers": {...}}, a servers dict, or a single server config.'
    )


def _parse_servers_dict(servers: Any) -> list[dict[str, Any]]:
    if not isinstance(servers, dict):
        raise TypeError(f"'mcpServers' value must be a dict, got {type(servers).__name__}")
    result: list[dict[str, Any]] = []
    for name, cfg in servers.items():
        if not isinstance(cfg, dict):
            raise TypeError(f"Server config for '{name}' must be a dict, got {type(cfg).__name__}")
        entry = dict(cfg)
        entry.setdefault("name", name)
        result.append(entry)
    return result


def _load_string_or_file(value: str) -> dict[str, Any]:
    """Try to parse *value* as JSON; if that fails, treat it as a file path."""
    stripped = value.strip()
    if stripped.startswith("{"):
        return json.loads(stripped)  # type: ignore[no-any-return]

    path = Path(value)
    if path.is_file():
        return json.loads(path.read_text(encoding="utf-8"))  # type: ignore[no-any-return]

    raise ValueError(f"'{value}' is neither valid JSON nor an existing file path")
