"""Manages MCP server connections and converts their tools to LangChain format."""

from __future__ import annotations

import logging
from typing import Any

from langchain_core.tools import BaseTool

from databao.agent.mcp.adapter import mcp_tools_to_langchain
from databao.agent.mcp.connection import McpConnection

logger = logging.getLogger(__name__)

_VALID_TRANSPORTS = ("sse", "streamable_http")


class McpManager:
    """Owns the lifecycle of MCP server connections.

    Each connection is keyed by a server name (either user-supplied or auto-generated
    from the transport and URL/command).
    """

    def __init__(self) -> None:
        self._connections: dict[str, McpConnection] = {}

    @property
    def servers(self) -> list[str]:
        """Return names of connected MCP servers."""
        return list(self._connections)

    def connect(
        self,
        *,
        name: str | None = None,
        url: str | None = None,
        command: str | None = None,
        args: list[str] | None = None,
        env: dict[str, str] | None = None,
        headers: dict[str, Any] | None = None,
        transport: str | None = None,
        auth: Any | None = None,
    ) -> list[BaseTool]:
        """Connect to a single MCP server and return its tools as LangChain tools."""
        if command is not None and url is not None:
            raise ValueError("Specify either 'command' (stdio) or 'url' (sse/http), not both")
        if command is None and url is None:
            raise ValueError("Specify either 'command' (stdio) or 'url' (sse/http)")

        if transport is not None and transport not in _VALID_TRANSPORTS:
            raise ValueError(f"Unknown transport {transport!r}; expected one of {_VALID_TRANSPORTS}")

        if transport is None and url is not None and url.rstrip("/").endswith("/sse"):
            transport = "sse"

        resolved_auth = _resolve_auth(auth, url)

        if command is not None:
            if resolved_auth is not None:
                raise ValueError("'auth' requires a URL-based transport (SSE or Streamable HTTP), not stdio")
            connection = McpConnection.connect_stdio(command, args=args, env=env)
        elif transport == "sse":
            if url is None:
                raise ValueError("url must not be None")
            connection = McpConnection.connect_sse(url, headers=headers, auth=resolved_auth)
        else:
            if url is None:
                raise ValueError("url must not be None")
            connection = McpConnection.connect_streamable_http(url, headers=headers, auth=resolved_auth)

        server_name = name or connection.server_name
        if server_name in self._connections:
            logger.warning("MCP server %r registered more than once; replacing previous connection", server_name)
            self._connections[server_name].close()
        self._connections[server_name] = connection

        lc_tools = mcp_tools_to_langchain(connection)

        existing_names = {t.name for c in self._connections.values() if c is not connection for t in c.tools}
        for tool in lc_tools:
            if tool.name in existing_names:
                logger.warning(
                    "MCP tool name collision: '%s' from %s shadows an existing tool",
                    tool.name,
                    connection.server_name,
                )

        if not lc_tools:
            logger.warning("MCP server %s registered 0 tools", connection.server_name)
        else:
            logger.info(
                "Registered %d MCP tools from %s: %s",
                len(lc_tools),
                connection.server_name,
                [t.name for t in lc_tools],
            )

        return lc_tools

    def connect_from_config(self, config: dict[str, Any] | str) -> list[BaseTool]:
        """Parse a config and connect to all servers defined in it.

        Returns the combined list of LangChain tools from all connected servers.
        """
        from databao.agent.mcp.config import parse_mcp_config

        all_tools: list[BaseTool] = []
        for server_cfg in parse_mcp_config(config):
            tools = self.connect(
                name=server_cfg.get("name"),
                url=server_cfg.get("url"),
                command=server_cfg.get("command"),
                args=server_cfg.get("args"),
                env=server_cfg.get("env"),
                headers=server_cfg.get("headers"),
                transport=server_cfg.get("transport"),
                auth=server_cfg.get("auth"),
            )
            all_tools.extend(tools)
        return all_tools

    def close(self) -> None:
        """Close all MCP connections."""
        for conn in self._connections.values():
            conn.close()
        self._connections.clear()


def _resolve_auth(auth: Any, url: str | None) -> Any:
    """Resolve the *auth* parameter into an ``httpx.Auth`` or ``None``."""
    if auth is None or auth is False:
        return None
    if auth is True or auth == "oauth":
        if url is None:
            raise ValueError("OAuth auth requires a URL-based transport")
        from databao.agent.mcp.oauth import create_oauth_provider

        return create_oauth_provider(url)
    import httpx

    if isinstance(auth, httpx.Auth):
        return auth
    raise TypeError(f"'auth' must be True, 'oauth', or an httpx.Auth instance, got {type(auth).__name__}")
