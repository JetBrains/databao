from __future__ import annotations

import asyncio
import logging
import threading
from contextlib import AsyncExitStack
from typing import Any

import httpx
from mcp import ClientSession
from mcp.client.sse import sse_client
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.client.streamable_http import streamablehttp_client
from mcp.types import CallToolResult
from mcp.types import Tool as McpTool

logger = logging.getLogger(__name__)

_CONNECT_TIMEOUT_SECONDS = 30
_CONNECT_TIMEOUT_AUTH_SECONDS = 300
_CALL_TIMEOUT_SECONDS = 120


class McpConnection:
    """Manages a persistent connection to an MCP server.

    The MCP SDK is async-only, so this class runs a background event loop in a daemon thread
    to keep the connection alive and bridge sync calls to async MCP operations.
    """

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._exit_stack: AsyncExitStack | None = None
        self._session: ClientSession | None = None
        self._tools: list[McpTool] = []
        self._server_name: str = ""

    @classmethod
    def connect_stdio(
        cls,
        command: str,
        args: list[str] | None = None,
        env: dict[str, str] | None = None,
    ) -> McpConnection:
        """Connect to an MCP server via stdio transport."""
        conn = cls()
        conn._server_name = f"stdio:{command}"
        conn._start_loop()
        try:
            conn._run_sync(conn._setup_stdio(command, args or [], env))
        except Exception:
            conn.close()
            raise
        return conn

    @classmethod
    def connect_sse(
        cls,
        url: str,
        headers: dict[str, Any] | None = None,
        auth: httpx.Auth | None = None,
    ) -> McpConnection:
        """Connect to an MCP server via SSE transport."""
        conn = cls()
        conn._server_name = f"sse:{url}"
        conn._start_loop()
        timeout = _CONNECT_TIMEOUT_AUTH_SECONDS if auth else _CONNECT_TIMEOUT_SECONDS
        try:
            conn._run_sync(conn._setup_sse(url, headers, auth=auth), timeout=timeout)
        except Exception:
            conn.close()
            raise
        return conn

    @classmethod
    def connect_streamable_http(
        cls,
        url: str,
        headers: dict[str, Any] | None = None,
        auth: httpx.Auth | None = None,
    ) -> McpConnection:
        """Connect to an MCP server via Streamable HTTP transport."""
        conn = cls()
        conn._server_name = f"http:{url}"
        conn._start_loop()
        timeout = _CONNECT_TIMEOUT_AUTH_SECONDS if auth else _CONNECT_TIMEOUT_SECONDS
        try:
            conn._run_sync(conn._setup_streamable_http(url, headers, auth=auth), timeout=timeout)
        except Exception:
            conn.close()
            raise
        return conn

    @property
    def tools(self) -> list[McpTool]:
        return list(self._tools)

    @property
    def server_name(self) -> str:
        return self._server_name

    def call_tool(self, name: str, arguments: dict[str, Any]) -> CallToolResult:
        """Call an MCP tool synchronously."""
        if self._session is None:
            raise RuntimeError("MCP connection is not established")
        result: CallToolResult = self._run_sync(self._session.call_tool(name, arguments), timeout=_CALL_TIMEOUT_SECONDS)
        return result

    def close(self) -> None:
        """Shut down the MCP connection and background event loop."""
        if self._exit_stack is not None and self._loop is not None:
            try:
                future = asyncio.run_coroutine_threadsafe(self._exit_stack.aclose(), self._loop)
                future.result(timeout=5)
            except Exception:
                logger.debug("Could not cleanly close MCP connection %s", self._server_name, exc_info=True)
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._session = None
        self._exit_stack = None
        self._loop = None
        self._thread = None

    # --- Private helpers ---

    def _start_loop(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True, name=f"mcp-{id(self)}")
        self._thread.start()

    def _run_sync(self, coro: Any, *, timeout: float = _CONNECT_TIMEOUT_SECONDS) -> Any:
        if self._loop is None:
            raise RuntimeError("MCP event loop is not running")
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result(timeout=timeout)

    async def _finalize_session(self) -> None:
        """Initialize the session, list tools, and log. Called by all _setup_* methods."""
        if self._session is None:
            raise RuntimeError("Session not initialized")
        await self._session.initialize()
        result = await self._session.list_tools()
        self._tools = list(result.tools)
        logger.info("Connected to MCP server %s, %d tools available", self._server_name, len(self._tools))

    async def _setup_stdio(self, command: str, args: list[str], env: dict[str, str] | None) -> None:
        self._exit_stack = AsyncExitStack()
        server_params = StdioServerParameters(command=command, args=args, env=env)
        read, write = await self._exit_stack.enter_async_context(stdio_client(server_params))
        self._session = await self._exit_stack.enter_async_context(ClientSession(read, write))
        await self._finalize_session()

    async def _setup_sse(self, url: str, headers: dict[str, Any] | None, *, auth: httpx.Auth | None = None) -> None:
        self._exit_stack = AsyncExitStack()
        read, write = await self._exit_stack.enter_async_context(sse_client(url, headers=headers, auth=auth))
        self._session = await self._exit_stack.enter_async_context(ClientSession(read, write))
        await self._finalize_session()

    async def _setup_streamable_http(
        self, url: str, headers: dict[str, Any] | None, *, auth: httpx.Auth | None = None
    ) -> None:
        self._exit_stack = AsyncExitStack()
        read, write, _ = await self._exit_stack.enter_async_context(
            streamablehttp_client(url, headers=headers, auth=auth)
        )
        self._session = await self._exit_stack.enter_async_context(ClientSession(read, write))
        await self._finalize_session()
