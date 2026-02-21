import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, TextIO

from langchain_core.language_models.chat_models import BaseChatModel
from pandas import DataFrame

from databao.core.data_source import DBDataSource, DFDataSource, Sources
from databao.core.domain import Domain, _Domain
from databao.core.thread import Thread
from databao.databases import DBConnection

if TYPE_CHECKING:
    from databao.configs.agent import AgentConfig
    from databao.configs.llm import LLMConfig
    from databao.core.cache import Cache
    from databao.core.executor import Executor
    from databao.core.visualizer import Visualizer
    from databao.mcp.connection import McpConnection

logger = logging.getLogger(__name__)


class Agent:
    """An agent manages all databases and Dataframes as well as the context for them.
    Agent determines what LLM to use, what executor to use and how to visualize data for all threads.
    Several threads can be spawned out of the agent.
    """

    def __init__(
            self,
            domain: "_Domain",
            llm: "LLMConfig",
            agent_config: "AgentConfig",
            data_executor: "Executor",
            visualizer: "Visualizer",
            cache: "Cache",
            *,
            name: str = "default_agent",
            rows_limit: int,
            stream_ask: bool = True,
            stream_plot: bool = False,
            lazy_threads: bool = False,
            auto_output_modality: bool = True,
    ):
        self.__domain = domain
        self.__name = name
        self.__llm = llm.new_chat_model()
        self.__llm_config = llm
        self.__agent_config = agent_config

        self.__executor = data_executor
        self.__visualizer = visualizer
        self.__cache = cache

        # MCP server name → connection (kept alive for tool calls)
        self.__mcp_connections: dict[str, McpConnection] = {}

        # Thread defaults
        self.__rows_limit = rows_limit
        self.__lazy_threads = lazy_threads
        self.__auto_output_modality = auto_output_modality
        self.__stream_ask = stream_ask
        self.__stream_plot = stream_plot

        self._init_executor()

    def _init_executor(self) -> None:
        self.__domain.finalize_sources()
        for db_source in self.sources.dbs.values():
            if db_source.connectable:
                self.executor.register_db(db_source)
        for df_source in self.sources.dfs.values():
            self.executor.register_df(df_source)

    def add_db(self, conn: DBConnection, *, name: str | None = None, context: str | Path | None = None) -> None:
        raise NotImplementedError(
            "This method was removed. "
            "Please create a Domain, add a source to it, and initialize the Agent with that Domain."
        )

    def add_df(self, df: DataFrame, *, name: str | None = None, context: str | Path | None = None) -> None:
        raise NotImplementedError(
            "This method was removed. "
            "Please create a Domain, add a source to it, and initialize the Agent with that Domain."
        )

    def add_mcp(
            self,
            config: dict[str, Any] | str | None = None,
            *,
            url: str | None = None,
            command: str | None = None,
            args: list[str] | None = None,
            env: dict[str, str] | None = None,
            headers: dict[str, Any] | None = None,
            transport: str | None = None,
            auth: Any | None = None,
    ) -> None:
        """Connect to one or more MCP servers and register their tools with this agent.

        Can be called with a Claude-Code-style config dict / JSON, or with explicit keyword
        arguments for a single server.

        **Config dict** (Claude Code / Anthropic format)::

              agent.add_mcp({
                  "mcpServers": {
                      "Name": {
                          "command": "npx",
                          "args": ["@command/mcp"],
                          "env": {"API_TOKEN": "..."}
                      }
                  }
              })

        A JSON string or a path to a ``.json`` file is also accepted::

              agent.add_mcp("/path/to/mcp_servers.json")

        **Keyword arguments** (single server)::

              agent.add_mcp(command="python", args=["my_server.py"])
              agent.add_mcp(url="http://localhost:8080/sse")
              agent.add_mcp(url="http://localhost:8080/mcp", transport="streamable_http")
              agent.add_mcp(url="http://example.com/sse", auth="oauth")

        Args:
            config: A config dict, a JSON string, or a path to a JSON file.
                Supports ``{"mcpServers": {name: server_cfg, ...}}``,
                ``{name: server_cfg, ...}``, or a single ``server_cfg`` dict.
                Each ``server_cfg`` contains ``command``/``args``/``env`` (stdio)
                or ``url``/``headers`` (SSE / Streamable HTTP) keys.
            url: Server URL for SSE or Streamable HTTP transport.
            command: Executable for stdio transport.
            args: Command-line arguments for the stdio executable.
            env: Environment variables for the stdio subprocess.
            headers: HTTP headers for SSE / Streamable HTTP transport.
            transport: Explicit transport selection (``"sse"`` or ``"streamable_http"``).
                Inferred automatically when *url* or *command* is provided.
            auth: Authentication for HTTP-based transports (SSE / Streamable HTTP).
                Pass ``"oauth"`` or ``True`` to trigger the default browser-based
                OAuth 2.1 flow (tokens are cached to ``~/.databao/mcp-tokens/``).
                An ``httpx.Auth`` instance can also be passed directly for custom auth.
        """
        if config is not None:
            from databao.mcp.config import parse_mcp_config

            has_kw = any(v is not None for v in (url, command, args, env, headers, transport, auth))
            if has_kw:
                raise ValueError("Cannot combine 'config' with keyword arguments; use one or the other")

            servers = parse_mcp_config(config)
            for server_cfg in servers:
                self._add_mcp_single(
                    url=server_cfg.get("url"),
                    command=server_cfg.get("command"),
                    args=server_cfg.get("args"),
                    env=server_cfg.get("env"),
                    headers=server_cfg.get("headers"),
                    transport=server_cfg.get("transport"),
                    auth=server_cfg.get("auth"),
                )
        else:
            self._add_mcp_single(
                url=url, command=command, args=args, env=env, headers=headers, transport=transport,
                auth=auth,
            )

    def close(self) -> None:
        """Close all MCP connections."""
        for conn in self.__mcp_connections.values():
            conn.close()
        self.__mcp_connections.clear()

    def _add_mcp_single(
            self,
            *,
            url: str | None = None,
            command: str | None = None,
            args: list[str] | None = None,
            env: dict[str, str] | None = None,
            headers: dict[str, Any] | None = None,
            transport: str | None = None,
            auth: Any | None = None,
    ) -> None:
        """Connect to a single MCP server."""
        from databao.mcp.adapter import mcp_tools_to_langchain
        from databao.mcp.connection import McpConnection

        if command is not None and url is not None:
            raise ValueError("Specify either 'command' (stdio) or 'url' (sse/http), not both")
        if command is None and url is None:
            raise ValueError("Specify either 'command' (stdio) or 'url' (sse/http)")

        _VALID_TRANSPORTS = ("sse", "streamable_http")
        if transport is not None and transport not in _VALID_TRANSPORTS:
            raise ValueError(f"Unknown transport {transport!r}; expected one of {_VALID_TRANSPORTS}")

        resolved_auth = self._resolve_auth(auth, url)

        if command is not None:
            if resolved_auth is not None:
                raise ValueError("'auth' is only supported for HTTP-based transports (SSE / Streamable HTTP)")
            connection = McpConnection.connect_stdio(command, args=args, env=env)
        elif transport == "streamable_http":
            if url is None:
                raise ValueError("url must not be None")
            connection = McpConnection.connect_streamable_http(url, headers=headers, auth=resolved_auth)
        else:
            if url is None:
                raise ValueError("url must not be None")
            connection = McpConnection.connect_sse(url, headers=headers, auth=resolved_auth)

        server_name = connection.server_name
        if server_name in self.__mcp_connections:
            logger.warning("MCP server %r registered more than once; replacing previous connection", server_name)
            self.__mcp_connections[server_name].close()
        self.__mcp_connections[server_name] = connection
        lc_tools = mcp_tools_to_langchain(connection)

        existing_names = {t.name for c in self.__mcp_connections.values() if c is not connection for t in c.tools}
        for tool in lc_tools:
            if tool.name in existing_names:
                logger.warning(
                    "MCP tool name collision: '%s' from %s shadows an existing tool",
                    tool.name,
                    connection.server_name,
                )

        self.__executor.register_tools(lc_tools)

        if not lc_tools:
            logger.warning("MCP server %s registered 0 tools", connection.server_name)
        else:
            logger.info(
                "Registered %d MCP tools from %s: %s",
                len(lc_tools),
                connection.server_name,
                [t.name for t in lc_tools],
            )

    @staticmethod
    def _resolve_auth(auth: Any, url: str | None) -> Any:
        """Resolve the *auth* parameter into an ``httpx.Auth`` or ``None``."""
        if auth is None or auth is False:
            return None
        if auth is True or auth == "oauth":
            if url is None:
                raise ValueError("OAuth auth requires a URL-based transport")
            from databao.mcp.oauth import create_oauth_provider

            return create_oauth_provider(url)
        import httpx

        if isinstance(auth, httpx.Auth):
            return auth
        raise TypeError(f"'auth' must be True, 'oauth', or an httpx.Auth instance, got {type(auth).__name__}")

    def thread(
            self,
            *,
            stream_ask: bool | None = None,
            stream_plot: bool | None = None,
            lazy: bool | None = None,
            auto_output_modality: bool | None = None,
            cache_scope: str | None = None,
            writer: TextIO | None = None,
    ) -> Thread:
        """Start a new thread in this agent."""
        return Thread(
            self,
            rows_limit=self.__rows_limit,
            stream_ask=stream_ask if stream_ask is not None else self.__stream_ask,
            stream_plot=stream_plot if stream_plot is not None else self.__stream_plot,
            lazy=lazy if lazy is not None else self.__lazy_threads,
            auto_output_modality=auto_output_modality
            if auto_output_modality is not None
            else self.__auto_output_modality,
            cache_scope=cache_scope,
            writer=writer,
        )

    @property
    def domain(self) -> Domain:
        return self.__domain

    @property
    def sources(self) -> Sources:
        return self.__domain.sources

    @property
    def dbs(self) -> dict[str, DBDataSource]:
        return dict(self.sources.dbs)

    @property
    def dfs(self) -> dict[str, DFDataSource]:
        return dict(self.sources.dfs)

    @property
    def name(self) -> str:
        return self.__name

    @property
    def llm(self) -> BaseChatModel:
        return self.__llm

    @property
    def llm_config(self) -> "LLMConfig":
        return self.__llm_config

    @property
    def agent_config(self) -> "AgentConfig":
        return self.__agent_config

    @property
    def executor(self) -> "Executor":
        return self.__executor

    @property
    def visualizer(self) -> "Visualizer":
        return self.__visualizer

    @property
    def cache(self) -> "Cache":
        return self.__cache

    @property
    def additional_context(self) -> list[str]:
        """General additional context not specific to any one data source."""
        return self.sources.additional_context

    @property
    def mcp_servers(self) -> list[str]:
        """Return names of connected MCP servers."""
        return list(self.__mcp_connections)
