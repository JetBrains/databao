import logging
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Any, TextIO

from langchain_core.language_models.chat_models import BaseChatModel
from pandas import DataFrame
from typing_extensions import deprecated

from databao.agent.core.data_source import DBDataSource, DFDataSource, Sources
from databao.agent.core.domain import Domain, _Domain
from databao.agent.core.thread import Thread
from databao.agent.databases import DBConnection
from databao.agent.mcp.manager import McpManager

if TYPE_CHECKING:
    from databao.agent.configs.agent import AgentConfig
    from databao.agent.configs.llm import LLMConfig
    from databao.agent.core.cache import Cache
    from databao.agent.core.executor import Executor
    from databao.agent.core.visualizer import Visualizer

logger = logging.getLogger(__name__)


class Agent:
    """
    An agent executes requests against a domain of data sources.
    It determines what LLM to use, what executor to use and how to visualize data for all threads.
    Several threads can be spawned out of the agent.
    """

    def __init__(
        self,
        domain: "Domain",
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
        if not isinstance(domain, _Domain):
            raise ValueError("Custom domains are not supported yet.")
        self.__domain = domain

        self.__name = name
        self.__llm = llm.new_chat_model()
        self.__llm_config = llm
        self.__agent_config = agent_config

        self.__executor: Executor = data_executor
        self.__visualizer = visualizer
        self.__cache = cache
        self.__mcp: McpManager = McpManager()

        # Thread defaults
        self.__rows_limit = rows_limit
        self.__lazy_threads = lazy_threads
        self.__auto_output_modality = auto_output_modality
        self.__stream_ask = stream_ask
        self.__stream_plot = stream_plot

    @deprecated("Use Domain.add_db() and initialize Agent with Domain instead.")
    def add_db(self, conn: DBConnection, *, name: str | None = None, context: str | Path | None = None) -> None:
        raise NotImplementedError(
            "This method was removed. Use Domain.add_db() and initialize Agent with Domain instead."
        )

    @deprecated("Use Domain.add_df() and initialize Agent with Domain instead.")
    def add_df(self, df: DataFrame, *, name: str | None = None, context: str | Path | None = None) -> None:
        raise NotImplementedError(
            "This method was removed. Use Domain.add_df() and initialize Agent with Domain instead."
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

        .. warning::
            This feature is **experimental** and may change in future releases.

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
        warnings.warn(
            "add_mcp() is an experimental feature and may change in future releases.",
            stacklevel=2,
        )
        if config is not None:
            has_kw = any(v is not None for v in (url, command, args, env, headers, transport, auth))
            if has_kw:
                raise ValueError("Cannot combine 'config' with keyword arguments; use one or the other")
            lc_tools = self.__mcp.connect_from_config(config)
        else:
            lc_tools = self.__mcp.connect(
                url=url,
                command=command,
                args=args,
                env=env,
                headers=headers,
                transport=transport,
                auth=auth,
            )
        self.__executor.register_tools(lc_tools)

    def close(self) -> None:
        """Close all MCP connections."""
        self.__mcp.close()

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
    def additional_description(self) -> list[str]:
        """General additional information not specific to any one data source."""
        return self.sources.additional_description

    @property
    def mcp_servers(self) -> list[str]:
        """Return names of connected MCP servers."""
        return self.__mcp.servers
