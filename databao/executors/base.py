from abc import ABC, abstractmethod
from typing import Any, TextIO

import duckdb
from langchain_core.messages import HumanMessage
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import BaseTool
from langgraph.graph.state import CompiledStateGraph

from databao.configs.agent import AgentConfig
from databao.configs.llm import LLMConfig
from databao.core import Cache
from databao.core.data_source import DBDataSource, DBTDataSource, DFDataSource
from databao.core.domain import Domain
from databao.core.executor import ExecutionResult, Executor, OutputModalityHints
from databao.core.opa import Opa
from databao.databases import register_db_in_duckdb
from databao.executors.frontend.text_frontend import TextStreamFrontend


class GraphExecutor(Executor, ABC):
    """
    Base class for LangGraph executors that execute with a DuckDB connection and LLM configuration.
    Provides common functionality for graph caching, message handling, and OPA processing.
    """

    def __init__(self, writer: TextIO | None = None) -> None:
        """Initialize agent with graph caching infrastructure.

        Args:
            writer: Optional TextIO for streaming output. If provided, streaming
                    output will be written to this writer instead of stdout.
        """
        self._graph_recursion_limit = 50
        self._writer = writer
        self._duckdb_connection: duckdb.DuckDBPyConnection = duckdb.connect(":memory:")
        self._registered_dbs: dict[str, DBDataSource] = {}
        self._registered_dfs: dict[str, DFDataSource] = {}
        self._registered_dbts: dict[str, DBTDataSource] = {}
        self._extra_tools: dict[str, BaseTool] = {}
        self._compiled_graph: CompiledStateGraph[Any] | None = None
        self._compiled_tools_version: int = 0
        self._compiled_at_version: int = -1

    def register_db(self, source: DBDataSource) -> None:
        """Register a database source into the shared DuckDB connection."""
        register_db_in_duckdb(self._duckdb_connection, source.config, source.name)
        self._registered_dbs[source.name] = source

    def register_df(self, source: DFDataSource) -> None:
        """Register a DataFrame source into the shared DuckDB connection."""
        self._duckdb_connection.register(source.name, source.df)
        self._registered_dfs[source.name] = source

    def register_dbt(self, source: DBTDataSource) -> None:
        self._registered_dbts[source.name] = source

    def register_tools(self, tools: list[BaseTool]) -> None:
        """Register additional LangChain tools and invalidate the cached compiled graph."""
        for t in tools:
            self._extra_tools[t.name] = t
        self._compiled_tools_version += 1

    @abstractmethod
    def _compile_graph(
        self,
        llm_config: LLMConfig,
        agent_config: AgentConfig,
        domain: Domain,
        extra_tools: list[BaseTool] | None,
    ) -> CompiledStateGraph[Any]:
        """Build and return a fresh compiled graph. Called by _get_compiled_graph when needed."""

    def _get_compiled_graph(
        self, llm_config: LLMConfig, agent_config: AgentConfig, domain: Domain
    ) -> CompiledStateGraph[Any]:
        """Return a cached compiled graph, recompiling when extra tools have changed."""
        if self._compiled_graph is None or self._compiled_at_version != self._compiled_tools_version:
            extra = list(self._extra_tools.values()) or None
            self._compiled_graph = self._compile_graph(llm_config, agent_config, domain, extra)
            self._compiled_at_version = self._compiled_tools_version
        return self._compiled_graph

    def _process_opas(self, opas: list[Opa], cache: Cache) -> list[Any]:
        """
        Process a single opa and convert it to a message, appending to message history.

        Returns:
            All messages including the new one
        """
        messages: list[Any] = cache.get("state", {}).get("messages", [])
        query = "\n\n".join(opa.query for opa in opas)
        messages.append(HumanMessage(content=query))
        return messages

    def _update_message_history(self, cache: Cache, final_messages: list[Any]) -> None:
        """Update message history in cache with final messages from graph execution."""
        if final_messages:
            cache.put("state", {"messages": final_messages})

    def _make_output_modality_hints(self, result: ExecutionResult) -> OutputModalityHints:
        # A separate LLM module could be used to fill out the hints
        vis_prompt = result.meta.get("visualization_prompt", None)
        if vis_prompt is not None and len(vis_prompt) == 0:
            vis_prompt = None
        df = result.df
        should_visualize = vis_prompt is not None and df is not None and len(df) >= 3
        return OutputModalityHints(visualization_prompt=vis_prompt, should_visualize=should_visualize)

    @classmethod
    def _executor_tag(cls) -> str:
        """Derive a short tag from the class name, e.g. 'LighthouseExecutor' → 'lighthouse'."""
        name = cls.__name__
        if name.endswith("Executor"):
            name = name[: -len("Executor")]
        # CamelCase → kebab-case: "DbtProject" → "dbt-project"
        import re

        return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "-", name).lower()

    def _build_invoke_config(self, agent_config: AgentConfig, opas: list[Opa]) -> RunnableConfig:
        """Build a RunnableConfig with automatic executor tagging and user-provided metadata."""
        opa = opas[-1] if opas else None
        executor_tag = self._executor_tag()

        metadata: dict[str, Any] = {"executor": executor_tag}
        if opa:
            metadata["question"] = opa.query
            if opa.metadata:
                metadata.update(opa.metadata)

        tags = [executor_tag]
        if opa and opa.tags:
            tags.extend(opa.tags)

        return RunnableConfig(
            recursion_limit=max(self._graph_recursion_limit, agent_config.recursion_limit),
            metadata=metadata,
            tags=tags,
        )

    @staticmethod
    def _invoke_graph_sync(
        compiled_graph: CompiledStateGraph[Any],
        start_state: Any,
        *,
        config: RunnableConfig | None = None,
        stream: bool = True,
        writer: TextIO | None = None,
        **kwargs: Any,
    ) -> Any:
        """Invoke the graph with the given start state and return the output state."""
        if stream:
            return GraphExecutor._execute_stream_sync(
                compiled_graph, start_state, config=config, writer=writer, **kwargs
            )
        else:
            return compiled_graph.invoke(start_state, config=config)

    @staticmethod
    async def _execute_stream(
        compiled_graph: CompiledStateGraph[Any],
        start_state: Any,
        *,
        config: RunnableConfig | None = None,
        writer: TextIO | None = None,
        **kwargs: Any,
    ) -> Any:
        frontend = TextStreamFrontend(start_state, writer=writer)
        last_state = None
        async for mode, chunk in compiled_graph.astream(
            start_state,
            stream_mode=["values", "messages"],
            config=config,
            **kwargs,
        ):
            frontend.write_stream_chunk(mode, chunk)
            if mode == "values":
                last_state = chunk
        frontend.end()
        if last_state is None:
            raise RuntimeError("Graph execution produced no output state")
        return last_state

    @staticmethod
    def _execute_stream_sync(
        compiled_graph: CompiledStateGraph[Any],
        start_state: Any,
        *,
        config: RunnableConfig | None = None,
        writer: TextIO | None = None,
        **kwargs: Any,
    ) -> Any:
        frontend = TextStreamFrontend(start_state, writer=writer)
        last_state = None
        for mode, chunk in compiled_graph.stream(
            start_state,
            stream_mode=["values", "messages"],
            config=config,
            **kwargs,
        ):
            frontend.write_stream_chunk(mode, chunk)
            if mode == "values":
                last_state = chunk
        frontend.end()
        if last_state is None:
            raise RuntimeError("Graph execution produced no output state")
        return last_state
