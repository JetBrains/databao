from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, TextIO

import duckdb
from databao_context_engine import DuckDBConnectionConfig
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
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
from databao.executors.history_cleaning import clean_tool_history


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

    def prepare_for_execution(self, domain: "Domain") -> None:
        if not domain.supports_context or domain.is_context_built():
            return

        # DuckDB does not allow two connections to hold a file open simultaneously.
        # Temporarily detach file-based DuckDB sources so the context engine can attach them.
        duckdb_file_sources = {
            name: source
            for name, source in self._registered_dbs.items()
            if isinstance(source.config, DuckDBConnectionConfig)
        }
        for name in duckdb_file_sources:
            self._duckdb_connection.execute(f'DETACH "{name}"')

        try:
            super().prepare_for_execution(domain)
        finally:
            for name, source in duckdb_file_sources.items():
                register_db_in_duckdb(self._duckdb_connection, source.config, name)

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

    def drop_last_opa_group(self, cache: Cache, n: int = 1) -> None:
        """Drop last n groups of operations from the message history."""
        messages = cache.get("state", default={}).get("messages", [])
        human_messages = [m for m in messages if isinstance(m, HumanMessage)]
        if len(human_messages) < n:
            raise ValueError(f"Cannot drop last {n} operations - only {len(human_messages)} operations found.")
        c = 0
        while c < n:
            m = messages.pop()
            if isinstance(m, HumanMessage):
                c += 1

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

    def _execute_core(
        self,
        opas: list[Opa],
        cache: Cache,
        llm_config: LLMConfig,
        agent_config: AgentConfig,
        domain: Domain,
        *,
        system_prompt: str,
        init_state: Any,
        get_result: Callable[[Any], ExecutionResult],
        extra_preamble: list[Any] | None = None,
        stream: bool = True,
        writer: TextIO | None = None,
    ) -> tuple[ExecutionResult, Any]:
        """Shared execution flow for graph-based executors.

        Args:
            opas: User intents to process.
            cache: Persistent cache for message history.
            llm_config: LLM configuration.
            agent_config: Agent configuration.
            domain: Domain with data sources.
            system_prompt: Rendered system prompt text.
            init_state: Pre-built initial state for the graph (graph-specific).
            get_result: Callable that extracts an ExecutionResult from the final graph state.
            extra_preamble: Optional extra messages after system message (e.g. task instruction).
            stream: Whether to stream output.
            writer: Optional output writer.

        Returns:
            Tuple of (ExecutionResult from graph, raw last_state for post-processing).
        """
        compiled_graph = self._get_compiled_graph(llm_config, agent_config, domain)
        messages: list[Any] = self._process_opas(opas, cache)

        all_messages_with_system = self._ensure_system_message(messages, system_prompt, extra_preamble)
        cleaned_messages = clean_tool_history(all_messages_with_system, llm_config.max_tokens_before_cleaning)

        # Patch messages into init_state (all graphs store messages under "messages" key)
        if isinstance(init_state, dict):
            init_state["messages"] = cleaned_messages
        else:
            init_state = {**init_state, "messages": cleaned_messages}

        invoke_config = self._build_invoke_config(agent_config, opas)
        last_state = self._invoke_graph_sync(
            compiled_graph, init_state, config=invoke_config, stream=stream, writer=writer or self._writer
        )

        execution_result = get_result(last_state)

        # Reconcile and persist message history
        all_messages_without_system, all_messages = self._reconcile_messages(
            all_messages_with_system, cleaned_messages, last_state
        )
        if all_messages:
            if execution_result.meta.get(ExecutionResult.META_MESSAGES_KEY):
                execution_result.meta[ExecutionResult.META_MESSAGES_KEY] = all_messages
            self._update_message_history(cache, all_messages_without_system)

        # Set modality hints
        execution_result.meta[OutputModalityHints.META_KEY] = self._make_output_modality_hints(execution_result)

        return execution_result, last_state

    @staticmethod
    def _ensure_system_message(
        messages: list[BaseMessage],
        system_content: str,
        extra_preamble: list[BaseMessage] | None = None,
    ) -> list[BaseMessage]:
        """Prepend a SystemMessage (and optional extra preamble) if not already present."""
        if messages and messages[0].type == "system":
            return messages
        preamble: list[BaseMessage] = [SystemMessage(system_content)]
        if extra_preamble:
            preamble.extend(extra_preamble)
        return [*preamble, *messages]

    @staticmethod
    def _reconcile_messages(
        all_messages_with_system: list[BaseMessage],
        cleaned_messages: list[BaseMessage],
        last_state: dict[str, Any],
    ) -> tuple[list[BaseMessage], list[BaseMessage]]:
        """Compute the final message list (without system messages) after graph execution.

        Merges new messages produced by the graph with the original conversation,
        then strips system messages (which are added dynamically per-invocation).
        """
        final_messages = last_state.get("messages", [])
        if not final_messages:
            return [], []
        new_messages = final_messages[len(cleaned_messages) :]
        all_messages = all_messages_with_system + new_messages
        all_messages_without_system = [m for m in all_messages if m.type != "system"]
        return all_messages_without_system, all_messages

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
