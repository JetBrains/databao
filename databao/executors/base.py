import logging
from abc import ABC
from typing import Any, TextIO

import duckdb
from langchain_core.messages import HumanMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph.state import CompiledStateGraph

from databao.core import Cache
from databao.core.data_source import DBDataSource, DFDataSource
from databao.core.executor import ExecutionResult, Executor, OutputModalityHints
from databao.core.opa import Opa
from databao.databases import register_in_duckdb
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
        self._attached_db_paths: dict[str, str] = {}
        self._registered_dfs: dict[str, Any] = {}

    def register_db(self, source: DBDataSource) -> None:
        """Register a database source into the shared DuckDB connection."""
        if not source.connectable:
            logging.getLogger(__name__).debug(
                "Skipping non-connectable datasource '%s'",
                source.name,
            )
            return

        register_in_duckdb(self._duckdb_connection, source.config, source.name)
        db_path = source.config.content.get("database_path")
        if db_path is None:
            db_path = source.config.content.get("connection", {}).get("database_path")
        if db_path is not None:
            self._attached_db_paths[source.name] = db_path

    def register_df(self, source: DFDataSource) -> None:
        """Register a DataFrame source into the shared DuckDB connection."""
        self._registered_dfs[source.name] = source.df
        self._duckdb_connection.register(source.name, source.df)

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
        assert last_state is not None
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
        assert last_state is not None
        return last_state
