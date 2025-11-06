import pickle
from abc import abstractmethod
from io import BytesIO
from typing import Any

from langchain_core.messages import BaseMessage, HumanMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph.state import CompiledStateGraph

from databao.agents.frontend.text_frontend import TextStreamFrontend
from databao.configs.llm import LLMConfig
from databao.core import Executor, Opa, Session
from databao.data.configs.schema_inspection_config import InspectionOptions, SchemaInspectionConfig
from databao.data.database_schema_types import DatabaseSchema
from databao.data.duckdb.duckdb_collection import DuckDBCollection
from databao.data.schema_summary import summarize_schema


class AgentExecutor(Executor):
    """
    Base class for LangGraph agents that execute with a DuckDB connection and LLM configuration.
    Provides common functionality for graph caching, message handling, and OPA processing.
    """

    def __init__(self) -> None:
        """Initialize agent with graph caching infrastructure."""
        # TODO Caching should be scoped to the Session/Pipe/Thread, not the Executor instance
        # For now assume the Executor will not be reused across sessions. Otherwise we would need per-session instances.
        self._compiled_graph: CompiledStateGraph[Any] | None = None
        self._llm_config: LLMConfig | None = None
        # For now assume this Executor is only used with DuckDB compatible data sources.
        self._duckdb_collection = DuckDBCollection()

        # Cache schema inspection results
        self._inspected_schema: DatabaseSchema | None = None
        self._inspected_schema_options: InspectionOptions | None = None

        self._graph_recursion_limit = 50

    def _update_data_connections(self, session: Session) -> bool:
        # TODO The interaction and responsibilities of Session and Executor need to be redesigned.
        # The Executor is responsible for connecting to the available data sources.

        # Currently, we assume connections can only be added to a session and not removed.
        # Otherwise, we would need to invalidate removed connections as well.
        existing_db_names = {db_source.name for db_source in self._duckdb_collection.db_sources}
        session_db_names = {name for name in session.dbs}
        new_db_names = session_db_names.difference(existing_db_names)
        for name in new_db_names:
            engine = session.dbs[name]
            additional_context = session.db_contexts.get(name)
            self._duckdb_collection.add_db(engine, name=name, additional_context=additional_context)

        # Same as above for DataFrames
        existing_df_names = {df_source.name for df_source in self._duckdb_collection.df_sources}
        session_df_names = {name for name in session.dfs}
        new_df_names = session_df_names.difference(existing_df_names)
        for name in new_df_names:
            engine = session.dfs[name]
            additional_context = session.df_contexts.get(name)
            self._duckdb_collection.add_df(engine, name=name, additional_context=additional_context)

        return self._duckdb_collection.register_data_sources()

    def _summarize_schema(self, inspection_config: SchemaInspectionConfig) -> str:
        # We can cache the schema inspection since we assume the data connection won't change
        inspection_options = inspection_config.inspection_options
        if self._inspected_schema is not None and self._inspected_schema_options == inspection_options:
            return summarize_schema(self._inspected_schema, inspection_config.summary_type)
        self._inspected_schema_options = inspection_options
        self._inspected_schema = self._duckdb_collection.inspect_schema_sync(inspection_options)
        return summarize_schema(self._inspected_schema, inspection_config.summary_type)

    def _get_messages(self, session: Session, cache_scope: str) -> list[BaseMessage]:
        """Retrieve messages from the session cache."""
        try:
            buffer = BytesIO()
            session.cache.scoped(cache_scope).get("messages", buffer)
            buffer.seek(0)
            result: list[Any] = pickle.load(buffer)
            return result
        except (KeyError, EOFError):
            return []

    def _set_messages(self, session: Session, cache_scope: str, messages: list[Any]) -> None:
        """Store messages in the session cache."""
        buffer = BytesIO()
        pickle.dump(messages, buffer)
        buffer.seek(0)
        session.cache.scoped(cache_scope).put("messages", buffer)

    @abstractmethod
    def _create_graph(self, session: Session) -> CompiledStateGraph[Any]:
        """
        Create and compile the agent graph.

        Subclasses must implement this method to return their specific graph implementation.

        Returns:
            Compiled graph ready for execution
        """
        pass

    def _get_or_create_cached_graph(self, session: Session) -> CompiledStateGraph[Any]:
        """Get cached graph or create new one if connection/config changed."""
        connections_updated = self._update_data_connections(session)
        should_recompile_graph = (
            self._compiled_graph is None or connections_updated or self._llm_config != session.llm_config
        )
        if self._compiled_graph is not None and not should_recompile_graph:
            return self._compiled_graph

        compiled_graph = self._create_graph(session)
        self._compiled_graph = compiled_graph
        self._llm_config = session.llm_config
        return compiled_graph

    def _process_opa(self, session: Session, opa: Opa, cache_scope: str) -> list[BaseMessage]:
        """
        Process a single opa and convert it to a message, appending to message history.

        Returns:
            All messages including the new one
        """
        messages = self._get_messages(session, cache_scope)
        messages.append(HumanMessage(content=opa.query))
        return messages

    def _update_message_history(self, session: Session, cache_scope: str, final_messages: list[BaseMessage]) -> None:
        """Update message history in cache with final messages from graph execution."""
        if final_messages:
            self._set_messages(session, cache_scope, final_messages)

    def _invoke_graph(
        self,
        compiled_graph: CompiledStateGraph[Any],
        start_state: dict[str, Any],
        *,
        config: RunnableConfig | None = None,
        stream: bool = True,
        **kwargs: Any,
    ) -> Any:
        """Invoke the graph with the given start state and return the output state."""
        if stream:
            return self._execute_stream_sync(compiled_graph, start_state, config=config, **kwargs)
        else:
            return compiled_graph.invoke(start_state, config=config)

    @staticmethod
    async def _execute_stream(
        compiled_graph: CompiledStateGraph[Any],
        start_state: dict[str, Any],
        *,
        config: RunnableConfig | None = None,
        **kwargs: Any,
    ) -> Any:
        writer = TextStreamFrontend(start_state)
        last_state = None
        async for mode, chunk in compiled_graph.astream(
            start_state,
            stream_mode=["values", "messages"],
            config=config,
            **kwargs,
        ):
            writer.write_stream_chunk(mode, chunk)
            if mode == "values":
                last_state = chunk
        writer.end()
        assert last_state is not None
        return last_state

    @staticmethod
    def _execute_stream_sync(
        compiled_graph: CompiledStateGraph[Any],
        start_state: dict[str, Any],
        *,
        config: RunnableConfig | None = None,
        **kwargs: Any,
    ) -> Any:
        writer = TextStreamFrontend(start_state)
        last_state = None
        for mode, chunk in compiled_graph.stream(
            start_state,
            stream_mode=["values", "messages"],
            config=config,
            **kwargs,
        ):
            writer.write_stream_chunk(mode, chunk)
            if mode == "values":
                last_state = chunk
        writer.end()
        assert last_state is not None
        return last_state
