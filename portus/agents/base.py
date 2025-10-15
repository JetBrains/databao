import pickle
from abc import abstractmethod
from io import BytesIO
from typing import Any

from langchain_core.messages import HumanMessage

from portus.configs.llm import LLMConfig
from portus.core import Executor, Opa, Session

try:
    from duckdb import DuckDBPyConnection
except ImportError:
    DuckDBPyConnection = Any  # type: ignore


class AgentExecutor(Executor):
    """
    Base class for agents that execute with a DuckDB connection and LLM configuration.
    Provides common functionality for graph caching, message handling, and OPA processing.
    """

    def __init__(self) -> None:
        """Initialize agent with graph caching infrastructure."""
        self._cached_compiled_graph: Any | None = None
        self._cached_connection_id: int | None = None
        self._cached_llm_config_id: int | None = None

    def _get_data_connection(self, session: Session) -> Any:
        """Get DuckDB connection from session."""
        from duckdb import DuckDBPyConnection

        dbs = session.dbs
        if not dbs:
            raise RuntimeError("No database connection available. Add a database to the session using session.add_db()")

        # Filter for DuckDB connections only
        duckdb_connections = [conn for conn in dbs.values() if isinstance(conn, DuckDBPyConnection)]

        if not duckdb_connections:
            raise RuntimeError(
                "No DuckDB connection found. LighthouseAgent requires a DuckDB connection. "
                "Use portus.duckdb.register_sqlalchemy() or similar to attach external databases to DuckDB."
            )

        # Use the first DuckDB connection
        return duckdb_connections[0]

    def _get_llm_config(self, session: Session) -> LLMConfig:
        """Get LLM config from session."""
        return session.llm_config

    def _get_messages(self, session: Session, cache_scope: str) -> list[Any]:
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
    def _create_graph(self, data_connection: Any, llm_config: LLMConfig) -> Any:
        """
        Create and compile the agent graph.

        Subclasses must implement this method to return their specific graph implementation.

        Args:
            data_connection: DuckDB connection
            llm_config: LLM configuration

        Returns:
            Compiled graph ready for execution
        """
        pass

    def _should_recompile_graph(self, connection_id: int, llm_config_id: int) -> bool:
        """Check if graph needs recompilation due to connection or config changes."""
        return (
            self._cached_compiled_graph is None
            or self._cached_connection_id != connection_id
            or self._cached_llm_config_id != llm_config_id
        )

    def _cache_graph(self, compiled_graph: Any, connection_id: int, llm_config_id: int) -> None:
        """Cache the compiled graph and associated IDs."""
        self._cached_compiled_graph = compiled_graph
        self._cached_connection_id = connection_id
        self._cached_llm_config_id = llm_config_id

    def _get_or_create_cached_graph(self, session: Session) -> tuple[Any, Any]:
        """
        Get cached graph or create new one if connection/config changed.

        Returns:
            Tuple of (data_connection, compiled_graph)
        """
        data_connection = self._get_data_connection(session)
        llm_config = self._get_llm_config(session)

        connection_id = id(data_connection)
        llm_config_id = id(llm_config)

        if self._should_recompile_graph(connection_id, llm_config_id):
            compiled_graph = self._create_graph(data_connection, llm_config)
            self._cache_graph(compiled_graph, connection_id, llm_config_id)

        return data_connection, self._cached_compiled_graph

    def _process_opa(self, session: Session, opa: Opa, cache_scope: str) -> list[Any]:
        """
        Process a single opa and convert it to a message, appending to message history.

        Returns:
            All messages including the new one
        """
        messages = self._get_messages(session, cache_scope)
        messages.append(HumanMessage(content=opa.query))
        return messages

    def _update_message_history(self, session: Session, cache_scope: str, final_messages: list[Any]) -> None:
        """Update message history in cache with final messages from graph execution."""
        if final_messages:
            self._set_messages(session, cache_scope, final_messages)
