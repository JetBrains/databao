import logging
from typing import Any

from duckdb import DuckDBPyConnection
from langgraph.graph.state import CompiledStateGraph

from portus.agents.base import AgentExecutor
from portus.agents.react_duckdb.react_tools import AgentResponse, make_react_duckdb_agent, sql_strip
from portus.core import ExecutionResult, Opa, Session
from portus.data.duckdb.duckdb_collection import DuckDBCollection

logger = logging.getLogger(__name__)


class ReactDuckDBAgent(AgentExecutor):
    def __init__(self) -> None:
        super().__init__()
        self._session_connections: dict[str, DuckDBPyConnection] = {}

    def _create_connection(self, session: Session) -> DuckDBPyConnection:
        """Get or create a connection to the DuckDB database."""
        data_engine = session.data_engine
        duckdb_collection = next((s for s in data_engine.sources.values() if isinstance(s, DuckDBCollection)), None)
        if duckdb_collection is None:
            raise RuntimeError(f"No DuckDBCollection found in the data engine when using {self.__class__.__name__}.")
        # Use a native duckdb connection for backwards compatibility.
        connection = duckdb_collection.make_duckdb_connection()
        self._session_connections[session.name] = connection
        return connection

    def _create_graph(self, session: Session) -> CompiledStateGraph[Any]:
        """Create and compile the ReAct DuckDB agent graph."""
        connection = self._create_connection(session)
        chat_model = session.llm_config.chat_model
        return make_react_duckdb_agent(connection, chat_model)

    def execute(
        self, session: Session, opa: Opa, *, rows_limit: int = 100, cache_scope: str = "common_cache"
    ) -> ExecutionResult:
        # Get or create graph (cached after first use)
        compiled_graph = self._get_or_create_cached_graph(session)
        connection = self._session_connections[session.name]

        # Process the opa and get messages
        messages = self._process_opa(session, opa, cache_scope)

        # Execute the graph
        state = compiled_graph.invoke({"messages": messages})
        answer: AgentResponse = state["structured_response"]
        logger.info("Generated query: %s", answer.sql)
        df = connection.execute(f"SELECT * FROM ({sql_strip(answer.sql)}) t LIMIT {rows_limit}").df()

        # Update message history
        final_messages = state.get("messages", [])
        self._update_message_history(session, cache_scope, final_messages)

        return ExecutionResult(text=answer.explanation, code=answer.sql, df=df, meta={})
