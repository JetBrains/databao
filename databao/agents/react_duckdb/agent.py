import logging
from typing import Any

from duckdb import DuckDBPyConnection
from langchain_core.runnables import RunnableConfig
from langgraph.graph.state import CompiledStateGraph

from databao.agents.base import AgentExecutor
from databao.agents.react_duckdb.react_tools import AgentResponse, make_react_duckdb_agent, sql_strip
from databao.core import ExecutionResult, Opa, Session

logger = logging.getLogger(__name__)


class ReactDuckDBAgent(AgentExecutor):
    def __init__(self) -> None:
        super().__init__()
        self._session_connections: dict[str, DuckDBPyConnection] = {}

    def _create_connection(self, session: Session) -> DuckDBPyConnection:
        """Get or create a connection to the DuckDB database."""
        # Use a native duckdb connection for backwards compatibility.
        connection = self._duckdb_collection.make_duckdb_connection()
        self._session_connections[session.name] = connection
        return connection

    def _create_graph(self, session: Session) -> CompiledStateGraph[Any]:
        """Create and compile the ReAct DuckDB agent graph."""
        connection = self._create_connection(session)
        chat_model = session.llm_config.chat_model
        return make_react_duckdb_agent(connection, chat_model)

    def execute(
        self,
        session: Session,
        opa: Opa,
        *,
        rows_limit: int = 100,
        cache_scope: str = "common_cache",
        stream: bool = True,
    ) -> ExecutionResult:
        # Get or create graph (cached after first use)
        compiled_graph = self._get_or_create_cached_graph(session)
        connection = self._session_connections[session.name]

        # Process the opa and get messages
        messages = self._process_opa(session, opa, cache_scope)

        # Execute the graph
        init_state = {"messages": messages}
        invoke_config = RunnableConfig(recursion_limit=self._graph_recursion_limit)
        last_state = self._invoke_graph(compiled_graph, init_state, config=invoke_config, stream=stream)
        answer: AgentResponse = last_state["structured_response"]
        logger.info("Generated query: %s", answer.sql)
        df = connection.execute(f"SELECT * FROM ({sql_strip(answer.sql)}) t LIMIT {rows_limit}").df()

        # Update message history
        final_messages = last_state.get("messages", [])
        self._update_message_history(session, cache_scope, final_messages)

        return ExecutionResult(text=answer.explanation, code=answer.sql, df=df, meta={})
