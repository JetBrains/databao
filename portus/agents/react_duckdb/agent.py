import logging
from typing import Any

from portus.agents.base import AgentExecutor
from portus.agents.react_duckdb.react_tools import AgentResponse, make_react_duckdb_agent, sql_strip
from portus.configs.llm import LLMConfig
from portus.core import ExecutionResult, Opa, Session

logger = logging.getLogger(__name__)


class ReactDuckDBAgent(AgentExecutor):
    def _create_graph(self, data_connection: Any, llm_config: LLMConfig) -> Any:
        """Create and compile the ReAct DuckDB agent graph."""
        return make_react_duckdb_agent(data_connection, llm_config.chat_model)

    def execute(
        self, session: Session, opa: Opa, *, rows_limit: int = 100, cache_scope: str = "common_cache"
    ) -> ExecutionResult:
        # Get or create graph (cached after first use)
        data_connection, compiled_graph = self._get_or_create_cached_graph(session)

        # Process the opa and get messages
        messages = self._process_opa(session, opa, cache_scope)

        # Execute the graph
        state = compiled_graph.invoke({"messages": messages})
        answer: AgentResponse = state["structured_response"]
        logger.info("Generated query: %s", answer.sql)
        df = data_connection.execute(f"SELECT * FROM ({sql_strip(answer.sql)}) t LIMIT {rows_limit}").df()

        # Update message history
        final_messages = state.get("messages", [])
        self._update_message_history(session, cache_scope, final_messages)

        return ExecutionResult(text=answer.explanation, code=answer.sql, df=df, meta={})
