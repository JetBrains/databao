import logging
from typing import Any

from langchain_core.runnables import RunnableConfig

from databao.agents.base import AgentExecutor
from databao.configs.llm import LLMConfig
from databao.core import ExecutionResult, Opa, Session
from databao.core.executor import OutputModalityHints
from databao.duckdb.react_tools import AgentResponse, make_react_duckdb_agent, sql_strip

logger = logging.getLogger(__name__)


class ReactDuckDBAgent(AgentExecutor):
    def _create_graph(self, data_connection: Any, llm_config: LLMConfig) -> Any:
        """Create and compile the ReAct DuckDB agent graph."""
        return make_react_duckdb_agent(data_connection, llm_config.chat_model)

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
        data_connection, compiled_graph = self._get_or_create_cached_graph(session)

        # Process the opa and get messages
        messages = self._process_opa(session, opa, cache_scope)

        # Execute the graph
        init_state = {"messages": messages}
        invoke_config = RunnableConfig(recursion_limit=self._graph_recursion_limit)
        last_state = self._invoke_graph_sync(compiled_graph, init_state, config=invoke_config, stream=stream)
        answer: AgentResponse = last_state["structured_response"]
        logger.info("Generated query: %s", answer.sql)
        df = data_connection.execute(f"SELECT * FROM ({sql_strip(answer.sql)}) t LIMIT {rows_limit}").df()

        # Update message history
        final_messages = last_state.get("messages", [])
        self._update_message_history(session, cache_scope, final_messages)

        execution_result = ExecutionResult(text=answer.explanation, code=answer.sql, df=df, meta={})

        # Set modality hints
        execution_result.meta[OutputModalityHints.META_KEY] = self._make_output_modality_hints(execution_result)

        return execution_result
