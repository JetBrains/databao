import logging
from typing import Any

import duckdb
import pandas as pd
from langchain_core.runnables import RunnableConfig
from langgraph.graph.state import CompiledStateGraph
from sqlalchemy import Engine

from databao.configs.llm import LLMConfig
from databao.core import Agent, ExecutionResult, Opa
from databao.core.executor import OutputModalityHints
from databao.duckdb import register_sqlalchemy
from databao.duckdb.react_tools import AgentResponse, execute_duckdb_sql, make_react_duckdb_agent
from databao.duckdb.utils import get_db_path
from databao.executors.base import GraphExecutor

logger = logging.getLogger(__name__)


class ReactDuckDBExecutor(GraphExecutor):
    def __init__(self) -> None:
        """Initialize agent with lazy graph compilation."""
        super().__init__()
        self._duckdb_connection = duckdb.connect(":memory:")
        self._compiled_graph: CompiledStateGraph[Any] | None = None

    def _create_graph(self, data_connection: Any, llm_config: LLMConfig) -> CompiledStateGraph[Any]:
        """Create and compile the ReAct DuckDB agent graph."""
        return make_react_duckdb_agent(data_connection, llm_config.chat_model)

    def register_db(self, name: str, connection: Any) -> None:
        """Register DB in the DuckDB connection."""

        if isinstance(connection, duckdb.DuckDBPyConnection):
            path = get_db_path(connection)
            if path is not None:
                connection.close()
                self._duckdb_connection.execute(f"ATTACH '{path}' AS {name}")
            else:
                raise RuntimeError("Memory-based DuckDB is not supported.")
        elif isinstance(connection, Engine):
            register_sqlalchemy(self._duckdb_connection, connection, name)
        else:
            raise ValueError("Only DuckDB or SQLAlchemy connections are supported.")

    def register_df(self, name: str, df: pd.DataFrame) -> None:
        self._duckdb_connection.register(name, df)

    def execute(
        self,
        session: Agent,
        opa: Opa,
        *,
        rows_limit: int = 100,
        cache_scope: str = "common_cache",
        stream: bool = True,
    ) -> ExecutionResult:
        # Get or create graph (cached after first use)
        compiled_graph = self._compiled_graph or self._create_graph(self._duckdb_connection, session.llm_config)

        # Process the opa and get messages
        messages = self._process_opa(session, opa, cache_scope)

        # Execute the graph
        init_state = {"messages": messages}
        invoke_config = RunnableConfig(recursion_limit=self._graph_recursion_limit)
        last_state = self._invoke_graph_sync(compiled_graph, init_state, config=invoke_config, stream=stream)
        answer: AgentResponse = last_state["structured_response"]
        logger.info("Generated query: %s", answer.sql)
        df = execute_duckdb_sql(answer.sql, self._duckdb_connection, limit=rows_limit)

        # Update message history
        final_messages = last_state.get("messages", [])
        self._update_message_history(session, cache_scope, final_messages)

        execution_result = ExecutionResult(text=answer.explanation, code=answer.sql, df=df, meta={})

        # Set modality hints
        execution_result.meta[OutputModalityHints.META_KEY] = self._make_output_modality_hints(execution_result)

        return execution_result
