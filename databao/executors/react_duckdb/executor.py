import logging
from typing import Any, TextIO

import duckdb
from langchain_core.messages import HumanMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph.state import CompiledStateGraph
from sqlalchemy import Connection, Engine

from databao.configs.agent import AgentConfig
from databao.configs.llm import LLMConfig
from databao.core import Cache, Context, ExecutionResult, Opa
from databao.core.data_source import DBDataSource, DFDataSource
from databao.core.executor import OutputModalityHints
from databao.databases import DBConnectionConfig, register_in_duckdb
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

    def _create_graph(self, data_connection: Any, llm_config: LLMConfig, context: Context) -> CompiledStateGraph[Any]:
        """Create and compile the ReAct DuckDB agent graph."""
        return make_react_duckdb_agent(data_connection, llm_config.new_chat_model(), context)

    def register_db(self, source: DBDataSource) -> None:
        """Register DB in the DuckDB connection."""
        connection = source.db_connection
        if isinstance(connection, Connection):
            connection = connection.engine

        if isinstance(connection, duckdb.DuckDBPyConnection):
            path = get_db_path(connection)
            if path is not None:
                connection.close()
                self._duckdb_connection.execute(f"ATTACH '{path}' AS {source.name}")
            else:
                raise RuntimeError("Memory-based DuckDB is not supported.")
        elif isinstance(connection, Engine):
            register_sqlalchemy(self._duckdb_connection, connection, source.name)
        elif isinstance(connection, DBConnectionConfig):
            register_in_duckdb(self._duckdb_connection, connection, source.name)
        else:
            raise ValueError("Only DuckDB or SQLAlchemy connections are supported.")

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

    def register_df(self, source: DFDataSource) -> None:
        self._duckdb_connection.register(source.name, source.df)

    def execute(
        self,
        opas: list[Opa],
        cache: Cache,
        llm_config: LLMConfig,
        agent_config: AgentConfig,
        context: Context,
        *,
        rows_limit: int = 100,
        stream: bool = True,
        writer: TextIO | None = None,
    ) -> ExecutionResult:
        # Get or create graph (cached after first use)
        compiled_graph = self._compiled_graph or self._create_graph(self._duckdb_connection, llm_config, context)

        # Process the opa and get messages
        messages = self._process_opas(opas, cache)

        # Execute the graph
        init_state = {"messages": messages}
        invoke_config = RunnableConfig(recursion_limit=agent_config.recursion_limit)
        last_state = self._invoke_graph_sync(
            compiled_graph, init_state, config=invoke_config, stream=stream, writer=writer
        )
        answer: AgentResponse = last_state["structured_response"]
        logger.info("Generated query: %s", answer.sql)
        df = execute_duckdb_sql(answer.sql, self._duckdb_connection, limit=rows_limit)

        # Update message history
        final_messages = last_state.get("messages", [])
        self._update_message_history(cache, final_messages)

        execution_result = ExecutionResult(text=answer.explanation, code=answer.sql, df=df, meta={})

        # Set modality hints
        execution_result.meta[OutputModalityHints.META_KEY] = self._make_output_modality_hints(execution_result)

        return execution_result
