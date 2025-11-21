from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
from langchain_core.messages import SystemMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph.state import CompiledStateGraph
from sqlalchemy import Connection, Engine

from databao.core import Agent, ExecutionResult, Opa
from databao.core.executor import OutputModalityHints
from databao.duckdb.utils import describe_duckdb_schema, get_db_path, register_sqlalchemy
from databao.executors.base import GraphExecutor
from databao.executors.lighthouse.graph import ExecuteSubmit
from databao.executors.lighthouse.history_cleaning import clean_tool_history
from databao.executors.lighthouse.utils import get_today_date_str, read_prompt_template


class LighthouseExecutor(GraphExecutor):
    def __init__(self) -> None:
        super().__init__()
        self._prompt_template = read_prompt_template(Path("system_prompt.jinja"))

        # Create a DuckDB connection for the agent
        self._duckdb_connection = duckdb.connect(":memory:")
        self._graph: ExecuteSubmit = ExecuteSubmit(self._duckdb_connection)
        self._compiled_graph: CompiledStateGraph[Any] | None = None

    def render_system_prompt(self, data_connection: Any, agent: Agent) -> str:
        """Render system prompt with database schema."""
        db_schema = describe_duckdb_schema(data_connection)

        context = ""
        for db_name, db_context in agent.db_context.items():
            context += f"## Context for DB {db_name}\n\n{db_context}\n\n"
        for df_name, df_context in agent.df_context.items():
            context += f"## Context for DF {df_name} (fully qualified name 'temp.main.{df_name}')\n\n{df_context}\n\n"
        for idx, additional_ctx in enumerate(agent.additional_context, start=1):
            additional_context = additional_ctx.strip()
            context += f"## General information {idx}\n\n{additional_context}\n\n"
        context = context.strip()

        prompt = self._prompt_template.render(
            date=get_today_date_str(), db_schema=db_schema, context=context, tool_limit=self._graph_recursion_limit // 2
        )

        return prompt.strip()

    def register_db(self, name: str, connection: duckdb.DuckDBPyConnection | Connection | Engine) -> None:
        """Register DB in the DuckDB connection."""
        if isinstance(connection, Connection):
            connection = connection.engine

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

    def _get_compiled_graph(self, agent: Agent) -> CompiledStateGraph[Any]:
        """Get compiled graph."""
        compiled_graph = self._compiled_graph or self._graph.compile(agent.llm_config)
        self._compiled_graph = compiled_graph

        return compiled_graph

    def execute(
        self,
        agent: Agent,
        opa: Opa,
        *,
        rows_limit: int = 100,
        cache_scope: str = "common_cache",
        stream: bool = True,
    ) -> ExecutionResult:
        compiled_graph = self._get_compiled_graph(agent)

        messages = self._process_opa(agent, opa, cache_scope)

        # Prepend system message if not present
        all_messages_with_system = messages
        if not all_messages_with_system or all_messages_with_system[0].type != "system":
            all_messages_with_system = [
                SystemMessage(self.render_system_prompt(self._duckdb_connection, agent)),
                *all_messages_with_system,
            ]
        cleaned_messages = clean_tool_history(all_messages_with_system, agent.llm_config.max_tokens_before_cleaning)

        init_state = self._graph.init_state(cleaned_messages, limit_max_rows=rows_limit)
        invoke_config = RunnableConfig(recursion_limit=self._graph_recursion_limit)
        last_state = self._invoke_graph_sync(compiled_graph, init_state, config=invoke_config, stream=stream)
        execution_result = self._graph.get_result(last_state)

        # Update message history (excluding system message which we add dynamically)
        final_messages = last_state.get("messages", [])
        if final_messages:
            new_messages = final_messages[len(cleaned_messages) :]
            all_messages = all_messages_with_system + new_messages
            all_messages_without_system = [msg for msg in all_messages if msg.type != "system"]
            if execution_result.meta.get("messages"):
                execution_result.meta["messages"] = all_messages
            self._update_message_history(agent, cache_scope, all_messages_without_system)

        # Set modality hints
        execution_result.meta[OutputModalityHints.META_KEY] = self._make_output_modality_hints(execution_result)

        return execution_result
