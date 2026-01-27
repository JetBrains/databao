from pathlib import Path
from typing import Any

import duckdb
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph.state import CompiledStateGraph
from sqlalchemy import Connection, Engine

from databao.configs import LLMConfig
from databao.configs.agent import AgentConfig
from databao.core import Cache, ExecutionResult, Opa
from databao.core.data_source import DBDataSource, DFDataSource, Sources
from databao.core.executor import OutputModalityHints
from databao.databases import DBConnectionConfig, register_in_duckdb
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

    def render_system_prompt(
        self,
        data_connection: Any,
        sources: Sources,
        recursion_limit: int = 50,
    ) -> str:
        """Render system prompt with database schema."""
        db_schema = describe_duckdb_schema(data_connection)

        context = ""
        for db_name, source in sources.dbs.items():
            if source.context:
                context += f"## Context for DB {db_name}\n\n{source.context}\n\n"
        for df_name, source in sources.dfs.items():
            if source.context:
                context += (
                    f"## Context for DF {df_name} (fully qualified name 'temp.main.{df_name}')\n\n{source.context}\n\n"
                )
        for idx, add_ctx in enumerate(sources.additional_context, start=1):
            context += f"## General information {idx}\n\n{add_ctx.strip()}\n\n"
        context = context.strip()

        prompt = self._prompt_template.render(
            date=get_today_date_str(), db_schema=db_schema, context=context, tool_limit=recursion_limit // 2
        )

        return prompt.strip()

    def register_db(self, source: DBDataSource) -> None:
        """Register DB in the DuckDB connection."""
        connection = source.db_connection
        if isinstance(connection, Connection):
            connection = connection.engine

        if isinstance(connection, duckdb.DuckDBPyConnection):
            path = get_db_path(connection)
            if path is not None:
                connection.close()
                self._duckdb_connection.execute(f"ATTACH '{path}' AS {source.name} (READ_ONLY)")
            else:
                raise RuntimeError("Memory-based DuckDB is not supported.")
        elif isinstance(connection, Engine):
            register_sqlalchemy(self._duckdb_connection, connection, source.name)
        elif isinstance(connection, DBConnectionConfig):
            register_in_duckdb(self._duckdb_connection, connection, source.name)
        else:
            raise ValueError("Only DuckDB or SQLAlchemy connections are supported.")

    def register_df(self, source: DFDataSource) -> None:
        self._duckdb_connection.register(source.name, source.df)

    def _get_compiled_graph(self, llm_config: LLMConfig, agent_config: AgentConfig) -> CompiledStateGraph[Any]:
        """Get compiled graph."""
        compiled_graph = self._compiled_graph or self._graph.compile(llm_config, agent_config)
        self._compiled_graph = compiled_graph

        return compiled_graph

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

    def execute(
        self,
        opas: list[Opa],
        cache: Cache,
        llm_config: LLMConfig,
        agent_config: AgentConfig,
        sources: Sources,
        *,
        rows_limit: int = 100,
        stream: bool = True,
    ) -> ExecutionResult:
        compiled_graph = self._get_compiled_graph(llm_config, agent_config)
        messages: list[BaseMessage] = self._process_opas(opas, cache)

        # Prepend system message if not present
        all_messages_with_system = messages
        if not all_messages_with_system or all_messages_with_system[0].type != "system":
            all_messages_with_system = [
                SystemMessage(
                    self.render_system_prompt(self._duckdb_connection, sources, agent_config.recursion_limit)
                ),
                *all_messages_with_system,
            ]
        cleaned_messages = clean_tool_history(all_messages_with_system, llm_config.max_tokens_before_cleaning)

        init_state = self._graph.init_state(cleaned_messages, limit_max_rows=rows_limit)
        invoke_config = RunnableConfig(recursion_limit=agent_config.recursion_limit)
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
            self._update_message_history(cache, all_messages_without_system)

        # Set modality hints
        execution_result.meta[OutputModalityHints.META_KEY] = self._make_output_modality_hints(execution_result)

        return execution_result
