from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import jinja2
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph.state import CompiledStateGraph
from sqlalchemy import Connection, Engine

from databao.configs import LLMConfig
from databao.core import Cache, ExecutionResult, Opa
from databao.core.data_source import DBDataSource, DFDataSource, Sources
from databao.core.executor import OutputModalityHints
from databao.dbt.config import DbtConfig
from databao.duckdb.utils import get_db_path, register_sqlalchemy
from databao.executors.base import GraphExecutor
from databao.executors.dbt.graph import DbtProjectGraph
from databao.executors.lighthouse.history_cleaning import clean_tool_history


class DbtProjectExecutor(GraphExecutor):
    """
    A Lighthouse-style executor that runs the dbt project graph (DbtProjectGraph),
    but uses the *same* dbt system prompt rendering approach as the dbt LangChain agent:
    - assemble_dbt_project_summary(project_dir)
    - render databao.dbt/system_prompt.jinja with dbt_overview + dbt_directory
    """

    _DBT_TASK_INSTRUCTION = (
        "Complete the dbt project. Make sure that the project builds successfully! And then answer the user's question.\n\n"
        "If completing the project is needed to answer correctly, do it (e.g., when the requested metric/transformation "
        "looks reusable and should live in dbt). Otherwise, answer directly without adding models.\n"
    )

    def __init__(self, *, dbt_config: DbtConfig) -> None:
        super().__init__()
        self._dbt_config = dbt_config

        self._prompt_template = self._read_prompt_template("system_prompt.jinja")

        self._duckdb_connection = duckdb.connect(":memory:")
        self._graph = DbtProjectGraph(db_conn=self._duckdb_connection)
        self._compiled_graph: CompiledStateGraph[Any] | None = None

    @staticmethod
    def _read_prompt_template(template_name: str) -> jinja2.Template:
        env = jinja2.Environment(
            loader=jinja2.PackageLoader("databao.executors.dbt", ""),
            trim_blocks=True,
            lstrip_blocks=True,
        )
        return env.get_template(template_name)

    def render_system_prompt(self) -> str:
        from databao.dbt.agent import assemble_dbt_project_summary

        project_dir = self._dbt_config.project_dir.resolve()
        dbt_overview = assemble_dbt_project_summary(project_dir)

        system_prompt = self._prompt_template.render(
            dbt_overview=dbt_overview,
            dbt_directory=project_dir.absolute(),
        )
        return system_prompt.strip()

    def register_db(self, source: DBDataSource) -> None:
        connection = source.db_connection
        if isinstance(connection, Connection):
            connection = connection.engine

        if isinstance(connection, duckdb.DuckDBPyConnection):
            path = get_db_path(connection)
            if path is None:
                raise RuntimeError("Memory-based DuckDB is not supported.")
            connection.close()
            self._duckdb_connection.execute(f"ATTACH '{path}' AS {source.name} (READ_ONLY)")
            return

        if isinstance(connection, Engine):
            register_sqlalchemy(self._duckdb_connection, connection, source.name)
            return

        raise ValueError("Only DuckDB or SQLAlchemy connections are supported.")

    def register_df(self, source: DFDataSource) -> None:
        self._duckdb_connection.register(source.name, source.df)

    def _get_compiled_graph(self, llm_config: LLMConfig) -> CompiledStateGraph[Any]:
        compiled_graph = self._compiled_graph or self._graph.compile(llm_config)
        self._compiled_graph = compiled_graph
        return compiled_graph

    def drop_last_opa_group(self, cache: Cache, n: int = 1) -> None:
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
        sources: Sources,
        *,
        rows_limit: int = 100,  # not used here; kept for interface compatibility
        stream: bool = True,
    ) -> ExecutionResult:
        compiled_graph = self._get_compiled_graph(llm_config)
        messages: list[BaseMessage] = self._process_opas(opas, cache)

        all_messages_with_system = messages
        if not all_messages_with_system or all_messages_with_system[0].type != "system":
            all_messages_with_system = [
                SystemMessage(self.render_system_prompt()),
                HumanMessage(self._DBT_TASK_INSTRUCTION),
                *all_messages_with_system,
            ]

        cleaned_messages = clean_tool_history(all_messages_with_system, llm_config.max_tokens_before_cleaning)

        project_dir = self._dbt_config.project_dir.resolve()
        pre_existing_files = [str(p.resolve()) for p in project_dir.rglob("*") if p.is_file()]

        init_state = self._graph.init_state(
            cleaned_messages,
            project_dir=project_dir,
            pre_existing_files=pre_existing_files,
            dbt_timeout_seconds=self._dbt_config.dbt_timeout_seconds,
        )

        invoke_config = RunnableConfig(recursion_limit=llm_config.agent_recursion_limit)
        last_state = self._invoke_graph_sync(compiled_graph, init_state, config=invoke_config, stream=stream)

        result = self._graph.get_result(last_state)

        final_messages = last_state.get("messages", [])
        if final_messages:
            new_messages = final_messages[len(cleaned_messages) :]
            all_messages = all_messages_with_system + new_messages
            all_messages_without_system = [m for m in all_messages if m.type != "system"]
            self._update_message_history(cache, all_messages_without_system)

        execution_result = ExecutionResult(
            text=str(result.get("text", "")),
            df=None,
            code=result.get("code"),
            meta={
                "messages": final_messages or [],
                "tool_calls": result.get("tool_calls", []),
                "dbt_project_dir": str(project_dir),
            },
        )

        execution_result.meta[OutputModalityHints.META_KEY] = OutputModalityHints(
            visualization_prompt=None,
            should_visualize=False,
        )
        return execution_result
