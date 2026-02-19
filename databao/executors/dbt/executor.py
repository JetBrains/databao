from __future__ import annotations

from pathlib import Path
from typing import Any, TextIO

import duckdb
import jinja2
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph.state import CompiledStateGraph

from databao.configs import LLMConfig
from databao.configs.agent import AgentConfig
from databao.core import Cache, Domain, ExecutionResult, Opa
from databao.core.executor import OutputModalityHints
from databao.executors.base import GraphExecutor
from databao.executors.dbt.config import DbtConfig
from databao.executors.dbt.dbt_runner import (
    PostDbtRunHook,
    assemble_dbt_project_summary,
    duckdb_post_run_hook,
)
from databao.executors.dbt.graph import DbtProjectGraph
from databao.executors.dbt.query_runner import DuckDbQueryRunner
from databao.executors.lighthouse.history_cleaning import clean_tool_history


class DbtProjectExecutor(GraphExecutor):
    """
    A Lighthouse-style executor that runs the dbt project graph (DbtProjectGraph)
    """

    def __init__(
        self,
        *,
        dbt_config: DbtConfig,
        post_dbt_run_hook: PostDbtRunHook | None = None,
        writer: TextIO | None = None,
    ) -> None:
        super().__init__(writer=writer)
        self._dbt_config = dbt_config

        self._prompt_template = self._read_prompt_template("system_prompt.jinja")
        self._task_instruction = self._read_prompt_template("task_instruction.jinja").render()

        # Auto-detect post-run hook: DuckDB projects need checkpoint, others don't.
        # Can be overridden explicitly via constructor.
        self._post_dbt_run_hook = post_dbt_run_hook if post_dbt_run_hook is not None else duckdb_post_run_hook

        self._graph = DbtProjectGraph(
            query_runner_factory=self._make_query_runner,
            post_dbt_run_hook=self._post_dbt_run_hook,
        )
        self._compiled_graph: CompiledStateGraph[Any] | None = None
        self._current_cache_scope: str | None = None
        self._dbt_dirty: bool = True

    def _make_query_runner(self) -> DuckDbQueryRunner:
        """Create a short-lived DuckDB read-only query runner from the shared connection state.

        Uses the base class's shared DuckDB connection metadata (attached paths + registered DFs)
        to build a fresh read-only connection. This ensures dbt's writes are visible after each run.
        """
        con = duckdb.connect(":memory:")
        first_db_name: str | None = None
        for name, path in self._attached_db_paths.items():
            resolved_path = str(Path(path).resolve())
            con.execute(f"ATTACH '{resolved_path}' AS \"{name}\" (READ_ONLY)")
            if first_db_name is None:
                first_db_name = name
        for name, df in self._registered_dfs.items():
            con.register(name, df)
        if first_db_name is not None:
            con.execute(f'USE "{first_db_name}"')
        return DuckDbQueryRunner(con)

    @staticmethod
    def _read_prompt_template(template_name: str) -> jinja2.Template:
        env = jinja2.Environment(
            loader=jinja2.PackageLoader("databao.executors.dbt", ""),
            trim_blocks=True,
            lstrip_blocks=True,
        )
        return env.get_template(template_name)

    def render_system_prompt(self) -> str:
        project_dir = self._dbt_config.project_dir.resolve()
        dbt_overview = assemble_dbt_project_summary(project_dir)
        attached_catalogs = list(self._attached_db_paths.keys()) or []

        system_prompt = self._prompt_template.render(
            dbt_overview=dbt_overview,
            dbt_directory=project_dir.absolute(),
            attached_catalogs=attached_catalogs,
        )
        return system_prompt.strip()

    def _get_compiled_graph(self, llm_config: LLMConfig, agent_config: AgentConfig) -> CompiledStateGraph[Any]:
        compiled_graph = self._compiled_graph or self._graph.compile(llm_config, agent_config)
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
        agent_config: AgentConfig,
        domain: Domain,
        *,
        rows_limit: int = 100,
        stream: bool = True,
        writer: TextIO | None = None,
    ) -> ExecutionResult:
        # Detect thread switch via cache prefix and invalidate introspection cache
        # TODO: (@gas) revisit after integrating with DCE
        cache_prefix = getattr(cache, "_prefix", None)
        if cache_prefix != self._current_cache_scope:
            self._current_cache_scope = cache_prefix
            self._graph.invalidate_introspect_cache()

        compiled_graph = self._get_compiled_graph(llm_config, agent_config)
        messages: list[BaseMessage] = self._process_opas(opas, cache)

        all_messages_with_system = messages
        if not all_messages_with_system or all_messages_with_system[0].type != "system":
            all_messages_with_system = [
                SystemMessage(self.render_system_prompt()),
                HumanMessage(self._task_instruction),
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
            dbt_dirty=self._dbt_dirty,
        )

        invoke_config = RunnableConfig(recursion_limit=max(self._graph_recursion_limit, agent_config.recursion_limit))
        last_state = self._invoke_graph_sync(
            compiled_graph, init_state, config=invoke_config, stream=stream, writer=writer or self._writer
        )

        # Persist dbt_dirty flag across execute() calls
        self._dbt_dirty = last_state.get("dbt_dirty", True)

        result = self._graph.get_result(last_state)

        final_messages = last_state.get("messages", [])
        if final_messages:
            new_messages = final_messages[len(cleaned_messages) :]
            all_messages = all_messages_with_system + new_messages
            all_messages_without_system = [m for m in all_messages if m.type != "system"]
            self._update_message_history(cache, all_messages_without_system)

        execution_result = ExecutionResult(
            text=str(result.get("text", "")),
            df=result.get("df"),
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
