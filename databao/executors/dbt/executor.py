from __future__ import annotations

import contextlib
import datetime
from pathlib import Path
from typing import Any, TextIO, cast

import duckdb
import jinja2
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import BaseTool
from langgraph.graph.state import CompiledStateGraph

from databao.configs import LLMConfig
from databao.configs.agent import AgentConfig
from databao.core import Cache, Domain, ExecutionResult, Opa
from databao.core.data_source import Sources
from databao.core.domain import _Domain
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
from databao.executors.history_cleaning import clean_tool_history
from databao.executors.query_expansion import QueryExpansionConfig

_DBT_TARGET_FOLDER_KEY = "dbt_target_folder_path"


class DbtProjectExecutor(GraphExecutor):
    """
    A Lighthouse-style executor that runs the dbt project graph (DbtProjectGraph)
    """

    def __init__(
        self,
        *,
        dbt_config: DbtConfig | None = None,
        post_dbt_run_hook: PostDbtRunHook | None = None,
        expansion_config: QueryExpansionConfig | None = None,
        writer: TextIO | None = None,
    ) -> None:
        super().__init__(writer=writer)
        self._dbt_config = dbt_config or DbtConfig()
        self._expansion_config = expansion_config

        self._prompt_template = self._read_prompt_template("system_prompt.jinja")
        self._task_instruction = self._read_prompt_template("task_instruction.jinja").render()

        # Auto-detect post-run hook: DuckDB projects need checkpoint, others don't.
        self._post_dbt_run_hook = post_dbt_run_hook if post_dbt_run_hook is not None else duckdb_post_run_hook

        self._graph = DbtProjectGraph(
            query_runner_factory=self._make_query_runner,
            post_dbt_run_hook=self._post_dbt_run_hook,
        )
        self._dbt_dirty: bool = True

    def _detach_all_databases(self) -> None:
        """Detach all databases from the shared connection to release file locks."""
        for name in self._attached_db_paths:
            for name in self._attached_db_paths:
                with contextlib.suppress(Exception):
                    self._duckdb_connection.execute(f'DETACH "{name}"')

    @staticmethod
    def _resolve_project_dir(dbt_config: DbtConfig, sources: Sources) -> Path:
        """Extract the dbt project directory from explicit config or the domain's datasources.

        A dbt datasource stores ``dbt_target_folder_path`` pointing to ``<project_dir>/target``.
        The project root is its parent.
        """
        if dbt_config.project_dir is not None:
            return dbt_config.project_dir.resolve()
        for source in sources.dbs.values():
            target_path = source.config.content.get(_DBT_TARGET_FOLDER_KEY)
            if target_path is not None:
                return Path(target_path).resolve().parent
        raise ValueError(
            "Could not resolve dbt project directory. "
            "Ensure a dbt datasource with dbt_target_folder_path is configured in the DCE project."
        )

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

    @staticmethod
    def _get_today_date_str() -> str:
        return datetime.datetime.now().strftime("%Y-%m-%d")

    def render_system_prompt(self, sources: Sources, project_dir: Path, recursion_limit: int = 50) -> str:
        dbt_overview = assemble_dbt_project_summary(project_dir)
        attached_catalogs = list(self._attached_db_paths.keys()) or []

        context_text = ""
        for db_name, source in sources.dbs.items():
            if source.context:
                context_text += f"## Context for DB {db_name}\n\n{source.context}\n\n"
        for df_name, source in sources.dfs.items():
            if source.context:
                context_text += f"## Context for DF {df_name} (registered as '{df_name}')\n\n{source.context}\n\n"
        for idx, add_ctx in enumerate(sources.additional_context, start=1):
            context_text += f"## General information {idx}\n\n{add_ctx.strip()}\n\n"
        context_text = context_text.strip()

        datasource_entries = self._build_datasource_list(sources)

        system_prompt = self._prompt_template.render(
            dbt_overview=dbt_overview,
            dbt_directory=project_dir.absolute(),
            attached_catalogs=attached_catalogs,
            date=self._get_today_date_str(),
            context=context_text,
            datasources=datasource_entries,
            tool_limit=recursion_limit // 2,
        )
        return system_prompt.strip()

    @staticmethod
    def _build_datasource_list(sources: Sources) -> list[dict[str, str]]:
        """Build a list of datasource names with descriptions and types for the system prompt."""
        entries: list[dict[str, str]] = []
        for db_name, source in sources.dbs.items():
            desc = ""
            if source.context:
                first_line = source.context.strip().split("\n")[0][:120]
                desc = first_line
            entries.append(
                {
                    "name": db_name,
                    "description": desc,
                    "type": source.config.type.full_type,
                }
            )
        for df_name, source in sources.dfs.items():
            desc = ""
            if source.context:
                first_line = source.context.strip().split("\n")[0][:120]
                desc = first_line
            entries.append({"name": df_name, "description": desc, "type": "dataframe"})
        return entries

    def _compile_graph(
        self, llm_config: LLMConfig, agent_config: AgentConfig, domain: Domain, extra_tools: list[BaseTool] | None
    ) -> CompiledStateGraph[Any]:
        expansion_llm = llm_config.new_chat_model() if self._expansion_config else None
        self._graph._expansion_llm = expansion_llm
        self._graph._expansion_config = self._expansion_config
        return self._graph.compile(llm_config, agent_config, domain, extra_tools=extra_tools)

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
        # NOTE: (@gas) release file locks — this executor uses short-lived connections
        # Detaching allows the dbt subprocess to write freely.
        self._detach_all_databases()

        compiled_graph = self._get_compiled_graph(llm_config, agent_config, domain)
        messages: list[BaseMessage] = self._process_opas(opas, cache)

        sources = cast(_Domain, domain).sources
        project_dir = self._resolve_project_dir(self._dbt_config, sources)

        all_messages_with_system = messages
        if not all_messages_with_system or all_messages_with_system[0].type != "system":
            all_messages_with_system = [
                SystemMessage(self.render_system_prompt(sources, project_dir, agent_config.recursion_limit)),
                HumanMessage(self._task_instruction),
                *all_messages_with_system,
            ]

        cleaned_messages = clean_tool_history(all_messages_with_system, llm_config.max_tokens_before_cleaning)

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
