from __future__ import annotations

from pathlib import Path
from typing import Any, TextIO, cast

import duckdb
from langchain_core.messages import HumanMessage
from langchain_core.tools import BaseTool
from langgraph.graph.state import CompiledStateGraph

from databao.configs import LLMConfig
from databao.configs.agent import AgentConfig
from databao.core import Cache, Domain, ExecutionResult, Opa
from databao.core.data_source import Sources
from databao.core.domain import _Domain
from databao.databases.databases import db_type, register_db_in_duckdb
from databao.executors.base import GraphExecutor
from databao.executors.dbt.config import DbtConfig
from databao.executors.dbt.dbt_runner import (
    PostDbtRunHook,
    assemble_dbt_project_summary,
    duckdb_post_run_hook,
)
from databao.executors.dbt.errors import DbtMissingWarehouseError
from databao.executors.dbt.graph import DbtProjectGraph
from databao.executors.dbt.query_runner import DuckDbQueryRunner
from databao.executors.prompt import build_context_text, get_today_date_str, load_prompt_template
from databao.executors.query_expansion import QueryExpansionConfig


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

        self._prompt_template = load_prompt_template("databao.executors.dbt", "system_prompt.jinja")
        self._task_instruction = load_prompt_template("databao.executors.dbt", "task_instruction.jinja").render()

        # Auto-detect post-run hook: DuckDB projects need checkpoint, others don't.
        self._post_dbt_run_hook = post_dbt_run_hook if post_dbt_run_hook is not None else duckdb_post_run_hook

        self._graph = DbtProjectGraph(
            query_runner_factory=self._make_query_runner,
            post_dbt_run_hook=self._post_dbt_run_hook,
        )
        self._dbt_dirty: bool = True

    @staticmethod
    def _resolve_project_dir(dbt_config: DbtConfig, sources: Sources) -> Path:
        """Extract the dbt project directory from explicit config or the domain's datasources."""
        if dbt_config.project_dir is not None:
            return dbt_config.project_dir.resolve()
        # TODO: (@gas) the same sources are passed to register_dbt
        for source in sources.dbts.values():
            return source.dir.resolve()
        raise ValueError(
            "Could not resolve dbt project directory. "
            "Ensure a dbt datasource with dbt_target_folder_path is configured in the DCE project."
        )

    def _make_query_runner(self) -> DuckDbQueryRunner:
        """Create a short-lived DuckDB read-only query runner from the shared connection state.

        Uses the base class's registered data sources (populated by _init_sources_from_domain)
        to build a fresh read-only connection. This ensures dbt's writes are visible after each run.
        """
        con = duckdb.connect(":memory:")
        first_source_name: str | None = None
        for name, db_source in self._registered_dbs.items():
            register_db_in_duckdb(con, db_source.config, name)
            if first_source_name is None:
                first_source_name = name
        for name, df_source in self._registered_dfs.items():
            con.register(name, df_source.df)
            if first_source_name is None:
                first_source_name = name
        # TODO: (@gas) check - should work without USE "{first_db_name}"
        if first_source_name is not None:
            con.execute(f'USE "{first_source_name}"')
        return DuckDbQueryRunner(con)

    def render_system_prompt(self, sources: Sources, project_dir: Path, recursion_limit: int = 50) -> str:
        dbt_overview = assemble_dbt_project_summary(project_dir)
        attached_catalogs: list[str] = list(sources.dbs.keys()) + list(sources.dfs.keys())

        context_text = build_context_text(
            sources,
            include_dbts=True,
            df_label_fn=lambda name: f"DF {name} (registered as '{name}')",
        )

        datasource_entries = self._build_datasource_list(sources)

        system_prompt = self._prompt_template.render(
            dbt_overview=dbt_overview,
            dbt_directory=project_dir.absolute(),
            attached_catalogs=attached_catalogs,
            date=get_today_date_str(),
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
            if source.description:
                first_line = source.description.strip().split("\n")[0][:120]
                desc = first_line
            entries.append({"name": db_name, "description": desc, "type": db_type(source.config).full_type})
        for df_name, source in sources.dfs.items():
            desc = ""
            if source.description:
                first_line = source.description.strip().split("\n")[0][:120]
                desc = first_line
            entries.append({"name": df_name, "description": desc, "type": "dataframe"})
        for dbt_name, source in sources.dbts.items():
            desc = ""
            if source.description:
                first_line = source.description.strip().split("\n")[0][:120]
                desc = first_line
            entries.append({"name": dbt_name, "description": desc, "type": "dbt"})
        return entries

    def _compile_graph(
        self, llm_config: LLMConfig, agent_config: AgentConfig, domain: Domain, extra_tools: list[BaseTool] | None
    ) -> CompiledStateGraph[Any]:
        expansion_llm = llm_config.new_chat_model() if self._expansion_config else None
        self._graph._expansion_llm = expansion_llm
        self._graph._expansion_config = self._expansion_config
        return self._graph.compile(llm_config, agent_config, domain, extra_tools=extra_tools)

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
        self._init_sources_from_domain(domain, register_in_duckdb=False)

        if not self._registered_dbs and not self._registered_dfs:
            raise DbtMissingWarehouseError()

        sources = cast(_Domain, domain).sources
        project_dir = self._resolve_project_dir(self._dbt_config, sources)

        system_prompt = self.render_system_prompt(sources, project_dir, agent_config.recursion_limit)
        pre_existing_files = [str(p.resolve()) for p in project_dir.rglob("*") if p.is_file()]
        init_state = self._graph.init_state(
            [],
            project_dir=project_dir,
            pre_existing_files=pre_existing_files,
            dbt_timeout_seconds=self._dbt_config.dbt_timeout_seconds,
            dbt_dirty=self._dbt_dirty,
        )

        execution_result, last_state = self._execute_core(
            opas,
            cache,
            llm_config,
            agent_config,
            domain,
            system_prompt=system_prompt,
            init_state=init_state,
            get_result=self._graph.get_result,
            extra_preamble=[HumanMessage(self._task_instruction)],
            stream=stream,
            writer=writer,
        )

        self._dbt_dirty = last_state.get("dbt_dirty", True)
        execution_result.meta["dbt_project_dir"] = str(project_dir)

        return execution_result
