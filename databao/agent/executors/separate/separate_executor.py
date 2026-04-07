import logging
from dataclasses import replace
from typing import Any, TextIO, cast

from langchain_core.tools import BaseTool
from langgraph.graph.state import CompiledStateGraph
from sqlalchemy import Engine

from databao.agent.configs import LLMConfig
from databao.agent.configs.agent import AgentConfig
from databao.agent.core import Cache, Domain, ExecutionResult, Opa
from databao.agent.core.domain import _Domain
from databao.agent.databases.databases import db_type as get_db_type
from databao.agent.databases.databases import try_create_sqlalchemy_engine
from databao.agent.duckdb.schema_inspection import (
    TableInfo,
    summarize_duckdb_schema,
    summarize_duckdb_schema_overview,
)
from databao.agent.executors.base import GraphExecutor
from databao.agent.executors.prompt import build_context_text, get_today_date_str, load_prompt_template
from databao.agent.executors.separate.graph import SeparateGraph
from databao.agent.sqlalchemy.schema_inspection import inspect_sqlalchemy_schema

_LOGGER = logging.getLogger(__name__)


class SeparateExecutor(GraphExecutor):
    """Executor that works directly with each database via its own SQLAlchemy connection.

    SQL queries are routed to the appropriate engine by the ``datasource`` argument of
    the ``run_sql_query`` tool.
    """

    def __init__(self, writer: Any = None) -> None:
        super().__init__(writer=writer)
        self._sa_engines: dict[str, Engine] = {}
        self._prompt_template = load_prompt_template("databao.agent.executors.separate", "system_prompt.jinja")
        self._graph: SeparateGraph = SeparateGraph(self._sa_engines)

        self._max_columns_per_table: int | None = None
        self._max_schema_summary_length: int | None = 250_000  # 1 token ~= 4 characters

    def _init_sources_from_domain(self, domain: Domain, *, register_in_duckdb: bool = True) -> None:
        """Register domain sources.

        DB sources are connected via SQLAlchemy engines stored in ``_sa_engines``.
        The ``register_in_duckdb`` parameter is accepted for API compatibility but ignored.
        """
        if not isinstance(domain, _Domain):
            return
        sources = domain.sources

        for name, db_source in sources.dbs.items():
            if name not in self._registered_dbs:
                engine = try_create_sqlalchemy_engine(db_source.config)
                if engine is not None:
                    self._sa_engines[name] = engine
                else:
                    db_type = get_db_type(db_source.config)
                    _LOGGER.warning(
                        "SQLAlchemy engine creation not implemented for database '%s' (type '%s'); "
                        "continuing without SQLAlchemy engine",
                        name,
                        db_type,
                    )
                self._registered_dbs[name] = db_source

        for name, df_source in sources.dfs.items():
            if name not in self._registered_dfs:
                self._registered_dfs[name] = df_source

        for name, dbt_source in sources.dbts.items():
            if name not in self._registered_dbts:
                self._registered_dbts[name] = dbt_source

    def _inspect_database_schema(self) -> str:
        tables: list[TableInfo] = []

        for name, _db_source in self._registered_dbs.items():
            engine = self._sa_engines.get(name)
            if engine is None:
                continue
            try:
                db_tables = inspect_sqlalchemy_schema(engine)
                # Use the registered name as table_catalog so the LLM can derive
                # the datasource argument directly from the schema prefix.
                tables.extend(replace(t, table_catalog=name, columns_catalog=name) for t in db_tables)
            except Exception as e:
                _LOGGER.warning("Failed to inspect schema for '%s': %s", name, e)

        db_schema = _summarize(tables, self._max_columns_per_table)
        if self._max_schema_summary_length is None:
            return db_schema

        if len(db_schema) > self._max_schema_summary_length:
            db_schema = _summarize(tables, 0)

        if len(db_schema) > self._max_schema_summary_length:
            db_schema = _summarize_overview(tables)

        return db_schema

    def render_system_prompt(self, domain: Domain, recursion_limit: int = 50) -> str:
        domain = cast(_Domain, domain)

        db_types = {name: get_db_type(src.config).full_type for name, src in domain.sources.dbs.items()}
        db_schema = self._inspect_database_schema()

        sources = domain.sources
        context_text = build_context_text(sources, df_label_fn=lambda name: f"DF {name}")

        dce_search_enabled = self._graph.has_search_context_tool(domain)

        prompt = self._prompt_template.render(
            date=get_today_date_str(),
            db_schema=db_schema,
            context=context_text,
            tool_limit=recursion_limit // 2,
            db_types=db_types,
            dce_search_enabled=dce_search_enabled,
        )
        return prompt.strip()

    def _compile_graph(
        self, llm_config: LLMConfig, agent_config: AgentConfig, domain: Domain, extra_tools: list[BaseTool] | None
    ) -> CompiledStateGraph[Any]:
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
        self._init_sources_from_domain(domain)
        system_prompt = self.render_system_prompt(domain, agent_config.recursion_limit)
        init_state = self._graph.init_state([], limit_max_rows=rows_limit)

        execution_result, _ = self._execute_core(
            opas,
            cache,
            llm_config,
            agent_config,
            domain,
            system_prompt=system_prompt,
            init_state=init_state,
            get_result=self._graph.get_result,
            stream=stream,
            writer=writer,
        )
        return execution_result


def _summarize(tables: list[TableInfo], max_cols_per_table: int | None = None) -> str:
    if not tables:
        return "(no tables found)"
    return summarize_duckdb_schema(tables, max_cols_per_table=max_cols_per_table, include_original_catalog_name=False)


def _summarize_overview(tables: list[TableInfo]) -> str:
    if not tables:
        return "(no tables found)"
    return summarize_duckdb_schema_overview(tables, include_original_catalog_name=False)
