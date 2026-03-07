import logging
from typing import Any, TextIO, cast

from duckdb import DuckDBPyConnection
from langchain_core.tools import BaseTool
from langgraph.graph.state import CompiledStateGraph

from databao.agent.configs import LLMConfig
from databao.agent.configs.agent import AgentConfig
from databao.agent.core import Cache, Domain, ExecutionResult, Opa
from databao.agent.core.domain import _Domain
from databao.agent.databases.databases import db_type as get_db_type
from databao.agent.duckdb.schema_inspection import (
    TableInfo,
    inspect_duckdb_schema,
    summarize_duckdb_schema,
    summarize_duckdb_schema_overview,
)
from databao.agent.executors.base import GraphExecutor
from databao.agent.executors.lighthouse.graph import ExecuteSubmit
from databao.agent.executors.prompt import build_context_text, get_today_date_str, load_prompt_template

_LOGGER = logging.getLogger(__name__)


class LighthouseExecutor(GraphExecutor):
    def __init__(self, writer: Any = None) -> None:
        super().__init__(writer=writer)
        self._prompt_template = load_prompt_template("databao.agent.executors.lighthouse", "system_prompt.jinja")
        self._graph: ExecuteSubmit = ExecuteSubmit(self._duckdb_connection)

        self._max_columns_per_table: int | None = None
        self._max_schema_summary_length: int | None = 250_000  # 1 token ~= 4 characters

    def _summarize_db_schema(
        self, tables: list[TableInfo], db_types: dict[str, str], max_cols_per_table: int | None
    ) -> str:
        # As a workaround for snowflake where we execute queries directly using `snowflake_query`
        # we need the original catalog name for the agent to write correct queries.
        # For normal duckdb based execution, we instead need the "new" catalog name
        # which is the user provided name of the attached datasource.
        # This filtering works because table_catalog matches the name of the attached datasource.
        sf_db_names = {name for name, db_type in db_types.items() if db_type == "snowflake"}
        sf_tables = [table for table in tables if table.table_catalog in sf_db_names]
        duckdb_tables = [table for table in tables if table.table_catalog not in sf_db_names]

        sf_schema = (
            summarize_duckdb_schema(
                sf_tables, max_cols_per_table=max_cols_per_table, include_original_catalog_name=True
            )
            if len(sf_tables) > 0
            else ""
        )
        duckdb_schema = (
            summarize_duckdb_schema(
                duckdb_tables, max_cols_per_table=max_cols_per_table, include_original_catalog_name=False
            )
            if len(duckdb_tables) > 0
            else ""
        )
        schemas = [sf_schema, duckdb_schema]
        schemas = [schema for schema in schemas if len(schema) > 0]
        if len(schemas) == 0:
            return "(no tables found)"
        return "\n".join(schemas)

    def _summarize_db_schema_overview(self, tables: list[TableInfo], db_types: dict[str, str]) -> str:
        sf_db_names = {name for name, db_type in db_types.items() if db_type == "snowflake"}
        sf_tables = [table for table in tables if table.table_catalog in sf_db_names]
        duckdb_tables = [table for table in tables if table.table_catalog not in sf_db_names]

        sf_schema = (
            summarize_duckdb_schema_overview(sf_tables, include_original_catalog_name=True)
            if len(sf_tables) > 0
            else ""
        )
        duckdb_schema = (
            summarize_duckdb_schema_overview(duckdb_tables, include_original_catalog_name=False)
            if len(duckdb_tables) > 0
            else ""
        )
        schemas = [sf_schema, duckdb_schema]
        schemas = [schema for schema in schemas if len(schema) > 0]
        if len(schemas) == 0:
            return "(no tables found)"
        return "\n".join(schemas)

    def _inspect_database_schema(self, connection: DuckDBPyConnection, db_types: dict[str, str]) -> str:
        try:
            tables = inspect_duckdb_schema(connection)
        except Exception as e:
            _LOGGER.warning(f"Failed to inspect duckdb schema: {e}")
            return "(failed to fetch schema)"

        db_schema = self._summarize_db_schema(tables, db_types, self._max_columns_per_table)
        if self._max_schema_summary_length is None:
            return db_schema

        if len(db_schema) > self._max_schema_summary_length:
            # Retry by listing only table names without any column information.
            db_schema = self._summarize_db_schema(tables, db_types, 0)

        if len(db_schema) > self._max_schema_summary_length:
            # Fallback to showing only schemas without tables names.
            db_schema = self._summarize_db_schema_overview(tables, db_types)

        return db_schema

    def render_system_prompt(
        self,
        data_connection: DuckDBPyConnection,
        domain: Domain,
        recursion_limit: int = 50,
    ) -> str:
        """Render system prompt with database schema."""
        domain = cast(_Domain, domain)

        db_types = {}
        for name, source in domain.sources.dbs.items():
            db_type = get_db_type(source.config).full_type
            db_types[name] = db_type

        db_schema = self._inspect_database_schema(data_connection, db_types)

        sources = domain.sources
        context_text = build_context_text(
            sources,
            df_label_fn=lambda name: f"DF {name} (fully qualified name 'temp.main.{name}')",
        )

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
        system_prompt = self.render_system_prompt(self._duckdb_connection, domain, agent_config.recursion_limit)
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
