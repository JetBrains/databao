import logging
from typing import Any, TextIO, cast

from duckdb import DuckDBPyConnection
from langchain_core.tools import BaseTool
from langgraph.graph.state import CompiledStateGraph

from databao.configs import LLMConfig
from databao.configs.agent import AgentConfig
from databao.core import Cache, Domain, ExecutionResult, Opa
from databao.core.domain import _Domain
from databao.databases.databases import db_type as get_db_type
from databao.duckdb.utils import inspect_duckdb_schema, summarize_duckdb_schema
from databao.executors.base import GraphExecutor
from databao.executors.lighthouse.graph import ExecuteSubmit
from databao.executors.prompt import build_context_text, get_today_date_str, load_prompt_template

_LOGGER = logging.getLogger(__name__)


class LighthouseExecutor(GraphExecutor):
    def __init__(self, writer: Any = None) -> None:
        super().__init__(writer=writer)
        self._prompt_template = load_prompt_template("databao.executors.lighthouse", "system_prompt.jinja")
        self._graph: ExecuteSubmit = ExecuteSubmit(self._duckdb_connection)

        self._max_columns_per_table: int | None = None
        self._max_schema_summary_length: int | None = 250_000  # 1 token ~= 4 characters

    def _inspect_database_schema(self, connection: DuckDBPyConnection, db_types: dict[str, str]) -> str:
        try:
            tables = inspect_duckdb_schema(connection)
        except Exception as e:
            _LOGGER.warning(f"Failed to inspect duckdb schema: {e}")
            return "(failed to fetch schema)"

        # As a workaround for snowflake where we execute queries directly using `snowflake_query`
        # we need the original catalog name for the agent to write correct queries.
        # For normal duckdb based execution, we instead need the "new" catalog name
        # which is the user provided name of the attached datasource.
        need_original_catalog_name = False

        for _, db_type in db_types.items():
            if db_type == "snowflake":
                need_original_catalog_name = True

        db_schema = summarize_duckdb_schema(
            tables,
            max_cols_per_table=self._max_columns_per_table,
            include_original_catalog_name=need_original_catalog_name,
        )

        if self._max_schema_summary_length is None:
            return db_schema

        if len(db_schema) > self._max_schema_summary_length:
            # Retry by listing only table names without any column information.
            db_schema = summarize_duckdb_schema(
                tables, max_cols_per_table=0, include_original_catalog_name=need_original_catalog_name
            )

        if len(db_schema) > self._max_schema_summary_length:
            db_schema = "(the database schema is too large)"

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
