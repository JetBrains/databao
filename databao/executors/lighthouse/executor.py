from typing import Any, TextIO, cast

from langchain_core.tools import BaseTool
from langgraph.graph.state import CompiledStateGraph

from databao.configs import LLMConfig
from databao.configs.agent import AgentConfig
from databao.core import Cache, Domain, ExecutionResult, Opa
from databao.core.domain import _Domain
from databao.databases.databases import db_type
from databao.duckdb.utils import describe_duckdb_schema
from databao.executors.base import GraphExecutor
from databao.executors.lighthouse.graph import ExecuteSubmit
from databao.executors.prompt import build_context_text, get_today_date_str, load_prompt_template


class LighthouseExecutor(GraphExecutor):
    def __init__(self, writer: Any = None) -> None:
        super().__init__(writer=writer)
        self._prompt_template = load_prompt_template("databao.executors.lighthouse", "system_prompt.jinja")
        self._graph: ExecuteSubmit = ExecuteSubmit(self._duckdb_connection)

        self._max_columns_per_table: int | None = None

    def render_system_prompt(
        self,
        data_connection: Any,
        domain: Domain,
        recursion_limit: int = 50,
    ) -> str:
        """Render system prompt with database schema."""
        domain = cast(_Domain, domain)

        # As a workaround for snowflake where we execute queries directly using `snowflake_query`
        # we need the original catalog name for the agent to write correct queries.
        # For normal duckdb based execution, we instead need the "new" catalog name
        # which is the user provided name of the attached datasource.
        need_original_catalog_name = False

        db_types = {}
        for name, source in domain.sources.dbs.items():
            db_type_ = db_type(source.config).full_type
            db_types[name] = db_type_
            if db_type_ == "snowflake":
                need_original_catalog_name = True

        db_schema = describe_duckdb_schema(
            data_connection,
            max_cols_per_table=self._max_columns_per_table,
            include_original_catalog_name=need_original_catalog_name,
        )

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
