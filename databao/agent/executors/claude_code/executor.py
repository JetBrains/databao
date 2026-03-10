import logging
from typing import Any, TextIO, cast

from claude_agent_sdk import SdkMcpTool, tool
from duckdb import DuckDBPyConnection
from langchain_core.tools import BaseTool
from langgraph.graph.state import CompiledStateGraph

from databao.agent import Executor
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
from databao.agent.executors import LighthouseExecutor
from databao.agent.executors.base import GraphExecutor, DuckDBExecutor
from databao.agent.executors.lighthouse.graph import ExecuteSubmit, RUN_SQL_QUERY_TOOL_DESCRIPTION
from databao.agent.executors.prompt import build_context_text, get_today_date_str, load_prompt_template
from databao.agent.executors.utils import run_sql_query as _run_sql_query

_LOGGER = logging.getLogger(__name__)


class ClaudeCodeExecutor(DuckDBExecutor[SdkMcpTool]):
    DISPLAY_ROW_LIMIT = 12
    """Max number of rows to return in SQL tool calls."""

    DISPLAY_CELL_CHAR_LIMIT = 1024
    """Max number of characters a dataframe cell can have before it is trimmed."""

    SQL_ROW_LIMIT = None
    """Max number of rows to return in SQL tool calls."""

    def __init__(self, writer: Any = None) -> None:
        super().__init__(writer=writer)

        self._max_columns_per_table: int | None = None
        self._max_schema_summary_length: int | None = 250_000  # 1 token ~= 4 characters


    def register_tools(self, tools: list[SdkMcpTool]) -> None:
        """Register additional tools to be available during execution."""
        # TODO: add to allowed tool list

    def drop_last_opa_group(self, cache: "Cache", n: int = 1) -> None:
        pass



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

        db_schema = LighthouseExecutor.inspect_database_schema(
            data_connection,
            db_types,
            max_schema_summary_length=self._max_schema_summary_length,
            max_columns_per_table=self._max_columns_per_table,
        )

        sources = domain.sources
        context_text = build_context_text(
            sources,
            df_label_fn=lambda name: f"DF {name} (fully qualified name 'temp.main.{name}')",
        )

        dce_search_enabled = self.has_search_context_tool(domain)

        prompt = self._prompt_template.render(
            date=get_today_date_str(),
            db_schema=db_schema,
            context=context_text,
            tool_limit=recursion_limit // 2,
            db_types=db_types,
            dce_search_enabled=dce_search_enabled,
        )

        return prompt.strip()

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

        return execution_result

    def _build_tools(self) -> list[SdkMcpTool]:
        tools = []

        @tool("run_sql_query", RUN_SQL_QUERY_TOOL_DESCRIPTION, {"sql": str})
        def run_sql_query(args: dict) -> dict[str, Any]:
            result = _run_sql_query(args.get("sql", ""), con=self._duckdb_connection,
                                    sql_row_limit=self.SQL_ROW_LIMIT,
                                    display_row_limit=self.DISPLAY_ROW_LIMIT,
                                    display_cell_char_limit=self.DISPLAY_CELL_CHAR_LIMIT,)

            if (sql := result.get("sql")) and (csv := result.get("csv")):
                query_id = len(self._query_cache) + 1
                self._query_cache[query_id] = sql, csv
                result |= {"query_id": query_id}

            return {"content": [{"type": "text", "text": json.dumps(result, default=str)}]}

        tools.append(run_sql_query)

        if self.config.include_dce_search_context_tool:
            assert self.dce is not None, "DCE is required to use the DCE search context tool."
            _search_context = build_dce_search_tool(self.dce)

            @tool("search_context", DCE_SEARCH_CONTEXT_TOOL_DESCRIPTION, {"query": str})
            async def search_context(query: str) -> dict[str, Any]:
                result = await _search_context(query)
                return {"content": [{"type": "text", "text": json.dumps({"search_context_result": result})}]}

            tools.append(search_context)

        if self.config.include_submit_query_id_tool:

            @tool(
                "submit_query_id",
                """This tool call must be the last tool to be called by the model. 
                  It will provide to the user the generated sql and the output thereof resulting from the query with 
                   the respective query id. You will find the query ids of the error-free queries in the outputs of 
                   the run_sql_query tool in the`query_id` key. The `query_id` itself need not be the one of the last 
                   generated query, it rather needs to reference the query which most closely matches the 
                   user's question.

                   Args:
                        query_id: The ID of the query to submit.
                   """,
                {"query": int},
            )
            async def submit_query_id(args: dict) -> dict[str, Any]:
                query_id = args.get("query")
                if not query_id in self._query_cache:
                    return {
                        "content": [{"type": "text", "text": json.dumps({"error": f"Query id {query_id} not found"})}]
                    }
                sql, csv = self._query_cache[query_id]
                return {"content": [{"type": "text", "text": json.dumps({"sql": sql, "csv": csv})}]}

            tools.append(submit_query_id)

        return tools
