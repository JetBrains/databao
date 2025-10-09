import json
import logging
from typing import Any, TypedDict

from duckdb import DuckDBPyConnection
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import BaseMessage
from langchain_core.tools import tool
from langgraph.graph.state import CompiledStateGraph
from langgraph.prebuilt import create_react_agent

from portus.agent.base_agent import BaseAgent, ExecutionResult
from portus.agent.utils import sql_strip
from portus.data_source.data_collection import DataCollection

logger = logging.getLogger(__name__)


class AgentResponse(TypedDict):
    sql: str
    explanation: str


class ReactDuckDBAgent(BaseAgent):
    def __init__(
        self,
        data_collection: DataCollection,
        llm: BaseChatModel,
    ):
        self._data_collection = data_collection
        self._connection = self._data_collection.get_connection()
        self._compiled_graph = self._make_react_duckdb_agent(self._connection, llm)
        self._rows_limit = 100

    @staticmethod
    def describe_duckdb_schema(con: DuckDBPyConnection, max_cols_per_table: int = 40) -> str:
        rows = con.execute("""
                           SELECT table_catalog, table_schema, table_name
                           FROM information_schema.tables
                           WHERE table_type IN ('BASE TABLE', 'VIEW')
                             AND table_schema NOT IN ('pg_catalog', 'pg_toast', 'information_schema')
                           ORDER BY table_schema, table_name
                           """).fetchall()

        lines: list[str] = []
        for db, schema, table in rows:
            cols = con.execute(
                """
                               SELECT column_name, data_type
                               FROM information_schema.columns
                               WHERE table_schema = ?
                                 AND table_name = ?
                               ORDER BY ordinal_position
                               """,
                [schema, table],
            ).fetchall()
            if len(cols) > max_cols_per_table:
                cols = cols[:max_cols_per_table]
                suffix = " ... (truncated)"
            else:
                suffix = ""
            col_desc = ", ".join(f"{c} {t}" for c, t in cols)
            lines.append(f"{db}.{schema}.{table}({col_desc}){suffix}")
        return "\n".join(lines) if lines else "(no base tables found)"

    @staticmethod
    def _make_duckdb_tool(con: DuckDBPyConnection) -> Any:
        @tool("execute_sql")
        def execute_sql(sql: str, limit: int = 10) -> str:
            """
            Execute any SQL against DuckDB.

            Args:
                sql: The SQL statement to execute (single statement).
                limit: Optional row cap for result-returning statements (10 by default).

            Returns:
                JSON string: { "columns": [...], "rows": str, "limit": int, "note": str }
            """
            statement = sql_strip(sql)
            try:
                sql_to_run = statement
                if limit and " LIMIT " not in statement.upper():
                    sql_to_run = f"{statement} LIMIT {int(limit)}"
                df = con.execute(sql_to_run).df()
                payload = {
                    "columns": list(df.columns),
                    "rows": df.to_string(index=False),
                    "limit": limit,
                    "note": "Query executed successfully",
                }
                return json.dumps(payload)
            except Exception as e:
                payload = {
                    "columns": [],
                    "rows": [],
                    "limit": limit,
                    "note": f"SQL error: {type(e).__name__}: {e}",
                }
                return json.dumps(payload)

        return execute_sql

    @staticmethod
    def _make_react_duckdb_agent(con: DuckDBPyConnection, llm: BaseChatModel) -> CompiledStateGraph[Any]:
        schema_text = ReactDuckDBAgent.describe_duckdb_schema(con)
        # TODO move to .jinja (and fix indendation)
        SYSTEM_PROMPT = f"""You are a careful data analyst using the ReAct pattern with tools.
    Use the `execute_sql` tool to run exactly one DuckDB SQL statement when needed.

    Guidelines:
    - Translate the NL question to ONE DuckDB SQL statement.
    - Use provided schema.
    - You can fetch extra details about schema/tables/columns if needed using SQL queries.
    - After running, write a concise, user-friendly explanation.
    - Do NOT write any tables/lists to the output.
    - Always include the exact SQL you ran.
    - Always use the full table name in query with db name and schema name.

    Available schema:
    {schema_text}
    """
        # LangGraph prebuilt ReAct agent
        execute_sql_tool = ReactDuckDBAgent._make_duckdb_tool(con)
        tools = [execute_sql_tool]
        agent = create_react_agent(
            llm,
            tools=tools,
            prompt=SYSTEM_PROMPT,
            response_format=AgentResponse,  # type: ignore[arg-type]
        )
        return agent

    def execute(self, messages: list[BaseMessage]) -> ExecutionResult:
        state = self._compiled_graph.invoke({"messages": messages})
        answer: AgentResponse = state["structured_response"]
        logger.info("Generated query: %s", answer["sql"])
        df = self._connection.execute(f"SELECT * FROM ({sql_strip(answer['sql'])}) t LIMIT {self._rows_limit}").df()
        return ExecutionResult(text=answer["explanation"], sql=answer["sql"], df=df)
