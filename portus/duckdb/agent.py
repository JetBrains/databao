import json
import logging
from typing import List, TypedDict

from duckdb import DuckDBPyConnection
from pandas import DataFrame
from langchain_core.messages import HumanMessage
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.tools import tool
from langgraph.prebuilt import create_react_agent

from portus.data_executor import DataExecutor, DataResult
from portus.duckdb.utils import init_duckdb_con, sql_strip

logger = logging.getLogger(__name__)


class AgentResponse(TypedDict):
    sql: str
    explanation: str


class SimpleDuckDBAgenticExecutor(DataExecutor):
    @staticmethod
    def __describe_duckdb_schema(con: DuckDBPyConnection, max_cols_per_table: int = 40) -> str:
        rows = con.execute("""
                           SELECT table_catalog, table_schema, table_name
                           FROM information_schema.tables
                           WHERE table_type IN ('BASE TABLE', 'VIEW')
                             AND table_schema NOT IN ('pg_catalog', 'pg_toast', 'information_schema')
                           ORDER BY table_schema, table_name
                           """).fetchall()

        lines: List[str] = []
        for db, schema, table in rows:
            cols = con.execute("""
                               SELECT column_name, data_type
                               FROM information_schema.columns
                               WHERE table_schema = ?
                                 AND table_name = ?
                               ORDER BY ordinal_position
                               """, [schema, table]).fetchall()
            if len(cols) > max_cols_per_table:
                cols = cols[:max_cols_per_table]
                suffix = " ... (truncated)"
            else:
                suffix = ""
            col_desc = ", ".join(f"{c} {t}" for c, t in cols)
            lines.append(f"{db}.{schema}.{table}({col_desc}){suffix}")
        return "\n".join(lines) if lines else "(no base tables found)"

    @staticmethod
    def __make_duckdb_tool(con: DuckDBPyConnection):
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
                payload = {"columns": [], "rows": [], "limit": limit,
                           "note": f"SQL error: {type(e).__name__}: {e}"}
                return json.dumps(payload)

        return execute_sql

    @staticmethod
    def __make_react_duckdb_agent(
            con: DuckDBPyConnection,
            llm: BaseChatModel
    ):
        execute_sql_tool = SimpleDuckDBAgenticExecutor.__make_duckdb_tool(con)
        tools = [execute_sql_tool]
        schema_text = SimpleDuckDBAgenticExecutor.__describe_duckdb_schema(con)

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
        # noinspection PyTypeChecker
        agent = create_react_agent(
            llm,
            tools=tools,
            prompt=SYSTEM_PROMPT,
            response_format=AgentResponse
        )

        # Convenience runner that returns explanation + DataFrame
        def ask(question: str) -> AgentResponse:
            # noinspection PyTypeChecker
            state = agent.invoke({"messages": [HumanMessage(content=question)]})
            return state["structured_response"]

        return agent, ask

    def execute(
            self,
            query: str,
            llm: BaseChatModel,
            dbs: dict[str, object],
            dfs: dict[str, DataFrame],
            *,
            rows_limit: int = 100
    ) -> DataResult:
        con = init_duckdb_con(dbs, dfs)
        agent, ask = self.__make_react_duckdb_agent(con, llm)
        answer: AgentResponse = ask(query)
        logger.info("Generated query: %s", answer["sql"])
        df = con.execute(f'SELECT * FROM ({sql_strip(answer["sql"])}) t LIMIT {rows_limit}').df()
        return DataResult(answer["explanation"], df, {})
