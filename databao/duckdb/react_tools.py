import json
from typing import Any

import pandas as pd
import sqlglot
from duckdb import DuckDBPyConnection
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.tools import tool
from langgraph.graph.state import CompiledStateGraph
from langgraph.prebuilt import create_react_agent
from pydantic import BaseModel

from databao.duckdb.utils import describe_duckdb_schema


class AgentResponse(BaseModel):
    """Response model for ReAct DuckDB agent."""

    sql: str
    explanation: str


def sql_strip(query: str) -> str:
    """Strip whitespace and trailing semicolons from SQL query."""
    return query.strip().rstrip(";")


def sql_with_limit(sql: str, limit: int) -> str:
    """Ensure the SQL has a LIMIT clause, appending one if missing."""
    # TODO Change the limit value if limit is already present in the query?
    sql_to_run = sql_strip(sql)
    ast = sqlglot.parse_one(sql_to_run, dialect="duckdb")
    if not ast.args.get("limit") and isinstance(ast, sqlglot.expressions.Query):
        ast_with_limit = ast.limit(limit)  # Add outer LIMIT clause
        sql_to_run = ast_with_limit.sql()
    return sql_to_run


def execute_duckdb_sql(sql: str, con: DuckDBPyConnection, *, limit: int | None = None) -> pd.DataFrame:
    sql_to_run = sql_strip(sql)
    if limit is not None:
        sql_to_run = sql_with_limit(sql_to_run, limit)
    return con.execute(sql_to_run).df()


def make_duckdb_tool(con: DuckDBPyConnection) -> Any:
    """
    Create a DuckDB SQL execution tool for LangChain agents.

    Args:
        con: DuckDB connection to execute queries against.

    Returns:
        A LangChain tool that executes SQL queries.
    """

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
        try:
            df = execute_duckdb_sql(sql, con, limit=limit)
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


def make_react_duckdb_agent(con: DuckDBPyConnection, llm: BaseChatModel) -> CompiledStateGraph[Any]:
    """
    Create a ReAct agent configured to work with DuckDB.

    Args:
        con: DuckDB connection to execute queries against.
        llm: Language model to use for the agent.

    Returns:
        A compiled LangGraph ReAct agent.
    """
    schema_text = describe_duckdb_schema(con)
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
    execute_sql_tool = make_duckdb_tool(con)
    tools = [execute_sql_tool]
    agent = create_react_agent(
        llm,
        tools=tools,
        prompt=SYSTEM_PROMPT,
        response_format=AgentResponse,
    )
    return agent
