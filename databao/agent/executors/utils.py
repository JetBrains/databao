from typing import Any

from _duckdb import DuckDBPyConnection

from databao.agent.duckdb.react_tools import execute_duckdb_sql
from databao.agent.executors.frontend.text_frontend import dataframe_to_markdown
from databao.agent.executors.lighthouse.graph import trim_dataframe_values, exception_to_string


def run_sql_query(
    sql: str, con: DuckDBPyConnection, sql_row_limit: int | None, display_row_limit: int, display_cell_char_limit: int
) -> dict[str, Any]:
    """
    Run a SELECT SQL query in the database. Returns the first 12 rows in csv format.

    Args:
        sql: SQL query
        con: DuckDB connection
        sql_row_limit: Maximum number of rows to return from SQL query
        display_row_limit: Maximum number of rows to display in output
        display_cell_char_limit: Maximum number of characters to display in each cell of the output table
    """
    try:
        df = execute_duckdb_sql(sql, con, limit=sql_row_limit)

        # Limit the size of sampled values to show to avoid context size explosions (e.g., json/binary blobs)
        df_display = df.head(display_row_limit)
        df_display = trim_dataframe_values(df_display, max_cell_chars=display_cell_char_limit)

        df_csv = df_display.to_csv(index=False)
        df_markdown = dataframe_to_markdown(df_display, index=False)
        if len(df) > display_row_limit:
            df_csv += f"\nResult is truncated from {len(df)} to {display_row_limit} rows."
            df_markdown += f"\nResult is truncated from {len(df)} to {display_row_limit} rows."
        return {"df": df, "sql": sql, "csv": df_csv, "markdown": df_markdown}
    except Exception as e:
        return {"error": exception_to_string(e)}
