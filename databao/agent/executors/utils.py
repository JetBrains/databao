import math
from typing import Any

import pandas as pd
from _duckdb import DuckDBPyConnection

from databao.agent.duckdb.react_tools import execute_duckdb_sql
from databao.agent.executors.frontend.text_frontend import dataframe_to_markdown


def exception_to_string(e: Exception | str) -> str:
    if isinstance(e, str):
        return e
    return f"Exception Name: {type(e).__name__}. Exception Desc: {e}"


def trim_string_middle(
    content: str, max_length: int | None, sep: str = "[...trimmed...]", front_percentage: float = 0.7
) -> str:
    if max_length is None or len(content) <= max_length:
        return content
    take_front = max(0, math.ceil(max_length * front_percentage) - len(sep) // 2)
    take_end = max(0, max_length - take_front - len(sep))
    return content[:take_front] + sep + content[len(content) - take_end :]


def trim_dataframe_values(df: pd.DataFrame, max_cell_chars: int | None) -> pd.DataFrame:
    df_sanitized = df.copy()
    if max_cell_chars is None:
        return df_sanitized

    def trim_cell(val: Any) -> str:
        return trim_string_middle(str(val), max_cell_chars)

    for col, dtype in zip(df_sanitized.columns, df_sanitized.dtypes, strict=True):
        if not pd.api.types.is_object_dtype(dtype) and not pd.api.types.is_string_dtype(dtype):
            continue
        df_sanitized[col] = df_sanitized[col].apply(trim_cell)
    return df_sanitized


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
