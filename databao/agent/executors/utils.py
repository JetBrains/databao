import math
from typing import Any

import pandas as pd
from _duckdb import DuckDBPyConnection
from databao_context_engine import ContextSearchResult
from langchain_core.language_models import BaseChatModel

from databao.agent.core.domain import _DCEProjectDomain
from databao.agent.duckdb.utils import execute_duckdb_sql
from databao.agent.executors.frontend.text_frontend import dataframe_to_markdown
from databao.agent.executors.query_expansion import QueryExpansionConfig, expand_queries, reciprocal_rank_fusion
from databao.agent.integrations.dce import DatabaoContextApi


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


def search_context(retrieve_text: str, *, domain: _DCEProjectDomain) -> list[dict[str, Any]]:
    """Search the context for relevant information matching the given query text.
    Args:
        retrieve_text: Natural language query to search the context for relevant results.
        domain: The domain object to use to search the context.
    """
    search_result_list = domain.search_context(retrieve_text, datasource_name=None)
    return list(map(_search_result_to_dict, search_result_list))


def search_context_with_query_expansion(
    retrieve_text: str,
    *,
    domain: _DCEProjectDomain,
    expansion_llm: BaseChatModel,
    expansion_config: QueryExpansionConfig,
    datasource_name: str | None = None,
    datasource_type: str | None = None,
) -> list[dict[str, Any]]:
    """Search the context for relevant information matching the given query text.

    Internally expands the query into multiple retrieval-friendly variants adapted
    to the datasource naming conventions, then merges results via rank fusion.

    Args:
        retrieve_text: Natural language query to search the context for relevant results.
        domain: The domain object to use to search the context.
        expansion_llm: The llm used to expand the query.
        expansion_config: The configuration for query expansion.
        datasource_name: Optional datasource name to restrict the search to a specific data source.
        datasource_type: Optional datasource type hint (e.g. "dbt", "snowflake", "postgres").
            Used to adapt query expansion to the naming conventions of the target system.
    """
    queries = expand_queries(
        retrieve_text,
        expansion_llm,
        expansion_config,
        datasource_type=datasource_type or "sql",
    )

    ranked_lists: list[list[dict[str, Any]]] = []
    for q in queries:
        results = domain.search_context(q, datasource_name=datasource_name)
        ranked_lists.append(list(map(_search_result_to_dict, results)))

    if len(ranked_lists) <= 1:
        return ranked_lists[0] if ranked_lists else []

    return reciprocal_rank_fusion(ranked_lists, k=expansion_config.rrf_k)


def _search_result_to_dict(search_result: ContextSearchResult) -> dict[str, Any]:
    return {
        "data_source_name": _get_ds_name(search_result),
        "score": search_result.score,
        "context_result": search_result.context_result,
    }


def _get_ds_name(search_result: ContextSearchResult) -> str:
    ds_id = search_result.datasource_id
    return DatabaoContextApi.get_datasource_name(ds_id)
