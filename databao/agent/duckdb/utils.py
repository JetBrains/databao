import pandas as pd
from duckdb import DuckDBPyConnection


def execute_duckdb_sql(sql: str, con: DuckDBPyConnection, *, limit: int | None = None) -> pd.DataFrame:
    # Use duckdb's Relation API to inject a LIMIT clause
    rel = con.sql(sql)  # A lazy Relation

    # TODO Do we want to forbid non-SELECT statements?
    # Non-Select queries (CREATE TABLE, etc.) are executed immediately and return None
    if rel is None:
        return pd.DataFrame()

    if limit is not None:
        rel = rel.limit(limit)
    return rel.df()  # Execute and return DataFrame
