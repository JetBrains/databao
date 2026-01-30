from typing import Any

import pandas as pd


def db_introspect(db_conn: Any) -> pd.DataFrame:
    """
    Introspect a DuckDB database and return columns metadata.

    Args:
        db_path: Path to .duckdb database file. If None, uses an in-memory connection.

    Returns:
        pandas.DataFrame with:
            schema, table, column_name, data_type, is_nullable, column_default,
            column_index (1-based), is_primary_key (bool).
    """
    try:
        # Gather column metadata
        cols_query = """
        WITH cols AS (
            SELECT
                table_schema AS schema,
                table_name AS table,
                column_name,
                data_type,
                is_nullable,
                column_default,
                ordinal_position AS column_index
            FROM information_schema.columns
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        ),
        pks AS (
            SELECT
                tc.table_schema AS schema,
                tc.table_name AS table,
                kcu.column_name,
                TRUE AS is_primary_key
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
             AND tc.table_name = kcu.table_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
        )
        SELECT
            c.table,
            c.column_name,
            c.data_type,
            c.is_nullable,
            c.column_default,
            c.column_index,
            COALESCE(p.is_primary_key, FALSE) AS is_primary_key
        FROM cols c
        LEFT JOIN pks p
          ON c.schema = p.schema
         AND c.table = p.table
         AND c.column_name = p.column_name
        ORDER BY c.schema, c.table, c.column_index;
        """

        df = db_conn.execute(cols_query).df()
        return df
    finally:
        db_conn.close()
