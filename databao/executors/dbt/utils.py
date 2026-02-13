from contextlib import suppress
from typing import Any

import pandas as pd


def db_introspect(db_conn: Any) -> pd.DataFrame:
    """
    Introspect a DuckDB database and return columns metadata across ALL attached databases.

    DuckDB's information_schema is scoped to the current database, so we iterate
    over every non-internal attached database, USE it, and collect its columns.

    Returns:
        pandas.DataFrame with:
            catalog, schema, table, column_name, data_type, is_nullable, column_default,
            column_index (1-based), is_primary_key (bool), fully_qualified_name.
    """
    result_columns = [
        "catalog",
        "schema",
        "table",
        "fully_qualified_name",
        "column_name",
        "data_type",
        "is_nullable",
        "column_default",
        "column_index",
        "is_primary_key",
    ]

    # Discover all user-attached databases
    databases = db_conn.execute("""
        SELECT database_name
        FROM duckdb_databases()
        WHERE NOT internal
          AND database_name != 'memory'
    """).fetchall()

    if not databases:
        return pd.DataFrame(columns=result_columns)

    cols_query = """
    WITH cols AS (
        SELECT
            table_catalog AS catalog,
            table_schema AS schema,
            table_name AS "table",
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
            tc.table_catalog AS catalog,
            tc.table_schema AS schema,
            tc.table_name AS "table",
            kcu.column_name,
            TRUE AS is_primary_key
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_catalog = kcu.table_catalog
         AND tc.table_schema = kcu.table_schema
         AND tc.table_name = kcu.table_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
    )
    SELECT
        c.catalog,
        c.schema,
        c."table",
        c.catalog || '.' || c.schema || '.' || c."table" AS fully_qualified_name,
        c.column_name,
        c.data_type,
        c.is_nullable,
        c.column_default,
        c.column_index,
        COALESCE(p.is_primary_key, FALSE) AS is_primary_key
    FROM cols c
    LEFT JOIN pks p
      ON c.catalog = p.catalog
     AND c.schema = p.schema
     AND c."table" = p."table"
     AND c.column_name = p.column_name
    ORDER BY c.catalog, c.schema, c."table", c.column_index;
    """

    original_db = db_conn.execute("SELECT current_database()").fetchone()[0]
    frames: list[pd.DataFrame] = []

    for (db_name,) in databases:
        try:
            db_conn.execute(f'USE "{db_name}"')
            df = db_conn.execute(cols_query).df()
            if not df.empty:
                frames.append(df)
        except Exception:
            continue

    # Restore original database context
    with suppress(Exception):
        db_conn.execute(f'USE "{original_db}"')

    if not frames:
        return pd.DataFrame(columns=result_columns)

    return pd.concat(frames, ignore_index=True)
