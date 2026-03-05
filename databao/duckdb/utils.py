import logging
from collections import defaultdict

import duckdb
from duckdb import DuckDBPyConnection

_LOGGER = logging.getLogger(__name__)


def _inspect_columns(
    con: DuckDBPyConnection, db_qualifier: str, schemas: list[str], tables: list[str]
) -> list[tuple[str, str, str, list[str], list[str]]]:
    db = f'"{db_qualifier}".' if db_qualifier else ""
    return con.execute(
        f"""
            SELECT table_catalog,
                   table_schema,
                   table_name,
                   LIST(column_name) AS columns,
                   LIST(data_type) AS data_types
            FROM {db}information_schema.columns
            WHERE table_schema in ?
                AND table_name in ?
            GROUP BY table_catalog, table_schema, table_name
            ORDER BY table_catalog, table_schema, table_name""",
        [schemas, tables],
    ).fetchall()


def describe_duckdb_schema(
    con: DuckDBPyConnection, max_cols_per_table: int | None = None, include_original_catalog_name: bool = False
) -> str:
    """Return a compact textual description of tables and columns in DuckDB.

    Args:
        con: An open DuckDB connection.
        max_cols_per_table: Truncate column lists longer than this.
    """
    try:
        internal_db_mapping: dict[str, list[set[str]]] = defaultdict(lambda: [set(), set()])
        rows = con.execute("""
                           SELECT table_catalog, table_schema, table_name
                           FROM information_schema.tables
                           WHERE table_type IN ('BASE TABLE', 'VIEW')
                             AND table_schema NOT ILIKE 'pg_catalog'
                              AND table_schema NOT ILIKE 'pg_toast'
                              AND table_schema NOT ILIKE 'information_schema'
                           ORDER BY table_catalog, table_schema, table_name
                           """).fetchall()
        for db, schema, table in rows:
            internal_db_mapping[db][0].add(schema)
            internal_db_mapping[db][1].add(table)

        lines: list[str] = []
        for db, (schemas, tables) in internal_db_mapping.items():
            # Dataframes are loaded within `temp.main` and their columns can only be accessed
            # directly from information_schema.columns. Similarly, for attached sqlite databases,
            # we need to inspect information_schema.columns directly without the catalog name.
            # However, for Snowflake, we need to inspect from the correct catalog, otherwise we don't find any columns.
            db_qualifier = db if db != "temp" else ""
            try:
                cols = _inspect_columns(con, db_qualifier, list(schemas), list(tables))
            except duckdb.CatalogException as e:
                _LOGGER.debug(f"Failed to fetch schema using {db_qualifier=}: {e}")
                if db_qualifier == "":
                    raise
                # Fallback (for sqlite)
                cols = _inspect_columns(con, "", list(schemas), list(tables))

            for catalog, schema, table, columns, data_types in cols:
                if max_cols_per_table is not None and len(columns) > max_cols_per_table:
                    remaining_cols = len(columns) - max_cols_per_table
                    columns = columns[:max_cols_per_table]
                    data_types = data_types[:max_cols_per_table]
                    suffix = f", ... (truncated {remaining_cols} remaining columns)"
                else:
                    suffix = ""
                col_desc = ", ".join(f"{c}: {t}" for c, t in zip(columns, data_types, strict=False))
                catalog_name = f"{catalog}." if include_original_catalog_name else ""
                lines.append(f"{db}.{catalog_name}{schema}.{table}({col_desc}{suffix})")
    except Exception as e:
        _LOGGER.warning(f"Failed to fetch schema: {e}")
        return "(failed to fetch schema)"
    return "\n".join(lines) if lines else "(no base tables found)"
