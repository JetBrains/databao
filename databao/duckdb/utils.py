import logging
from collections import defaultdict

from duckdb import DuckDBPyConnection

_LOGGER = logging.getLogger(__name__)


def describe_duckdb_schema(con: DuckDBPyConnection, max_cols_per_table: int | None = None) -> str:
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
                            ORDER BY table_schema, table_name
                            """).fetchall()
        for db, schema, table in rows:
            internal_db_mapping[db][0].add(schema)
            internal_db_mapping[db][1].add(table)

        lines: list[str] = []
        for db, (schemas, tables) in internal_db_mapping.items():
            # dataframes are loaded within the `temp` database and their columns
            # can only be accessed directly from information_schema.columns
            db_qualifier = f"{db}." if db not in {"temp"} else ""

            cols = con.execute(
                f"""
                                SELECT table_schema,
                                       table_name,
                                       LIST(column_name) AS columns,
                                       LIST(data_type) AS data_types
                                FROM {db_qualifier}information_schema.columns
                                WHERE table_schema in ?
                                    AND table_name in ?
                                group by table_schema, table_name
                                """,
                [list(schemas), list(tables)],
            ).fetchall()

            for schema, table, columns, data_types in cols:
                if max_cols_per_table is not None and len(columns) > max_cols_per_table:
                    remaining_cols = len(columns) - max_cols_per_table
                    columns = columns[:max_cols_per_table]
                    data_types = data_types[:max_cols_per_table]
                    suffix = f", ... (truncated {remaining_cols} remaining columns)"
                else:
                    suffix = ""
                col_desc = ", ".join(f"{c}: {t}" for c, t in zip(columns, data_types, strict=False))
                lines.append(f"{db}.{schema}.{table}({col_desc}{suffix})")
    except Exception as e:
        _LOGGER.warning(f"Failed to fetch schema: {e}")
        return "(failed to fetch schema)"
    return "\n".join(lines) if lines else "(no base tables found)"
