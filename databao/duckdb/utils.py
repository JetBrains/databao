from duckdb import DuckDBPyConnection


def describe_duckdb_schema(con: DuckDBPyConnection, max_cols_per_table: int = 40) -> str:
    """Return a compact textual description of tables and columns in DuckDB.

    Args:
        con: An open DuckDB connection.
        max_cols_per_table: Truncate column lists longer than this.
    """
    try:
        rows = con.execute("""
                            SELECT table_catalog, table_schema, table_name
                            FROM information_schema.tables
                            WHERE table_type IN ('BASE TABLE', 'VIEW')
                                AND table_schema NOT IN ('pg_catalog', 'pg_toast', 'information_schema')
                            ORDER BY table_schema, table_name
                            """).fetchall()

        lines: list[str] = []
        for db, schema, table in rows:
            cols = con.execute(
                """
                                SELECT column_name, data_type
                                FROM information_schema.columns
                                WHERE table_schema = ?
                                    AND table_name = ?
                                ORDER BY ordinal_position
                                """,
                [schema, table],
            ).fetchall()
            if len(cols) > max_cols_per_table:
                cols = cols[:max_cols_per_table]
                suffix = " ... (truncated)"
            else:
                suffix = ""
            col_desc = ", ".join(f"{c} {t}" for c, t in cols)
            lines.append(f"{db}.{schema}.{table}({col_desc}){suffix}")
    except Exception:
        return "(failed to fetch schema)"
    return "\n".join(lines) if lines else "(no base tables found)"
