import logging
from collections import defaultdict
from dataclasses import dataclass, field

import duckdb
from duckdb import DuckDBPyConnection

_LOGGER = logging.getLogger(__name__)


@dataclass(kw_only=True)
class ColumnInfo:
    name: str
    data_type: str


@dataclass(kw_only=True)
class TableInfo:
    table_catalog: str  # table_catalog from information_schema.tables
    columns_catalog: str  # table_catalog from table_catalog.information_schema.columns
    schema: str
    name: str
    columns: list[ColumnInfo] = field(default_factory=list)

    def fully_qualified_name(self, use_original_catalog_name: bool) -> str:
        catalog_prefix = f"{self.columns_catalog}." if use_original_catalog_name else ""
        return f"{self.table_catalog}.{catalog_prefix}{self.schema}.{self.name}"


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


def inspect_duckdb_schema(con: DuckDBPyConnection) -> list[TableInfo]:
    """Inspect and return structured schema information from DuckDB."""
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

    result: list[TableInfo] = []
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
            result.append(
                TableInfo(
                    table_catalog=db,
                    columns_catalog=catalog,
                    schema=schema,
                    name=table,
                    columns=[ColumnInfo(name=c, data_type=t) for c, t in zip(columns, data_types, strict=True)],
                )
            )

    return result


def summarize_duckdb_schema(
    tables: list[TableInfo],
    max_cols_per_table: int | None = None,
    include_original_catalog_name: bool = False,
) -> str:
    """Format structured schema information into a compact textual description."""
    lines: list[str] = []
    for table in tables:
        columns = table.columns
        suffix = ""
        if max_cols_per_table is not None and len(columns) > max_cols_per_table:
            remaining = len(columns) - max_cols_per_table
            columns = columns[:max_cols_per_table]
            if max_cols_per_table <= 0:
                suffix = f"with {remaining} columns"
            else:
                suffix = f", ... truncated {remaining} remaining columns"
        col_desc = ", ".join(f"{c.name}: {c.data_type}" for c in columns)
        col_desc = f"({col_desc}{suffix})" if len(columns) > 0 else f" ({suffix})"
        table_name = table.fully_qualified_name(include_original_catalog_name)
        lines.append(f"{table_name}{col_desc}")
    return "\n".join(lines)


def summarize_duckdb_schema_overview(
    tables: list[TableInfo],
    include_original_catalog_name: bool = False,
) -> str:
    """Summarize schema as catalog.schema with table counts, without listing individual tables."""
    counts: dict[tuple[str, str, str], int] = defaultdict(int)
    for table in tables:
        # NB. We assume a one-to-one correspondence between table_catalog and columns_catalog
        counts[(table.table_catalog, table.columns_catalog, table.schema)] += 1
    lines: list[str] = []
    for (db, columns_catalog, schema), count in counts.items():
        catalog_prefix = f"{columns_catalog}." if include_original_catalog_name else ""
        lines.append(f"{db}.{catalog_prefix}{schema} ({count} tables)")
    return "\n".join(lines)
