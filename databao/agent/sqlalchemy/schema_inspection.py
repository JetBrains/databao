import logging
from collections import defaultdict

from sqlalchemy import Connection, Engine, text

from databao.agent.duckdb.schema_inspection import ColumnInfo, TableInfo

_LOGGER = logging.getLogger(__name__)


def inspect_sqlalchemy_schema(conn: Engine | Connection) -> list[TableInfo]:
    """Inspect and return structured schema information from a SQLAlchemy connection."""
    if isinstance(conn, Engine):
        with conn.connect() as connection:
            return _inspect(connection)
    return _inspect(conn)


def _inspect(conn: Connection) -> list[TableInfo]:
    dialect = conn.engine.dialect.name
    if dialect.startswith("snowflake"):
        return _inspect_snowflake(conn)
    raise NotImplementedError(f"SQLAlchemy schema inspection not supported for dialect: {dialect!r}")


def _inspect_snowflake(conn: Connection) -> list[TableInfo]:
    table_rows = conn.execute(
        text("""
            SELECT table_catalog, table_schema, table_name
            FROM information_schema.tables
            WHERE table_type IN ('BASE TABLE', 'VIEW')
              AND table_schema != 'INFORMATION_SCHEMA'
            ORDER BY table_catalog, table_schema, table_name
        """)
    ).fetchall()

    if not table_rows:
        return []

    valid_tables: set[tuple[str, str, str]] = {(r[0], r[1], r[2]) for r in table_rows}

    col_rows = conn.execute(
        text("""
            SELECT table_catalog, table_schema, table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema != 'INFORMATION_SCHEMA'
            ORDER BY table_catalog, table_schema, table_name, ordinal_position
        """)
    ).fetchall()

    col_map: dict[tuple[str, str, str], list[ColumnInfo]] = defaultdict(list)
    for catalog, schema, table, col_name, data_type in col_rows:
        key = (catalog, schema, table)
        if key in valid_tables:
            col_map[key].append(ColumnInfo(name=col_name, data_type=data_type))

    result: list[TableInfo] = []
    for catalog, schema, table in table_rows:
        key = (catalog, schema, table)
        result.append(
            TableInfo(
                table_catalog=catalog,
                columns_catalog=catalog,
                schema=schema,
                name=table,
                columns=col_map[key],
            )
        )

    return result
