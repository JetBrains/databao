from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

import duckdb
import sqlalchemy as sa
from duckdb import DuckDBPyConnection
from pandas import DataFrame

from portus.data.database_schema_types import ColumnSchema, DatabaseSchema, TableSchema


def execute(
    con: DuckDBPyConnection | sa.Connection, s: str, params: Any = None
) -> DuckDBPyConnection | sa.CursorResult[Any]:
    if isinstance(con, DuckDBPyConnection):
        return con.execute(s, parameters=params)
    else:
        return con.execute(sa.text(s), parameters=params)


def is_sqlalchemy_engine(obj: Any) -> bool:
    return isinstance(obj, sa.Engine) or (
        obj.__class__.__name__ == "Engine" and getattr(obj.__class__, "__module__", "").startswith("sqlalchemy.engine")
    )


def sqlalchemy_to_duckdb_mysql(sa_url: str, keep_query: bool = True) -> str:
    """
    Convert SQLAlchemy-style MySQL URL to DuckDB MySQL extension URI.

    Examples:
      mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam
      -> mysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam
    """
    # 1) Strip the SQLAlchemy driver (+pymysql, +mysqldb, etc.)
    #    Accept both 'mysql://' and 'mysql+driver://'
    if sa_url.startswith("mysql+"):
        sa_url = "mysql://" + sa_url.split("://", 1)[1]
    elif not sa_url.startswith("mysql://"):
        raise ValueError("Expected a MySQL URL starting with 'mysql://' or 'mysql+...'")

    # 2) Parse
    parts = urlsplit(sa_url)
    user = parts.username or ""
    pwd = parts.password or ""
    host = parts.hostname or ""
    port = parts.port
    path = parts.path or ""  # includes leading '/' if db is present
    query = parts.query if keep_query else ""

    # 3) Rebuild with proper quoting for user/pass
    auth = ""
    if user:
        auth = quote(user, safe="")
        if pwd:
            auth += ":" + quote(pwd, safe="")
        auth += "@"

    netloc = auth + host
    if port:
        netloc += f":{port}"

    return urlunsplit(("mysql", netloc, path, query, ""))


def register_duckdb_dialect(con: DuckDBPyConnection | sa.Connection, *, dialect: str, name: str, url: str) -> None:
    if dialect.startswith("postgres"):
        execute(con, "INSTALL postgres_scanner;")
        execute(con, "LOAD postgres_scanner;")
        execute(con, f"ATTACH '{url}' AS {name} (TYPE POSTGRES);")
    elif dialect.startswith(("mysql", "mariadb")):
        execute(con, "INSTALL mysql;")
        execute(con, "LOAD mysql;")
        mysql_url = sqlalchemy_to_duckdb_mysql(url)
        execute(con, f"ATTACH '{mysql_url}' AS {name} (TYPE MYSQL);")
    else:
        raise ValueError(f"Database engine '{dialect}' is not supported yet")


def register_sqlalchemy(con: DuckDBPyConnection | sa.Connection, sqlalchemy_engine: sa.Engine, name: str) -> None:
    url = sqlalchemy_engine.url.render_as_string(hide_password=False)
    dialect = getattr(getattr(sqlalchemy_engine, "dialect", None), "name", "")
    register_duckdb_dialect(con, dialect=dialect, name=name, url=url)


def init_duckdb_con(dbs: dict[str, Any], dfs: dict[str, DataFrame]) -> DuckDBPyConnection:
    con = duckdb.connect(database=":memory:", read_only=False)
    for name, db in dbs.items():
        if is_sqlalchemy_engine(db):
            register_sqlalchemy(con, db, name)
        else:
            raise ValueError(f"Connection type '{type(db)}' is not supported yet")

    for name, df in dfs.items():
        con.register(name, df)

    return con


def sql_strip(query: str) -> str:
    return query.strip().rstrip(";")


def list_inspectable_duckdb_tables(connection: DuckDBPyConnection | sa.Connection) -> list[tuple[str, str, str]]:
    rows = execute(
        connection,
        """
        SELECT table_catalog, table_schema, table_name
        FROM information_schema.tables
        WHERE table_type IN ('BASE TABLE', 'VIEW')
          AND table_schema NOT IN ('pg_catalog', 'pg_toast', 'information_schema')
        ORDER BY table_schema, table_name""",
    ).fetchall()
    return [(str(row[0]), str(row[1]), str(row[2])) for row in rows]


def inspect_duckdb_schema(
    connection: DuckDBPyConnection | sa.Connection, schema_prefix: str | None = None
) -> DatabaseSchema:
    inspectable_tables = list_inspectable_duckdb_tables(connection)
    if schema_prefix is not None:
        inspectable_tables = [
            (db, schema, table)
            for db, schema, table in inspectable_tables
            if f"{db}.{schema}".startswith(schema_prefix)
        ]

    table_schemas = {}
    for db, schema, table in inspectable_tables:
        cols_query = """
        SELECT column_name, data_type FROM information_schema.columns 
        WHERE table_schema = {} AND table_name = {} ORDER BY ordinal_position
        """.format(*(("?", "?") if isinstance(connection, DuckDBPyConnection) else (":schema", ":table")))
        col_rows = execute(
            connection,
            cols_query,
            [schema, table] if isinstance(connection, DuckDBPyConnection) else dict(schema=schema, table=table),
        ).fetchall()

        col_schemas = {}
        for col_name, col_type in col_rows:
            col_schema = ColumnSchema(name=col_name, dtype=col_type)
            col_schemas[col_name] = col_schema
        table_schema = TableSchema(name=table, schema_name=f"{db}.{schema}", columns=col_schemas)
        table_schemas[table_schema.qualified_name] = table_schema

    db_schema = DatabaseSchema(db_type="duckdb", tables=table_schemas)
    return db_schema
