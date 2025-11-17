import re
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

from duckdb import DuckDBPyConnection
from sqlalchemy import URL, Engine


def get_db_path(conn: Any) -> str | None:
    """Get the database file path for DuckDB connection, or None if in-memory."""
    if isinstance(conn, DuckDBPyConnection):
        db_path = conn.execute("PRAGMA database_list").fetchone()
        if db_path is None:
            return None
        db_path = db_path[2]
        return None if db_path == "memory" else db_path
    return None


def describe_duckdb_schema(con: DuckDBPyConnection, max_cols_per_table: int = 40) -> str:
    """Return a compact textual description of tables and columns in DuckDB.

    Args:
        con: An open DuckDB connection.
        max_cols_per_table: Truncate column lists longer than this.
    """
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
    return "\n".join(lines) if lines else "(no base tables found)"


def register_sqlalchemy(con: DuckDBPyConnection, sqlalchemy_engine: Engine, name: str) -> None:
    """Attach an external DB to DuckDB using an existing SQLAlchemy engine.

    Supports PostgreSQL and MySQL/MariaDB (via DuckDB extensions). The external
    database becomes available under the given `name` within the DuckDB connection.
    """
    sa_url = sqlalchemy_engine.url.render_as_string(hide_password=False)
    dialect = getattr(getattr(sqlalchemy_engine, "dialect", None), "name", "")
    if dialect.startswith("postgres"):
        con.execute("INSTALL postgres;")
        con.execute("LOAD postgres;")
        pg_url = sqlalchemy_to_postgres_url(sqlalchemy_engine.url)
        con.execute(f"ATTACH '{pg_url}' AS {name} (TYPE POSTGRES);")
    elif dialect.startswith(("mysql", "mariadb")):
        con.execute("INSTALL mysql;")
        con.execute("LOAD mysql;")
        mysql_url = sqlalchemy_to_duckdb_mysql(sa_url)
        con.execute(f"ATTACH '{mysql_url}' AS {name} (TYPE MYSQL);")
    elif dialect.startswith("sqlite"):
        con.execute("INSTALL sqlite;")
        con.execute("LOAD sqlite;")
        sqlite_path = re.sub("^sqlite:///", "", sa_url)
        con.execute(f"ATTACH '{sqlite_path}' AS {name} (TYPE SQLITE);")
    else:
        raise ValueError(f"Database engine '{dialect}' is not supported yet")


def sqlalchemy_to_postgres_url(url: URL) -> str:
    """Convert SQLAlchemy-style PostgreSQL URL to a PostgreSQL URI."""
    # https://docs.sqlalchemy.org/en/20/core/engines.html#postgresql
    # https://duckdb.org/docs/1.3/core_extensions/postgres#configuration
    # https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING-URIS
    new_url = url.set(drivername=url.drivername.split("+")[0])  # Remove the +driver part
    return new_url.render_as_string(hide_password=False)


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
