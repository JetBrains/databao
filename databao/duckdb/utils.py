import re
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

from duckdb import DuckDBPyConnection


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


def register_sqlalchemy(con: DuckDBPyConnection, sqlalchemy_engine: Any, name: str) -> None:
    """Attach an external DB to DuckDB using an existing SQLAlchemy engine.

    Supports PostgreSQL and MySQL/MariaDB (via DuckDB extensions). The external
    database becomes available under the given `name` within the DuckDB connection.
    """
    url = sqlalchemy_engine.url.render_as_string(hide_password=False)
    dialect = getattr(getattr(sqlalchemy_engine, "dialect", None), "name", "")
    if dialect.startswith("postgres"):
        con.execute("INSTALL postgres_scanner;")
        con.execute("LOAD postgres_scanner;")
        con.execute(f"ATTACH '{url}' AS {name} (TYPE POSTGRES);")
    elif dialect.startswith(("mysql", "mariadb")):
        con.execute("INSTALL mysql;")
        con.execute("LOAD mysql;")
        mysql_url = sqlalchemy_to_duckdb_mysql(str(url))
        con.execute(f"ATTACH '{mysql_url}' AS {name} (TYPE MYSQL);")
    elif dialect.startswith("sqlite"):
        con.execute("INSTALL sqlite;")
        con.execute("LOAD sqlite;")
        sqlite_path = re.sub("^sqlite:///", "", url)
        con.execute(f"ATTACH '{sqlite_path}' AS {name} (TYPE SQLITE);")
        con.execute(f"USE {name};")
    else:
        raise ValueError(f"Database engine '{sqlalchemy_engine.dialect.name}' is not supported yet")


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
