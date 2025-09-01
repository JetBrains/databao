from typing import Any
import importlib
from pandas import DataFrame
import duckdb
from duckdb import DuckDBPyConnection


def init_duckdb_con(dbs: dict[str, Any], dfs: dict[str, DataFrame]) -> DuckDBPyConnection:
    # Loading DB engines dynamically to have NO dependencies on all db-related packages
    try:
        engine_mod = importlib.import_module("sqlalchemy.engine")
        engine_class = getattr(engine_mod, "Engine")
    except ModuleNotFoundError:
        raise RuntimeError(
            "SQLAlchemy is not installed. "
            "Install with `pip install sqlalchemy`."
        )

    for name, engine in dbs.items():
        if not isinstance(engine, engine_class):
            raise TypeError("portus expects a SQLAlchemy Engine instance")

    con = duckdb.connect(database=':memory:', read_only=False)
    # TODO(artem.trofimov): support other DBs
    con.execute("INSTALL postgres_scanner;")
    con.execute("LOAD postgres_scanner;")

    for name, engine in dbs.items():
        url = engine.url
        pg_conn = (
            f"dbname={url.database} "
            f"user={url.username} "
            f"password={url.password or ''} "
            f"host={url.host} "
            f"port={url.port or 5432} "
            f"sslmode=require"
        )
        con.execute(f"ATTACH '{pg_conn}' AS {name} (TYPE POSTGRES);")

    for name, df in dfs.items():
        con.register(name, df)

    return con


def sql_strip(query: str) -> str:
    return query.strip().rstrip(";")
