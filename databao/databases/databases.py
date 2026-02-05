from _duckdb import DuckDBPyConnection
from databao_context_engine import DatasourceType

from databao.databases.database_adapter import DatabaseAdapter
from databao.databases.database_connection import DBConnectionConfig, DBConnectionRuntime
from databao.databases.duckdb_adapter import DuckDBAdapter
from databao.databases.mysql_adapter import MySQLAdapter
from databao.databases.postgresql_adapter import PostgreSQLAdapter
from databao.databases.snowflake_adapter import SnowflakeAdapter

DATABASE_ADAPTERS: list[DatabaseAdapter] = [
    DuckDBAdapter(),
    MySQLAdapter(),
    PostgreSQLAdapter(),
    SnowflakeAdapter(),
]


def supported_db_types() -> list[DatasourceType]:
    return [adapter.type() for adapter in DATABASE_ADAPTERS]


def convert_to_config(conn: DBConnectionRuntime) -> DBConnectionConfig:
    for adapter in DATABASE_ADAPTERS:
        if adapter.accept(conn):
            config = adapter.convert_to_config(conn)
            if config is None:
                break
            return config

    raise ValueError("Cannot convert connection to DBConnectionConfig")


def register_in_duckdb(shared: DuckDBPyConnection, conn: DBConnectionConfig, name: str) -> None:
    for adapter in DATABASE_ADAPTERS:
        if adapter.accept(conn):
            adapter.register_in_duckdb(shared, conn, name)
            return

    # raise ValueError("Cannot register connection in DuckDB")
