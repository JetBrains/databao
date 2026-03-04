from typing import Any

from _duckdb import DuckDBPyConnection
from databao_context_engine.pluginlib.build_plugin import AbstractConfigFile, DatasourceType

from databao.databases.bigquery_adapter import BigQueryAdapter
from databao.databases.database_adapter import DatabaseAdapter
from databao.databases.database_connection import (
    DBConnectionConfig,
    DBConnectionRuntime,
)
from databao.databases.duckdb_adapter import DuckDBAdapter
from databao.databases.mysql_adapter import MySQLAdapter
from databao.databases.postgresql_adapter import PostgreSQLAdapter
from databao.databases.snowflake_adapter import SnowflakeAdapter
from databao.databases.sqlite_adapter import SQLiteAdapter

DATABASE_ADAPTERS: list[DatabaseAdapter] = [
    BigQueryAdapter(),
    DuckDBAdapter(),
    MySQLAdapter(),
    PostgreSQLAdapter(),
    SnowflakeAdapter(),
    SQLiteAdapter(),
]


def db_type(config: DBConnectionConfig) -> DatasourceType:
    for adapter in DATABASE_ADAPTERS:
        if adapter.accept(config):
            return adapter.type()
    raise ValueError(f"Cannot determine database type for connection config of type {type(config)}.")


def create_db_config_file(config: DBConnectionConfig, name: str) -> AbstractConfigFile:
    for adapter in DATABASE_ADAPTERS:
        if adapter.accept(config):
            return adapter.create_config_file(config, name)
    raise ValueError(f"Cannot create config file for connection config of type {type(config)}.")


def create_db_config_from_runtime(run_conn: DBConnectionRuntime) -> DBConnectionConfig:
    for adapter in DATABASE_ADAPTERS:
        if adapter.accept(run_conn):
            return adapter.create_config_from_runtime(run_conn)
    raise ValueError(f"Cannot create config for runtime connection of type {type(run_conn)}.")


def try_create_db_config_from_content(type: DatasourceType, content: dict[str, Any]) -> DBConnectionConfig | None:
    for adapter in DATABASE_ADAPTERS:
        if adapter.type() == type:
            return adapter.create_config_from_content(content)
    return None


def register_db_in_duckdb(shared_conn: DuckDBPyConnection, config: DBConnectionConfig, name: str) -> None:
    for adapter in DATABASE_ADAPTERS:
        if adapter.accept(config):
            adapter.register_in_duckdb(shared_conn, config, name)
            return
    raise ValueError(f"Cannot register connection for config type {type(config)} in DuckDB.")
