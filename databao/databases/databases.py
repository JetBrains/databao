from typing import Any

from _duckdb import DuckDBPyConnection
from databao_context_engine import ConfiguredDatasource, DatasourceType

from databao.databases.database_adapter import DatabaseAdapter
from databao.databases.database_connection import DBConnectionConfig, DBConnectionRuntime
from databao.databases.duckdb_adapter import DuckDBAdapter
from databao.databases.mysql_adapter import MySQLAdapter
from databao.databases.postgresql_adapter import PostgreSQLAdapter
from databao.databases.snowflake_adapter import SnowflakeAdapter
from databao.databases.sqlite_adapter import SQLiteAdapter

DATABASE_ADAPTERS: list[DatabaseAdapter] = [
    DuckDBAdapter(),
    MySQLAdapter(),
    PostgreSQLAdapter(),
    SnowflakeAdapter(),
    SQLiteAdapter(),
]


def supported_db_types() -> list[DatasourceType]:
    return [adapter.type() for adapter in DATABASE_ADAPTERS]


# TODO (dce): use DCE config instead
def to_dce_config_content(config: DBConnectionConfig) -> dict[str, Any]:
    type = config.type
    content = config.content
    for adapter in DATABASE_ADAPTERS:
        if adapter.type() == type:
            main_property_keys = adapter.main_property_keys()
            additional_properties = {k: v for k, v in content.items() if k not in main_property_keys}
            main_properties = {k: v for k, v in content.items() if k in main_property_keys}
            main_properties["additional_properties"] = additional_properties
            return {"connection": main_properties}
    raise ValueError("Cannot convert DBConnectionConfig to DCE-format config")


# TODO (dce): use DCE config instead
def to_agent_config_content(dce_ds: ConfiguredDatasource) -> dict[str, Any]:
    dce_content = {str(k): v for k, v in dce_ds.config.items()}
    connection = dce_content.get("connection")
    if connection is None:
        raise ValueError("Cannot convert DCE config to Agent config: missing 'connection' key")
    connection = {str(k): v for k, v in connection.items()}
    additional_properties = connection.pop("additional_properties", {})
    return {**connection, **additional_properties}


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
