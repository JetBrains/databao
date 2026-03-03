from _duckdb import DuckDBPyConnection
from databao_context_engine import (
    DuckDBConnectionConfig,
    MySQLConnectionProperties,
    PostgresConnectionProperties,
    SnowflakeConnectionProperties,
    SQLiteConnectionConfig,
)
from databao_context_engine.plugins.databases.bigquery.config_file import BigQueryConnectionProperties
from sqlalchemy import Connection, Engine

DBConnectionConfig = \
    BigQueryConnectionProperties \
    | DuckDBConnectionConfig \
    | MySQLConnectionProperties \
    | PostgresConnectionProperties \
    | SnowflakeConnectionProperties \
    | SQLiteConnectionConfig

DBConnectionRuntime = DuckDBPyConnection | Engine | Connection

DBConnection = DBConnectionConfig | DBConnectionRuntime
