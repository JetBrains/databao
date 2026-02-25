from _duckdb import DuckDBPyConnection
from databao_context_engine import (
    DuckDBConnectionConfig,
    MySQLConnectionProperties,
    PostgresConnectionProperties,
    SnowflakeConnectionProperties,
    SQLiteConnectionConfig,
)
from sqlalchemy import Connection, Engine

DBConnectionConfig = \
    DuckDBConnectionConfig \
    | MySQLConnectionProperties \
    | PostgresConnectionProperties \
    | SnowflakeConnectionProperties \
    | SQLiteConnectionConfig

DBConnectionRuntime = DuckDBPyConnection | Engine | Connection

DBConnection = DBConnectionConfig | DBConnectionRuntime
