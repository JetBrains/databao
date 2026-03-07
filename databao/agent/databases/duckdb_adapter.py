from typing import Any

from _duckdb import DuckDBPyConnection
from databao_context_engine import DatasourceType, DuckDBConfigFile, DuckDBConnectionConfig
from databao_context_engine.pluginlib.build_plugin import AbstractConfigFile

from databao.agent.databases.database_adapter import DatabaseAdapter
from databao.agent.databases.database_connection import DBConnection, DBConnectionConfig, DBConnectionRuntime


class DuckDBAdapter(DatabaseAdapter):
    @classmethod
    def type(cls) -> DatasourceType:
        full_type = DuckDBConfigFile.model_fields["type"].default
        return DatasourceType(full_type=full_type)

    @classmethod
    def accept(cls, conn: DBConnection) -> bool:
        if isinstance(conn, DuckDBPyConnection):
            return True
        return isinstance(conn, DuckDBConnectionConfig)

    @classmethod
    def create_config_file(cls, config: DBConnectionConfig, name: str) -> AbstractConfigFile:
        if not isinstance(config, DuckDBConnectionConfig):
            raise ValueError(f"Invalid connection config type: expected DuckDBConnectionConfig, got {type(config)}.")
        return DuckDBConfigFile(connection=config, name=name)

    @classmethod
    def create_config_from_runtime(cls, run_conn: DBConnectionRuntime) -> DBConnectionConfig:
        if not isinstance(run_conn, DuckDBPyConnection):
            raise ValueError(f"Invalid runtime connection type: expected DuckDBPyConnection, got {type(run_conn)}.")
        database = cls._get_database(run_conn)
        if database is None:
            raise RuntimeError("Memory-based DuckDB is not supported.")

        run_conn.close()
        return DuckDBConnectionConfig(database_path=database)

    @classmethod
    def create_config_from_content(cls, content: dict[str, Any]) -> DBConnectionConfig:
        config_file = DuckDBConfigFile.model_validate({"name": "", **content})
        return config_file.connection

    @classmethod
    def register_in_duckdb(cls, shared_conn: DuckDBPyConnection, config: DBConnectionConfig, name: str) -> None:
        if not isinstance(config, DuckDBConnectionConfig):
            raise ValueError(f"Invalid connection config type: expected DuckDBConnectionConfig, got {type(config)}.")
        shared_conn.execute(f"ATTACH '{config.database_path}' AS \"{name}\" (READ_ONLY);")

    @staticmethod
    def _get_database(conn: DuckDBPyConnection) -> str | None:
        """Get the database file path for DuckDB connection, or None if in-memory."""
        database = conn.execute("PRAGMA database_list").fetchone()
        if database is None:
            return None
        database = database[2]
        return None if database == "memory" else database
