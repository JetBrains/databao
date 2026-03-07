from typing import Any

from _duckdb import DuckDBPyConnection
from databao_context_engine import DatasourceType, SQLiteConfigFile, SQLiteConnectionConfig
from databao_context_engine.pluginlib.build_plugin import AbstractConfigFile
from sqlalchemy import Connection, Engine, make_url

from databao.agent.databases.database_adapter import DatabaseAdapter
from databao.agent.databases.database_connection import DBConnection, DBConnectionConfig, DBConnectionRuntime


class SQLiteAdapter(DatabaseAdapter):
    @classmethod
    def type(cls) -> DatasourceType:
        full_type = SQLiteConfigFile.model_fields["type"].default
        return DatasourceType(full_type=full_type)

    @classmethod
    def accept(cls, conn: DBConnection) -> bool:
        if isinstance(conn, (Engine, Connection)):
            dialect = conn.dialect
            return dialect.name.startswith("sqlite")
        return isinstance(conn, SQLiteConnectionConfig)

    @classmethod
    def create_config_file(cls, config: DBConnectionConfig, name: str) -> AbstractConfigFile:
        if not isinstance(config, SQLiteConnectionConfig):
            raise ValueError(f"Invalid connection config type: expected SQLiteConnectionConfig, got {type(config)}.")
        return SQLiteConfigFile(connection=config, name=name)

    @classmethod
    def create_config_from_runtime(cls, run_conn: DBConnectionRuntime) -> DBConnectionConfig:
        if not isinstance(run_conn, (Engine, Connection)):
            raise ValueError(
                f"Invalid runtime connection type: expected SQLAlchemy Engine or Connection, got {type(run_conn)}."
            )

        engine = run_conn if isinstance(run_conn, Engine) else run_conn.engine
        dialect = engine.dialect
        if not dialect.name.startswith("sqlite"):
            raise ValueError(f'Invalid runtime connection dialect: expected "sqlite", got "{dialect.name}".')

        sa_url_str = engine.url.render_as_string(hide_password=False)
        sa_url = make_url(sa_url_str)
        database = sa_url.database
        if database == ":memory:":
            raise RuntimeError("Memory-based SQLite is not supported.")

        return SQLiteConnectionConfig(database_path=database)

    @classmethod
    def create_config_from_content(cls, content: dict[str, Any]) -> DBConnectionConfig:
        config_file = SQLiteConfigFile.model_validate({"name": "", **content})
        return config_file.connection

    @classmethod
    def register_in_duckdb(cls, shared_conn: DuckDBPyConnection, config: DBConnectionConfig, name: str) -> None:
        if not isinstance(config, SQLiteConnectionConfig):
            raise ValueError(f"Invalid connection config type: expected SQLiteConnectionConfig, got {type(config)}.")
        shared_conn.execute(f"ATTACH '{config.database_path}' AS \"{name}\" (TYPE SQLITE);")
