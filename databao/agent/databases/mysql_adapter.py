from typing import Any

from _duckdb import DuckDBPyConnection
from databao_context_engine import DatasourceType, MySQLConfigFile, MySQLConnectionProperties
from databao_context_engine.pluginlib.build_plugin import AbstractConfigFile
from sqlalchemy import URL, Connection, Engine, make_url

from databao.agent.databases.database_adapter import DatabaseAdapter
from databao.agent.databases.database_connection import DBConnection, DBConnectionConfig, DBConnectionRuntime
from databao.agent.databases.utils import str_dict

USER_KEY = "user"
PASSWORD_KEY = "password"
HOST_KEY = "host"
PORT_KEY = "port"
DATABASE_KEY = "database"

CLIENT_FLAG_KEY = "client_flag"

MAIN_KEYS = {USER_KEY, PASSWORD_KEY, HOST_KEY, PORT_KEY, DATABASE_KEY}

IGNORED_KEYS = {CLIENT_FLAG_KEY}

EXCLUDED_QUERY_KEYS = MAIN_KEYS | IGNORED_KEYS


class MySQLAdapter(DatabaseAdapter):
    @classmethod
    def type(cls) -> DatasourceType:
        full_type = MySQLConfigFile.model_fields["type"].default
        return DatasourceType(full_type=full_type)

    @classmethod
    def accept(cls, conn: DBConnection) -> bool:
        if isinstance(conn, (Engine, Connection)):
            dialect = conn.dialect
            return dialect.name.startswith(("mysql", "mariadb"))
        return isinstance(conn, MySQLConnectionProperties)

    @classmethod
    def create_config_file(cls, config: DBConnectionConfig, name: str) -> AbstractConfigFile:
        if not isinstance(config, MySQLConnectionProperties):
            raise ValueError(f"Invalid connection config type: expected MySQLConnectionProperties, got {type(config)}.")
        return MySQLConfigFile(connection=config, name=name)

    @classmethod
    def create_config_from_runtime(cls, run_conn: DBConnectionRuntime) -> DBConnectionConfig:
        if not isinstance(run_conn, (Engine, Connection)):
            raise ValueError(
                f"Invalid runtime connection type: expected SQLAlchemy Engine or Connection, got {type(run_conn)}."
            )

        engine = run_conn if isinstance(run_conn, Engine) else run_conn.engine
        dialect = engine.dialect
        if not dialect.name.startswith(("mysql", "mariadb")):
            raise ValueError(
                f'Invalid runtime connection dialect: expected "mysql" or "mariadb", got "{dialect.name}".'
            )

        sa_url_str = engine.url.render_as_string(hide_password=False)
        sa_url = make_url(sa_url_str)
        content = dict(dialect.create_connect_args(sa_url)[1])
        if "dbname" in content:
            content[DATABASE_KEY] = content.pop("dbname")

        return MySQLConnectionProperties(
            host=sa_url.host,
            port=sa_url.port,
            database=sa_url.database,
            user=sa_url.username,
            password=sa_url.password,
            additional_properties={k: v for k, v in content.items() if k not in EXCLUDED_QUERY_KEYS},
        )

    @classmethod
    def create_config_from_content(cls, content: dict[str, Any]) -> DBConnectionConfig:
        config_file = MySQLConfigFile.model_validate({"name": "", **content})
        return config_file.connection

    @classmethod
    def register_in_duckdb(cls, shared_conn: DuckDBPyConnection, config: DBConnectionConfig, name: str) -> None:
        if not isinstance(config, MySQLConnectionProperties):
            raise ValueError(f"Invalid connection config type: expected MySQLConnectionProperties, got {type(config)}.")
        url = cls._create_url(config)
        shared_conn.execute("INSTALL mysql;")
        shared_conn.execute("LOAD mysql;")
        shared_conn.execute(f"ATTACH '{url}' AS \"{name}\" (TYPE MYSQL);")

    @staticmethod
    def _create_url(config: MySQLConnectionProperties) -> str:
        url = URL.create(
            drivername="mysql",
            username=config.user,
            password=config.password,
            host=config.host,
            port=config.port,
            database=config.database,
            query=str_dict(config.additional_properties),
        )
        return url.render_as_string(hide_password=False)
