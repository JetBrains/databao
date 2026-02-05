from urllib.parse import quote_plus

from _duckdb import DuckDBPyConnection
from databao_context_engine import DatasourceType
from sqlalchemy import Connection, Engine, make_url

from databao.databases.database_adapter import DatabaseAdapter
from databao.databases.database_connection import DBConnection, DBConnectionConfig, DBConnectionRuntime

SNOWFLAKE_TYPE = DatasourceType(full_type="snowflake")

USER_KEY = "user"
PASSWORD_KEY = "password"
ACCOUNT_KEY = "account"
PORT_KEY = "port"
DATABASE_KEY = "database"


class SnowflakeAdapter(DatabaseAdapter):
    @classmethod
    def type(cls) -> DatasourceType:
        return SNOWFLAKE_TYPE

    @classmethod
    def accept(cls, conn: DBConnection) -> bool:
        if isinstance(conn, (Engine, Connection)):
            dialect = conn.dialect
            return dialect.name.startswith("snowflake")
        if isinstance(conn, DBConnectionConfig):
            return conn.type == SNOWFLAKE_TYPE  # type: ignore[no-any-return]
        return False

    @classmethod
    def convert_to_config(cls, run_conn: DBConnectionRuntime) -> DBConnectionConfig | None:
        if not isinstance(run_conn, (Engine, Connection)):
            return None

        engine = run_conn if isinstance(run_conn, Engine) else run_conn.engine
        dialect = engine.dialect
        if not dialect.name.startswith("snowflake"):
            return None

        sa_url_str = engine.url.render_as_string(hide_password=False)
        sa_url = make_url(sa_url_str)
        content = dict(dialect.create_connect_args(sa_url)[1])

        return DBConnectionConfig(type=SNOWFLAKE_TYPE, content=content)

    # TODO: url and name should be escaped properly
    @classmethod
    def register_in_duckdb(cls, shared_conn: DuckDBPyConnection, config: DBConnectionConfig, name: str) -> None:
        connection_string = cls._create_connection_string(config)
        shared_conn.execute("INSTALL snowflake FROM community;")
        shared_conn.execute("LOAD snowflake;")
        shared_conn.execute(f"ATTACH '{connection_string}' AS \"{name}\" (TYPE snowflake, READ_ONLY);")

    @staticmethod
    def _sql_string_literal(s: str) -> str:
        return "'" + s.replace("'", "''") + "'"

    @staticmethod
    def _create_connection_string(conn: DBConnectionConfig) -> str:
        content = conn.content
        connection = content.get("connection")

        if connection is None:
            raise ValueError("Cannot find snowflake connection in config")

        account = str(connection.get(ACCOUNT_KEY))
        database = str(connection.get(DATABASE_KEY))
        user = str(connection.get(USER_KEY))

        auth = connection.get("auth")

        if auth is None:
            raise ValueError("Cannot find snowflake auth in config")

        password = str(auth.get(PASSWORD_KEY))

        return (
            f"account={quote_plus(account)};"
            f"database={quote_plus(database)};"
            f"user={quote_plus(user)};"
            f"password={quote_plus(password)};"
        )
