from pathlib import Path
from typing import Any

from _duckdb import DuckDBPyConnection
from databao_context_engine import DatasourceType
from sqlalchemy import Connection, Engine, make_url

from databao.databases.database_adapter import DatabaseAdapter
from databao.databases.database_connection import DBConnection, DBConnectionConfig, DBConnectionRuntime

SNOWFLAKE_TYPE = DatasourceType(full_type="snowflake")

WAREHOUSE_KEY = "warehouse"
ACCOUNT_KEY = "account"
PORT_KEY = "port"
DATABASE_KEY = "database"
SCHEMA_KEY = "schema"
AUTH_KEY = "auth"
AUTH_TYPE_KEY = "auth_type"
USER_KEY = "user"
PASSWORD_KEY = "password"
TOKEN_KEY = "token"
PRIVATE_KEY_KEY = "private_key"
PRIVATE_KEY_FILE_KEY = "private_key_file"
PRIVATE_KEY_PASSPHRASE_KEY = "private_key_passphrase"
OKTA_URL_KEY = "okta_url"

CONNECTION_KEYS = {
    WAREHOUSE_KEY,
    ACCOUNT_KEY,
    PORT_KEY,
    DATABASE_KEY,
    SCHEMA_KEY,
    USER_KEY,
}

AUTH_KEYS = {
    PASSWORD_KEY,
    TOKEN_KEY,
    PRIVATE_KEY_KEY,
    PRIVATE_KEY_FILE_KEY,
    PRIVATE_KEY_PASSPHRASE_KEY,
    OKTA_URL_KEY,
}

KNOWN_KEYS = {
    *CONNECTION_KEYS,
    *AUTH_KEYS,
}

MAIN_KEYS = {*CONNECTION_KEYS, AUTH_KEY}

AUTH_TYPE_KEY_PAIR = "key_pair"
AUTH_TYPE_OAUTH = "oauth"
AUTH_TYPE_OKTA = "okta"


def _filter_paramaters(parameters: dict[str, Any], keys: set[str]) -> dict[str, Any]:
    return {k: v for k, v in parameters.items() if k in keys}


class SnowflakeAdapter(DatabaseAdapter):
    @classmethod
    def type(cls) -> DatasourceType:
        return SNOWFLAKE_TYPE

    @classmethod
    def main_property_keys(cls) -> set[str]:
        return MAIN_KEYS

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
        arguments = dict(dialect.create_connect_args(sa_url)[1])
        if "dbname" in arguments and "database" not in arguments:
            arguments["database"] = arguments.pop("dbname")

        content = _filter_paramaters(arguments, CONNECTION_KEYS)
        content[AUTH_KEY] = _filter_paramaters(arguments, AUTH_KEYS)

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

        if content is None:
            raise ValueError("Cannot find snowflake content in config")

        auth = content.get(AUTH_KEY)

        if auth is None:
            raise ValueError("Cannot find snowflake auth in config")

        connection_parameters = _filter_paramaters(content, KNOWN_KEYS)

        auth_type = SnowflakeAdapter._auth_type(auth)

        if auth_type is not None:
            connection_parameters[AUTH_TYPE_KEY] = auth_type

        # Work on a copy of the auth parameters to avoid mutating the original config.
        auth_params = dict(auth)

        if "private_key_file" in auth_params:
            auth_params["private_key"] = Path(auth_params["private_key_file"]).absolute()
            del auth_params["private_key_file"]

        all_parameters = {**connection_parameters, **auth_params}
        return ";".join(f"{k}={v}" for k, v in all_parameters.items())

    @staticmethod
    def _auth_type(parameters: dict[str, Any]) -> str | None:
        if parameters.keys() & {PRIVATE_KEY_KEY, PRIVATE_KEY_FILE_KEY}:
            return AUTH_TYPE_KEY_PAIR
        if TOKEN_KEY in parameters:
            return AUTH_TYPE_OAUTH
        if OKTA_URL_KEY in parameters:
            return AUTH_TYPE_OKTA

        return None
