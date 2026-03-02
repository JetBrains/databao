from typing import Any

from _duckdb import DuckDBPyConnection
from databao_context_engine import DatasourceType, PostgresConfigFile, PostgresConnectionProperties
from databao_context_engine.pluginlib.build_plugin import AbstractConfigFile
from sqlalchemy import URL, Connection, Engine, make_url

from databao.databases.database_adapter import DatabaseAdapter
from databao.databases.database_connection import DBConnection, DBConnectionConfig, DBConnectionRuntime

USER_KEY = "user"
PASSWORD_KEY = "password"
HOST_KEY = "host"
PORT_KEY = "port"
DATABASE_KEY = "database"

MAIN_KEYS = {USER_KEY, PASSWORD_KEY, HOST_KEY, PORT_KEY, DATABASE_KEY}

EXCLUDED_QUERY_KEYS = {*MAIN_KEYS}

# asyncpg uses server_settings dict instead of a libpq options string.
# These sslmode values all require SSL to be enabled.
_SSLMODE_REQUIRE = {"require", "verify-ca", "verify-full"}


def _parse_pg_options(options_str: str) -> dict[str, str]:
    """Parse a libpq options string into a key/value dict for asyncpg server_settings.

    Handles both '-c key=value' and plain 'key=value' tokens.
    """
    result: dict[str, str] = {}
    tokens = options_str.split()
    i = 0
    while i < len(tokens):
        token = tokens[i]
        if token == "-c" and i + 1 < len(tokens):
            i += 1
            token = tokens[i]
        if "=" in token:
            k, _, v = token.partition("=")
            result[k.strip()] = v.strip()
        i += 1
    return result


class PostgreSQLAdapter(DatabaseAdapter):
    @classmethod
    def type(cls) -> DatasourceType:
        full_type = PostgresConfigFile.model_fields["type"].default
        return DatasourceType(full_type=full_type)

    @classmethod
    def accept(cls, conn: DBConnection) -> bool:
        if isinstance(conn, (Engine, Connection)):
            dialect = conn.dialect
            return dialect.name.startswith("postgres")
        return isinstance(conn, PostgresConnectionProperties)

    @classmethod
    def create_config_file(cls, config: DBConnectionConfig, name: str) -> AbstractConfigFile:
        if not isinstance(config, PostgresConnectionProperties):
            raise ValueError(
                f"Invalid connection config type: expected PostgresConnectionProperties, got {type(config)}."
            )
        return PostgresConfigFile(connection=config, name=name)

    @classmethod
    def create_config_from_runtime(cls, run_conn: DBConnectionRuntime) -> DBConnectionConfig:
        if not isinstance(run_conn, (Engine, Connection)):
            raise ValueError(
                f"Invalid runtime connection type: expected SQLAlchemy Engine or Connection, got {type(run_conn)}."
            )

        engine = run_conn if isinstance(run_conn, Engine) else run_conn.engine
        dialect = engine.dialect
        if not dialect.name.startswith("postgres"):
            raise ValueError(f'Invalid runtime connection dialect: expected "postgres", got "{dialect.name}".')

        sa_url_str = engine.url.render_as_string(hide_password=False)
        sa_url = make_url(sa_url_str)
        content = dict(dialect.create_connect_args(sa_url)[1])
        if "dbname" in content:
            content[DATABASE_KEY] = content.pop("dbname")

        additional_properties: dict[str, Any] = {}
        for k, v in content.items():
            if k in EXCLUDED_QUERY_KEYS:
                continue
            if k == "options":
                # asyncpg uses server_settings dict; libpq uses an options string
                server_settings = _parse_pg_options(v)
                if server_settings:
                    additional_properties["server_settings"] = server_settings
            elif k == "sslmode":
                # asyncpg uses ssl=True/False; libpq uses sslmode string
                sslmode = str(v).lower()
                if sslmode == "disable":
                    # Explicitly disable SSL
                    additional_properties["ssl"] = False
                elif sslmode in _SSLMODE_REQUIRE or sslmode in {"prefer", "allow"}:
                    # SSL modes that attempt or prefer SSL map to enabling SSL
                    additional_properties["ssl"] = True
                # For other/unknown sslmode values, omit "ssl" and let driver defaults apply.
            else:
                additional_properties[k] = v

        return PostgresConnectionProperties(
            host=sa_url.host,
            port=sa_url.port,
            database=sa_url.database,
            user=sa_url.username,
            password=sa_url.password,
            additional_properties=additional_properties,
        )

    @classmethod
    def create_config_from_content(cls, content: dict[str, Any]) -> DBConnectionConfig:
        config_file = PostgresConfigFile.model_validate({"name": "", **content})
        return config_file.connection

    @classmethod
    def register_in_duckdb(cls, shared_conn: DuckDBPyConnection, config: DBConnectionConfig, name: str) -> None:
        if not isinstance(config, PostgresConnectionProperties):
            raise ValueError(
                f"Invalid connection config type: expected PostgresConnectionProperties, got {type(config)}."
            )
        url = cls._create_url(config)
        shared_conn.execute("INSTALL postgres;")
        shared_conn.execute("LOAD postgres;")
        shared_conn.execute(f"ATTACH '{url}' AS \"{name}\" (TYPE POSTGRES);")

    @staticmethod
    def _create_url(config: PostgresConnectionProperties) -> str:
        # Reconstruct libpq-style query params from asyncpg-style additional_properties.
        query: dict[str, str] = {}
        for k, v in config.additional_properties.items():
            if k == "server_settings" and isinstance(v, dict):
                # Convert server_settings dict back to libpq options string
                options = " ".join(f"{sk}={sv}" for sk, sv in v.items())
                if options:
                    query["options"] = options
            elif k == "sslmode":
                # Preserve an explicit sslmode if it was provided
                query["sslmode"] = str(v)
            elif k == "ssl" and isinstance(v, bool):
                # Convert ssl bool back to sslmode string, but do not overwrite an explicit sslmode
                if "sslmode" not in query:
                    # Map True to "require"; map False to "prefer" instead of "disable" to avoid
                    # collapsing all non-require modes (e.g. prefer/allow) into "disable".
                    query["sslmode"] = "require" if v else "prefer"
            else:
                query[k] = str(v)

        url = URL.create(
            drivername="postgresql",
            username=config.user,
            password=config.password,
            host=config.host,
            port=config.port,
            database=config.database,
            query=query,
        )
        return url.render_as_string(hide_password=False)
