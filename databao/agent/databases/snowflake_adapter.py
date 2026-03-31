from pathlib import Path
from typing import Any

from _duckdb import DuckDBPyConnection
from databao_context_engine import (
    DatasourceType,
    SnowflakeConfigFile,
    SnowflakeConnectionProperties,
    SnowflakeKeyPairAuth,
    SnowflakeOAuthAuth,
    SnowflakePasswordAuth,
    SnowflakeSSOAuth,
)
from databao_context_engine.pluginlib.build_plugin import AbstractConfigFile
from snowflake.connector.network import SNOWFLAKE_HOST_SUFFIX
from sqlalchemy import Connection, Engine, create_engine, make_url

from databao.agent.databases.database_adapter import DatabaseAdapter
from databao.agent.databases.database_connection import DBConnection, DBConnectionConfig, DBConnectionRuntime

WAREHOUSE_KEY = "warehouse"
ACCOUNT_KEY = "account"
DATABASE_KEY = "database"
USER_KEY = "user"

AUTH_KEY = "auth"
PASSWORD_KEY = "password"
PRIVATE_KEY_KEY = "private_key"
PRIVATE_KEY_FILE_KEY = "private_key_file"
PRIVATE_KEY_PASSPHRASE_KEY = "private_key_passphrase"
OKTA_URL_KEY = "okta_url"

TOKEN_KEY = "token"

MAIN_KEYS = {
    WAREHOUSE_KEY,
    ACCOUNT_KEY,
    DATABASE_KEY,
    USER_KEY,
}

AUTH_KEYS = {
    PASSWORD_KEY,
    PRIVATE_KEY_KEY,
    PRIVATE_KEY_FILE_KEY,
    PRIVATE_KEY_PASSPHRASE_KEY,
    OKTA_URL_KEY,
}

# Keys injected by SQLAlchemy's Snowflake dialect that are not valid Snowflake connection properties.
# Note: "host" is also dialect-internal but handled separately because its value is used to derive the account.
_SQLALCHEMY_INTERNAL_KEYS = {"port", "autocommit"}

EXCLUDED_QUERY_KEYS = {*MAIN_KEYS, *AUTH_KEYS}

AUTH_TYPE_KEY = "auth_type"
AUTH_TYPE_KEY_PAIR = "key_pair"
AUTH_TYPE_OAUTH = "oauth"
AUTH_TYPE_OKTA = "okta"


class SnowflakeAdapter(DatabaseAdapter):
    @classmethod
    def type(cls) -> DatasourceType:
        full_type = SnowflakeConfigFile.model_fields["type"].default
        return DatasourceType(full_type=full_type)

    @classmethod
    def accept(cls, conn: DBConnection) -> bool:
        if isinstance(conn, (Engine, Connection)):
            dialect = conn.dialect
            return dialect.name.startswith("snowflake")
        return isinstance(conn, SnowflakeConnectionProperties)

    @classmethod
    def create_config_file(cls, config: DBConnectionConfig, name: str) -> AbstractConfigFile:
        if not isinstance(config, SnowflakeConnectionProperties):
            raise ValueError(
                f"Invalid connection config type: expected SnowflakeConnectionProperties, got {type(config)}."
            )
        return SnowflakeConfigFile(connection=config, name=name)

    @classmethod
    def create_config_from_runtime(cls, run_conn: DBConnectionRuntime) -> DBConnectionConfig:
        if not isinstance(run_conn, (Engine, Connection)):
            raise ValueError(
                f"Invalid runtime connection type: expected SQLAlchemy Engine or Connection, got {type(run_conn)}."
            )

        engine = run_conn if isinstance(run_conn, Engine) else run_conn.engine
        dialect = engine.dialect
        if not dialect.name.startswith("snowflake"):
            raise ValueError(f'Invalid runtime connection dialect: expected "snowflake", got "{dialect.name}".')

        sa_url_str = engine.url.render_as_string(hide_password=False)
        sa_url = make_url(sa_url_str)
        content = dict(dialect.create_connect_args(sa_url)[1])
        if "dbname" in content:
            content[DATABASE_KEY] = content.pop("dbname")

        host: str | None = content.pop("host", None)
        for key in _SQLALCHEMY_INTERNAL_KEYS:
            content.pop(key, None)
        account: str = content.get(ACCOUNT_KEY, "")
        if host and host.endswith(SNOWFLAKE_HOST_SUFFIX):
            account = host[: -len(SNOWFLAKE_HOST_SUFFIX)]

        return SnowflakeConnectionProperties(
            account=account,
            warehouse=content.get(WAREHOUSE_KEY),
            database=content.get(DATABASE_KEY),
            user=content.get(USER_KEY),
            role=None,
            auth=cls._create_auth(content),
            additional_properties={k: v for k, v in content.items() if k not in EXCLUDED_QUERY_KEYS},
        )

    @classmethod
    def create_config_from_content(cls, content: dict[str, Any]) -> DBConnectionConfig:
        config_file = SnowflakeConfigFile.model_validate({"name": "", **content})
        return config_file.connection

    @classmethod
    def create_sqlalchemy_engine(cls, config: DBConnectionConfig) -> Engine | None:
        if not isinstance(config, SnowflakeConnectionProperties):
            return None

        from snowflake.sqlalchemy import URL  # type: ignore[import-untyped]

        url_kwargs: dict[str, str] = {"account": config.account}
        if config.user:
            url_kwargs["user"] = config.user
        if config.database:
            url_kwargs["database"] = config.database
        if config.warehouse:
            url_kwargs["warehouse"] = config.warehouse
        if config.role:
            url_kwargs["role"] = config.role

        connect_args: dict[str, Any] = {}
        auth = config.auth
        if isinstance(auth, SnowflakePasswordAuth):
            url_kwargs["password"] = auth.password
        elif isinstance(auth, SnowflakeKeyPairAuth):
            connect_args["private_key"] = cls._load_private_key_bytes(auth)
        elif isinstance(auth, SnowflakeSSOAuth):
            url_kwargs["authenticator"] = auth.authenticator
        else:
            return None

        if connect_args:
            return create_engine(URL(**url_kwargs), connect_args=connect_args)
        return create_engine(URL(**url_kwargs))

    @staticmethod
    def _load_private_key_bytes(auth: SnowflakeKeyPairAuth) -> bytes:
        from cryptography.hazmat.primitives import serialization

        if auth.private_key:
            pem_data = auth.private_key.encode()
        elif auth.private_key_file:
            pem_data = Path(auth.private_key_file).read_bytes()
        else:
            raise ValueError("No private key provided.")

        passphrase = auth.private_key_file_pwd.encode() if auth.private_key_file_pwd else None
        private_key = serialization.load_pem_private_key(pem_data, password=passphrase)
        return private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    # TODO: url and name should be escaped properly
    @classmethod
    def register_in_duckdb(cls, shared_conn: DuckDBPyConnection, config: DBConnectionConfig, name: str) -> None:
        if not isinstance(config, SnowflakeConnectionProperties):
            raise ValueError(
                f"Invalid connection config type: expected SnowflakeConnectionProperties, got {type(config)}."
            )
        # Build the secret params before ATTACH so that preparation errors (e.g. unreadable key file)
        # don't leave the connection in a partially-registered state.
        secret_params = cls._create_secret_params(config)
        formatted_secret_params = cls._format_sql_params(secret_params)
        safe_name = cls._escape(name, '"')

        shared_conn.execute("INSTALL snowflake FROM community;")
        shared_conn.execute("LOAD snowflake;")
        shared_conn.execute(f'CREATE OR REPLACE SECRET "{safe_name}" (TYPE snowflake, {formatted_secret_params});')
        shared_conn.execute(f'ATTACH \'\' AS "{safe_name}" (TYPE snowflake, SECRET "{safe_name}", READ_ONLY);')

    @staticmethod
    def _create_secret_params(config: SnowflakeConnectionProperties) -> dict[str, str]:
        params: dict[str, str] = {
            ACCOUNT_KEY: config.account,
            **{k: str(v) for k, v in config.additional_properties.items()},
        }
        if config.user:
            params[USER_KEY] = config.user
        if config.database:
            params[DATABASE_KEY] = config.database
        if config.warehouse:
            params[WAREHOUSE_KEY] = config.warehouse
        if config.role:
            params["role"] = config.role

        auth = config.auth
        if isinstance(auth, SnowflakePasswordAuth):
            params[PASSWORD_KEY] = auth.password
        elif isinstance(auth, SnowflakeKeyPairAuth):
            params[AUTH_TYPE_KEY] = AUTH_TYPE_KEY_PAIR
            if auth.private_key:
                params[PRIVATE_KEY_KEY] = auth.private_key
            elif auth.private_key_file:
                try:
                    params[PRIVATE_KEY_KEY] = Path(auth.private_key_file).read_text()
                except OSError as exc:
                    raise ValueError(
                        f"Unable to read Snowflake private key file specified in 'private_key_file': "
                        f"{auth.private_key_file}"
                    ) from exc
            else:
                raise ValueError("No private key provided.")
            if auth.private_key_file_pwd:
                params[PRIVATE_KEY_PASSPHRASE_KEY] = auth.private_key_file_pwd
        elif isinstance(auth, SnowflakeOAuthAuth):
            params[AUTH_TYPE_KEY] = AUTH_TYPE_OAUTH
            params[TOKEN_KEY] = auth.token
        elif isinstance(auth, SnowflakeSSOAuth):
            authenticator = auth.authenticator
            if SnowflakeAdapter._is_okta_url(authenticator):
                params[AUTH_TYPE_KEY] = AUTH_TYPE_OKTA
                params[OKTA_URL_KEY] = authenticator
            else:
                params[AUTH_TYPE_KEY] = authenticator
        else:
            raise ValueError("Unsupported Snowflake authentication type.")

        return params

    @staticmethod
    def _create_auth(
        content: dict[str, Any],
    ) -> SnowflakePasswordAuth | SnowflakeKeyPairAuth | SnowflakeSSOAuth | SnowflakeOAuthAuth:
        if PASSWORD_KEY in content:
            return SnowflakePasswordAuth(password=content[PASSWORD_KEY])
        if content.keys() & {PRIVATE_KEY_KEY, PRIVATE_KEY_FILE_KEY}:
            return SnowflakeKeyPairAuth(
                private_key_file=content.get(PRIVATE_KEY_FILE_KEY),
                private_key_file_pwd=content.get(PRIVATE_KEY_PASSPHRASE_KEY),
                private_key=content.get(PRIVATE_KEY_KEY),
            )
        if TOKEN_KEY in content:
            return SnowflakeOAuthAuth(token=content[TOKEN_KEY])
        if OKTA_URL_KEY in content:
            return SnowflakeSSOAuth(authenticator=content[OKTA_URL_KEY])
        raise ValueError("Unsupported Snowflake authentication type.")

    @staticmethod
    def _escape(value: str, quote: str) -> str:
        return value.replace(quote, quote + quote)

    @classmethod
    def _format_sql_params(cls, params: dict[str, str]) -> str:
        return ", ".join(f"""{k} '{cls._escape(v, "'")}'""" for k, v in params.items())

    @staticmethod
    def _is_okta_url(authenticator: str) -> bool:
        return authenticator.startswith("https://")
