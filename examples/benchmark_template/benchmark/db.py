from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Protocol

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from benchmark.helpers import must_env


class DBRunner(Protocol):
    def execute_sql(self, sql: str) -> tuple[bool, pd.DataFrame | str]: ...


class SQLAlchemyRunner:
    """Database runner for any SQLAlchemy-supported database.

    Works with PostgreSQL, MySQL, SQLite, BigQuery, etc.
    Just provide a connection string.
    """

    def __init__(self, connection_string: str) -> None:
        self.engine = create_engine(connection_string)

    def execute_sql(self, sql: str) -> tuple[bool, pd.DataFrame | str]:
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(text(sql), conn)
            return True, df
        except Exception as e:
            return False, str(e)


class DuckDBRunner:
    """Database runner for DuckDB using native duckdb connection.

    Provide the path to a .duckdb file. Opens a fresh connection per query
    to avoid file lock conflicts with databao's connection.
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    def execute_sql(self, sql: str) -> tuple[bool, pd.DataFrame | str]:
        import duckdb

        try:
            conn = duckdb.connect(self.db_path)
            try:
                df = conn.execute(sql).fetchdf()
                return True, df
            except Exception as e:
                return False, str(e)
            finally:
                conn.close()
        except Exception as e:
            return False, str(e)


class SnowflakeRunner:
    """Snowflake runner with password, key-pair, or SSO authentication.

    Auth methods:
        "password"  - uses SNOWFLAKE_USER + SNOWFLAKE_PASSWORD
        "key_pair"  - uses SNOWFLAKE_USER + SNOWFLAKE_PRIVATE_KEY_PATH
        "sso"       - opens browser for SSO login
    """

    def __init__(
        self,
        user: str,
        account: str,
        database: str,
        schema: str,
        auth: str = "password",
        warehouse: str = "",
        password: str = "",
        private_key_path: str = "",
    ) -> None:
        connect_args: dict[str, object] = {}
        query_params: dict[str, str] = {}

        if auth == "password":
            pass  # password is passed via URL.create() below
        elif auth == "key_pair":
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import serialization

            private_key_resolved_path = Path(private_key_path).expanduser()
            with private_key_resolved_path.open("rb") as f:
                private_key = serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())
            connect_args["private_key"] = private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        elif auth == "sso":
            connect_args["authenticator"] = "externalbrowser"
        else:
            raise ValueError(f"Unknown auth method: {auth!r}. Use 'password', 'key_pair', or 'sso'.")

        if warehouse:
            query_params["warehouse"] = warehouse

        url = URL.create(
            "snowflake",
            username=user,
            password=password if auth == "password" else None,
            host=account,
            database=f"{database}/{schema}",
            query=query_params,
        )
        self.engine = create_engine(url, connect_args=connect_args)

    def execute_sql(self, sql: str) -> tuple[bool, pd.DataFrame | str]:
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(text(sql), conn)
            return True, df
        except Exception as e:
            return False, str(e)


def create_runner() -> SQLAlchemyRunner | DuckDBRunner | SnowflakeRunner:
    """Create a sql query runner based on DATABASE_TYPE env var."""
    db_type = os.environ.get("DATABASE_TYPE", "sqlalchemy")

    if db_type == "snowflake":
        auth = os.environ.get("SNOWFLAKE_AUTH", "password")
        return SnowflakeRunner(
            user=must_env("SNOWFLAKE_USER"),
            account=must_env("SNOWFLAKE_ACCOUNT"),
            database=must_env("SNOWFLAKE_DATABASE"),
            schema=must_env("SNOWFLAKE_SCHEMA"),
            auth=auth,
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", ""),
            password=must_env("SNOWFLAKE_PASSWORD") if auth == "password" else "",
            private_key_path=must_env("SNOWFLAKE_PRIVATE_KEY_PATH") if auth == "key_pair" else "",
        )
    elif db_type == "duckdb":
        return DuckDBRunner(os.environ.get("DUCKDB_PATH", ""))
    else:
        return SQLAlchemyRunner(os.environ.get("DATABASE_URL", "sqlite:///:memory:"))


def create_databao_domain(runner: SQLAlchemyRunner | DuckDBRunner | SnowflakeRunner | None = None) -> Any:
    """Create a databao domain pre-configured with the database from env vars.

    Uses the same database connection as the benchmark runner, so gold SQLs
    and databao agent queries run against the same database.

    Args:
        runner: An existing runner (from create_runner()). If None, creates one.
    """
    import databao.agent as bao

    if runner is None:
        runner = create_runner()

    db_type = os.environ.get("DATABASE_TYPE", "sqlalchemy")
    domain = bao.domain()

    if db_type == "snowflake":
        from databao_context_engine import SnowflakeConnectionProperties

        auth_method = os.environ.get("SNOWFLAKE_AUTH", "password").lower()
        if auth_method == "key_pair":
            from databao_context_engine import SnowflakeKeyPairAuth

            private_key_path = str(Path(os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "")).expanduser())
            auth = SnowflakeKeyPairAuth(private_key_file=private_key_path)
        elif auth_method == "password":
            from databao_context_engine import SnowflakePasswordAuth

            auth = SnowflakePasswordAuth(password=os.environ.get("SNOWFLAKE_PASSWORD", ""))
        elif auth_method == "sso":
            from databao_context_engine import SnowflakeSSOAuth

            auth = SnowflakeSSOAuth()
        else:
            raise ValueError(f"Unknown SNOWFLAKE_AUTH: {auth_method!r}")

        domain.add_db(
            SnowflakeConnectionProperties(
                user=os.environ.get("SNOWFLAKE_USER", ""),
                account=os.environ.get("SNOWFLAKE_ACCOUNT", ""),
                database=os.environ.get("SNOWFLAKE_DATABASE", ""),
                warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "") or None,
                auth=auth,
            ),
            name="db1",
        )
    elif db_type == "duckdb":
        import duckdb

        domain.add_db(duckdb.connect(os.environ.get("DUCKDB_PATH", "")))
    else:
        assert isinstance(runner, SQLAlchemyRunner)
        domain.add_db(runner.engine, name="db1")

    return domain
