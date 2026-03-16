from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


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
        connect_args: dict = {}
        base_url = f"snowflake://{user}@{account}/{database}/{schema}"

        if auth == "password":
            base_url = f"snowflake://{user}:{password}@{account}/{database}/{schema}"
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
            base_url += f"?warehouse={warehouse}"

        self.engine = create_engine(base_url, connect_args=connect_args)

    def execute_sql(self, sql: str) -> tuple[bool, pd.DataFrame | str]:
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(text(sql), conn)
            return True, df
        except Exception as e:
            return False, str(e)


def create_runner():
    """Create a database runner based on DATABASE_TYPE env var."""
    db_type = os.environ.get("DATABASE_TYPE", "sqlalchemy")

    if db_type == "snowflake":
        return SnowflakeRunner(
            user=os.environ.get("SNOWFLAKE_USER", ""),
            account=os.environ.get("SNOWFLAKE_ACCOUNT", ""),
            database=os.environ.get("SNOWFLAKE_DATABASE", ""),
            schema=os.environ.get("SNOWFLAKE_SCHEMA", ""),
            auth=os.environ.get("SNOWFLAKE_AUTH", "password"),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", ""),
            password=os.environ.get("SNOWFLAKE_PASSWORD", ""),
            private_key_path=os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", ""),
        )
    elif db_type == "duckdb":
        return DuckDBRunner(os.environ.get("DUCKDB_PATH", ""))
    else:
        return SQLAlchemyRunner(os.environ.get("DATABASE_URL", "sqlite:///:memory:"))


def create_databao_domain(runner=None):
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
        domain.add_db(runner.engine, name="db1")

    return domain
