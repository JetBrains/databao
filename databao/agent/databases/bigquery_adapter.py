from typing import Any

from _duckdb import DuckDBPyConnection
from databao_context_engine import DatasourceType
from databao_context_engine.pluginlib.build_plugin import AbstractConfigFile
from databao_context_engine.plugins.databases.bigquery.config_file import (
    BigQueryConfigFile,
    BigQueryConnectionProperties,
    BigQueryDefaultAuth,
    BigQueryServiceAccountJsonAuth,
    BigQueryServiceAccountKeyFileAuth,
)
from sqlalchemy import Connection, Engine, make_url

from databao.agent.databases.database_adapter import DatabaseAdapter
from databao.agent.databases.database_connection import DBConnection, DBConnectionConfig, DBConnectionRuntime

PROJECT_KEY = "project"
DATASET_KEY = "dataset"
LOCATION_KEY = "location"
CREDENTIALS_FILE_KEY = "credentials_file"
CREDENTIALS_JSON_KEY = "credentials_json"

MAIN_KEYS = {PROJECT_KEY, DATASET_KEY, LOCATION_KEY, CREDENTIALS_FILE_KEY, CREDENTIALS_JSON_KEY}


class BigQueryAdapter(DatabaseAdapter):
    @classmethod
    def type(cls) -> DatasourceType:
        full_type = BigQueryConfigFile.model_fields["type"].default
        return DatasourceType(full_type=full_type)

    @classmethod
    def accept(cls, conn: DBConnection) -> bool:
        if isinstance(conn, (Engine, Connection)):
            return conn.dialect.name.startswith("bigquery")
        return isinstance(conn, BigQueryConnectionProperties)

    @classmethod
    def create_config_file(cls, config: DBConnectionConfig, name: str) -> AbstractConfigFile:
        if not isinstance(config, BigQueryConnectionProperties):
            raise ValueError(
                f"Invalid connection config type: expected BigQueryConnectionProperties, got {type(config)}."
            )
        return BigQueryConfigFile(connection=config, name=name)

    @classmethod
    def create_config_from_runtime(cls, run_conn: DBConnectionRuntime) -> DBConnectionConfig:
        if not isinstance(run_conn, (Engine, Connection)):
            raise ValueError(
                f"Invalid runtime connection type: expected SQLAlchemy Engine or Connection, got {type(run_conn)}."
            )

        engine = run_conn if isinstance(run_conn, Engine) else run_conn.engine
        dialect = engine.dialect
        if not dialect.name.startswith("bigquery"):
            raise ValueError(f'Invalid runtime connection dialect: expected "bigquery", got "{dialect.name}".')

        sa_url_str = engine.url.render_as_string(hide_password=False)
        sa_url = make_url(sa_url_str)

        # BigQuery SQLAlchemy URL format: bigquery://project/dataset
        project = sa_url.host or ""
        database = sa_url.database
        dataset = database.lstrip("/") if database else None

        query = dict(sa_url.query)

        return BigQueryConnectionProperties(
            project=project,
            dataset=dataset or None,
            location=query.get(LOCATION_KEY),
            auth=cls._create_auth_from_query(query),
            additional_properties={k: v for k, v in query.items() if k not in MAIN_KEYS},
        )

    @classmethod
    def create_config_from_content(cls, content: dict[str, Any]) -> DBConnectionConfig:
        config_file = BigQueryConfigFile.model_validate({"name": "", **content})
        return config_file.connection

    @classmethod
    def register_in_duckdb(cls, shared_conn: DuckDBPyConnection, config: DBConnectionConfig, name: str) -> None:
        if not isinstance(config, BigQueryConnectionProperties):
            raise ValueError(
                f"Invalid connection config type: expected BigQueryConnectionProperties, got {type(config)}."
            )
        connection_string = cls._create_connection_string(config)
        shared_conn.execute("INSTALL bigquery FROM community;")
        shared_conn.execute("LOAD bigquery;")
        shared_conn.execute(f"ATTACH '{connection_string}' AS \"{name}\" (TYPE bigquery, READ_ONLY);")
        # Workaround for a bug in the DuckDB BigQuery community extension: when ORDER BY and LIMIT
        # appear together DuckDB's optimizer generates a TopN filter that the extension's
        # TransformFilter method does not handle, causing an INTERNAL error.  Disabling only the
        # top_n optimizer rule avoids the crash with minimal impact on other query plans.
        shared_conn.execute("SET disabled_optimizers='top_n';")

    @staticmethod
    def _create_auth_from_query(
        query: dict[str, Any],
    ) -> BigQueryDefaultAuth | BigQueryServiceAccountKeyFileAuth | BigQueryServiceAccountJsonAuth:
        if CREDENTIALS_JSON_KEY in query:
            return BigQueryServiceAccountJsonAuth(credentials_json=query[CREDENTIALS_JSON_KEY])
        if CREDENTIALS_FILE_KEY in query:
            return BigQueryServiceAccountKeyFileAuth(credentials_file=query[CREDENTIALS_FILE_KEY])
        return BigQueryDefaultAuth()

    @staticmethod
    def _create_connection_string(config: BigQueryConnectionProperties) -> str:
        params: dict[str, str] = {PROJECT_KEY: config.project}
        if config.dataset:
            params[DATASET_KEY] = config.dataset
        if config.location:
            params[LOCATION_KEY] = config.location

        auth = config.auth
        if isinstance(auth, BigQueryServiceAccountKeyFileAuth):
            params[CREDENTIALS_FILE_KEY] = auth.credentials_file
        elif isinstance(auth, BigQueryServiceAccountJsonAuth):
            params[CREDENTIALS_JSON_KEY] = auth.credentials_json

        for k, v in config.additional_properties.items():
            params[k] = str(v)

        return " ".join(f"{k}={v}" for k, v in params.items())
