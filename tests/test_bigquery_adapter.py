from typing import Any
from unittest.mock import MagicMock

import pytest
from databao_context_engine.plugins.databases.bigquery.config_file import (
    BigQueryConfigFile,
    BigQueryConnectionProperties,
    BigQueryDefaultAuth,
    BigQueryServiceAccountJsonAuth,
    BigQueryServiceAccountKeyFileAuth,
)

from databao.databases.bigquery_adapter import BigQueryAdapter
from databao.databases.duckdb_adapter import DuckDBAdapter
from databao.databases.postgresql_adapter import PostgreSQLAdapter


@pytest.fixture
def adapter() -> BigQueryAdapter:
    return BigQueryAdapter()


@pytest.fixture
def default_config() -> BigQueryConnectionProperties:
    return BigQueryConnectionProperties(project="my-project")


@pytest.fixture
def key_file_config() -> BigQueryConnectionProperties:
    return BigQueryConnectionProperties(
        project="my-project",
        dataset="my_dataset",
        auth=BigQueryServiceAccountKeyFileAuth(credentials_file="/path/to/key.json"),
    )


@pytest.fixture
def json_config() -> BigQueryConnectionProperties:
    return BigQueryConnectionProperties(
        project="my-project",
        dataset="my_dataset",
        location="US",
        auth=BigQueryServiceAccountJsonAuth(credentials_json='{"type":"service_account"}'),
    )


class TestAccept:
    def test_accepts_bigquery_connection_properties(
        self, adapter: BigQueryAdapter, default_config: BigQueryConnectionProperties
    ) -> None:
        assert adapter.accept(default_config) is True

    def test_rejects_other_config_types(self, adapter: BigQueryAdapter) -> None:
        from databao_context_engine import DuckDBConnectionConfig

        assert adapter.accept(DuckDBConnectionConfig(database_path="/tmp/db.duckdb")) is False

    def test_accepts_bigquery_sqlalchemy_engine(self, adapter: BigQueryAdapter) -> None:
        from sqlalchemy import Engine

        mock_engine = MagicMock()
        mock_engine.__class__ = Engine  # type: ignore[assignment]
        mock_engine.dialect.name = "bigquery"
        assert adapter.accept(mock_engine) is True

    def test_rejects_postgres_sqlalchemy_engine(self, adapter: BigQueryAdapter) -> None:
        from sqlalchemy import Engine

        mock_engine = MagicMock()
        mock_engine.__class__ = Engine  # type: ignore[assignment]
        mock_engine.dialect.name = "postgresql"
        assert adapter.accept(mock_engine) is False


class TestType:
    def test_type_is_bigquery(self, adapter: BigQueryAdapter) -> None:
        assert adapter.type().full_type == "bigquery"

    def test_type_differs_from_other_adapters(self, adapter: BigQueryAdapter) -> None:
        assert adapter.type() != DuckDBAdapter().type()
        assert adapter.type() != PostgreSQLAdapter().type()


class TestCreateConfigFile:
    def test_returns_bigquery_config_file(
        self, adapter: BigQueryAdapter, default_config: BigQueryConnectionProperties
    ) -> None:
        result = adapter.create_config_file(default_config, "my_ds")
        assert isinstance(result, BigQueryConfigFile)

    def test_config_file_has_correct_name(
        self, adapter: BigQueryAdapter, default_config: BigQueryConnectionProperties
    ) -> None:
        result = adapter.create_config_file(default_config, "my_ds")
        assert isinstance(result, BigQueryConfigFile)
        assert result.name == "my_ds"

    def test_config_file_has_correct_connection(
        self, adapter: BigQueryAdapter, default_config: BigQueryConnectionProperties
    ) -> None:
        result = adapter.create_config_file(default_config, "my_ds")
        assert isinstance(result, BigQueryConfigFile)
        assert result.connection == default_config

    def test_raises_on_wrong_config_type(self, adapter: BigQueryAdapter) -> None:
        from databao_context_engine import DuckDBConnectionConfig

        with pytest.raises(ValueError, match="BigQueryConnectionProperties"):
            adapter.create_config_file(DuckDBConnectionConfig(database_path="/tmp/db.duckdb"), "name")


class TestCreateConfigFromContent:
    def test_roundtrip_default_auth(self, adapter: BigQueryAdapter) -> None:
        content: dict[str, Any] = {
            "type": "bigquery",
            "connection": {"project": "my-project"},
        }
        result = adapter.create_config_from_content(content)
        assert isinstance(result, BigQueryConnectionProperties)
        assert result.project == "my-project"
        assert isinstance(result.auth, BigQueryDefaultAuth)

    def test_roundtrip_key_file_auth(self, adapter: BigQueryAdapter) -> None:
        content: dict[str, Any] = {
            "type": "bigquery",
            "connection": {
                "project": "my-project",
                "dataset": "ds",
                "auth": {"credentials_file": "/path/key.json"},
            },
        }
        result = adapter.create_config_from_content(content)
        assert isinstance(result, BigQueryConnectionProperties)
        assert isinstance(result.auth, BigQueryServiceAccountKeyFileAuth)
        assert result.auth.credentials_file == "/path/key.json"

    def test_roundtrip_json_auth(self, adapter: BigQueryAdapter) -> None:
        content: dict[str, Any] = {
            "type": "bigquery",
            "connection": {
                "project": "my-project",
                "auth": {"credentials_json": '{"type":"service_account"}'},
            },
        }
        result = adapter.create_config_from_content(content)
        assert isinstance(result, BigQueryConnectionProperties)
        assert isinstance(result.auth, BigQueryServiceAccountJsonAuth)


class TestCreateConfigFromRuntime:
    @staticmethod
    def _make_engine(url_string: str) -> MagicMock:
        from sqlalchemy import Engine, make_url

        mock = MagicMock()
        mock.__class__ = Engine  # type: ignore[assignment]
        mock.dialect.name = "bigquery"
        mock.url = make_url(url_string)
        return mock

    def test_basic_project_and_dataset(self, adapter: BigQueryAdapter) -> None:
        engine = self._make_engine("bigquery://my-project/my_dataset")
        result = adapter.create_config_from_runtime(engine)
        assert isinstance(result, BigQueryConnectionProperties)
        assert result.project == "my-project"
        assert result.dataset == "my_dataset"
        assert isinstance(result.auth, BigQueryDefaultAuth)

    def test_project_only(self, adapter: BigQueryAdapter) -> None:
        engine = self._make_engine("bigquery://my-project")
        result = adapter.create_config_from_runtime(engine)
        assert isinstance(result, BigQueryConnectionProperties)
        assert result.project == "my-project"
        assert result.dataset is None

    def test_location_extracted(self, adapter: BigQueryAdapter) -> None:
        engine = self._make_engine("bigquery://my-project/ds?location=US")
        result = adapter.create_config_from_runtime(engine)
        assert isinstance(result, BigQueryConnectionProperties)
        assert result.location == "US"

    def test_credentials_file_creates_key_file_auth(self, adapter: BigQueryAdapter) -> None:
        engine = self._make_engine("bigquery://my-project/ds?credentials_file=/path/to/key.json")
        result = adapter.create_config_from_runtime(engine)
        assert isinstance(result, BigQueryConnectionProperties)
        assert isinstance(result.auth, BigQueryServiceAccountKeyFileAuth)
        assert result.auth.credentials_file == "/path/to/key.json"

    def test_credentials_json_creates_json_auth(self, adapter: BigQueryAdapter) -> None:
        engine = self._make_engine('bigquery://my-project/ds?credentials_json={"type":"service_account"}')
        result = adapter.create_config_from_runtime(engine)
        assert isinstance(result, BigQueryConnectionProperties)
        assert isinstance(result.auth, BigQueryServiceAccountJsonAuth)
        assert result.auth.credentials_json == '{"type":"service_account"}'

    def test_unknown_query_params_in_additional_properties(self, adapter: BigQueryAdapter) -> None:
        engine = self._make_engine("bigquery://my-project/ds?timeout=30&max_results=100")
        result = adapter.create_config_from_runtime(engine)
        assert isinstance(result, BigQueryConnectionProperties)
        assert result.additional_properties == {"timeout": "30", "max_results": "100"}

    def test_known_keys_excluded_from_additional_properties(self, adapter: BigQueryAdapter) -> None:
        engine = self._make_engine("bigquery://my-project/ds?location=US&credentials_file=/k.json&timeout=30")
        result = adapter.create_config_from_runtime(engine)
        assert isinstance(result, BigQueryConnectionProperties)
        assert "location" not in result.additional_properties
        assert "credentials_file" not in result.additional_properties
        assert result.additional_properties == {"timeout": "30"}

    def test_raises_on_non_sqlalchemy_input(self, adapter: BigQueryAdapter) -> None:
        with pytest.raises(ValueError, match="SQLAlchemy Engine or Connection"):
            adapter.create_config_from_runtime(MagicMock())

    def test_raises_on_wrong_dialect(self, adapter: BigQueryAdapter) -> None:
        from sqlalchemy import Engine

        mock = MagicMock()
        mock.__class__ = Engine  # type: ignore[assignment]
        mock.dialect.name = "postgresql"
        with pytest.raises(ValueError, match="bigquery"):
            adapter.create_config_from_runtime(mock)


class TestRegisterInDuckdb:
    def test_default_auth_executes_correct_sql(
        self, adapter: BigQueryAdapter, default_config: BigQueryConnectionProperties
    ) -> None:
        mock_conn = MagicMock()
        adapter.register_in_duckdb(mock_conn, default_config, "bq_ds")
        mock_conn.execute.assert_any_call("INSTALL bigquery FROM community;")
        mock_conn.execute.assert_any_call("LOAD bigquery;")
        mock_conn.execute.assert_any_call("ATTACH 'project=my-project' AS \"bq_ds\" (TYPE bigquery, READ_ONLY);")
        mock_conn.execute.assert_any_call("SET disabled_optimizers='top_n';")

    def test_key_file_auth_includes_credentials_file(
        self, adapter: BigQueryAdapter, key_file_config: BigQueryConnectionProperties
    ) -> None:
        mock_conn = MagicMock()
        adapter.register_in_duckdb(mock_conn, key_file_config, "bq_ds")
        attach_call = mock_conn.execute.call_args_list[2]
        attach_sql: str = attach_call[0][0]
        assert "credentials_file=/path/to/key.json" in attach_sql
        assert "project=my-project" in attach_sql
        assert "dataset=my_dataset" in attach_sql

    def test_json_auth_includes_credentials_json(
        self, adapter: BigQueryAdapter, json_config: BigQueryConnectionProperties
    ) -> None:
        mock_conn = MagicMock()
        adapter.register_in_duckdb(mock_conn, json_config, "bq_ds")
        attach_call = mock_conn.execute.call_args_list[2]
        attach_sql: str = attach_call[0][0]
        assert "credentials_json=" in attach_sql
        assert "location=US" in attach_sql

    def test_raises_on_wrong_config_type(self, adapter: BigQueryAdapter) -> None:
        from databao_context_engine import DuckDBConnectionConfig

        mock_conn = MagicMock()
        with pytest.raises(ValueError, match="BigQueryConnectionProperties"):
            adapter.register_in_duckdb(mock_conn, DuckDBConnectionConfig(database_path="/tmp/db.duckdb"), "name")

    def test_install_load_attach_order(
        self, adapter: BigQueryAdapter, default_config: BigQueryConnectionProperties
    ) -> None:
        mock_conn = MagicMock()
        adapter.register_in_duckdb(mock_conn, default_config, "bq_ds")
        calls = [c[0][0] for c in mock_conn.execute.call_args_list]
        assert calls[0] == "INSTALL bigquery FROM community;"
        assert calls[1] == "LOAD bigquery;"
        assert calls[2].startswith("ATTACH")
        assert calls[3] == "SET disabled_optimizers='top_n';"


class TestConnectionString:
    def test_default_auth_only_has_project(self, default_config: BigQueryConnectionProperties) -> None:
        result = BigQueryAdapter._create_connection_string(default_config)
        assert result == "project=my-project"

    def test_key_file_auth_connection_string(self, key_file_config: BigQueryConnectionProperties) -> None:
        result = BigQueryAdapter._create_connection_string(key_file_config)
        assert "project=my-project" in result
        assert "dataset=my_dataset" in result
        assert "credentials_file=/path/to/key.json" in result

    def test_json_auth_connection_string(self, json_config: BigQueryConnectionProperties) -> None:
        result = BigQueryAdapter._create_connection_string(json_config)
        assert "project=my-project" in result
        assert "dataset=my_dataset" in result
        assert "location=US" in result
        assert 'credentials_json={"type":"service_account"}' in result

    def test_no_dataset_when_none(self) -> None:
        config = BigQueryConnectionProperties(project="p", dataset=None)
        result = BigQueryAdapter._create_connection_string(config)
        assert "dataset" not in result

    def test_additional_properties_included(self) -> None:
        config = BigQueryConnectionProperties(project="p", additional_properties={"timeout": "30"})
        result = BigQueryAdapter._create_connection_string(config)
        assert "timeout=30" in result
