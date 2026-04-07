"""Tests for SeparateExecutor and related helpers."""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from databao_context_engine import SnowflakeConnectionProperties, SnowflakePasswordAuth

from databao.agent.core.data_source import DBDataSource, Sources
from databao.agent.duckdb.schema_inspection import ColumnInfo, TableInfo
from databao.agent.executors.separate.graph import _validate_read_only
from databao.agent.executors.separate.separate_executor import SeparateExecutor, _summarize

# ---------------------------------------------------------------------------
# _validate_read_only — allowed statements
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1",
        "select * from t",
        "  SELECT * FROM t",
        "WITH cte AS (SELECT 1) SELECT * FROM cte",
        "SHOW TABLES",
        "DESCRIBE TABLE t",
        "EXPLAIN SELECT 1",
        "(SELECT 1 UNION SELECT 2)",
    ],
)
def test_validate_read_only_allows_select(sql: str) -> None:
    _validate_read_only(sql)  # should not raise


# ---------------------------------------------------------------------------
# _validate_read_only — forbidden statements
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql,keyword",
    [
        ("INSERT INTO t VALUES (1)", "INSERT"),
        ("  insert into t values (1)", "INSERT"),
        ("UPDATE t SET x=1", "UPDATE"),
        ("DELETE FROM t", "DELETE"),
        ("DROP TABLE t", "DROP"),
        ("ALTER TABLE t ADD COLUMN x INT", "ALTER"),
        ("CREATE TABLE t (x INT)", "CREATE"),
        ("TRUNCATE TABLE t", "TRUNCATE"),
        ("MERGE INTO t USING s ON t.id=s.id WHEN MATCHED THEN DELETE", "MERGE"),
        ("GRANT SELECT ON t TO role_x", "GRANT"),
        ("REVOKE SELECT ON t FROM role_x", "REVOKE"),
        ("CALL my_procedure()", "CALL"),
        ("EXEC my_procedure()", "EXEC"),
        ("EXECUTE my_procedure()", "EXECUTE"),
        ("BEGIN TRANSACTION", "BEGIN"),
        ("COMMIT", "COMMIT"),
        ("ROLLBACK", "ROLLBACK"),
        ("COPY INTO t FROM @stage", "COPY"),
    ],
)
def test_validate_read_only_rejects_write_statements(sql: str, keyword: str) -> None:
    with pytest.raises(ValueError, match=f"Got statement starting with '{keyword}'"):
        _validate_read_only(sql)


# ---------------------------------------------------------------------------
# SeparateExecutor._init_sources_from_domain — engine creation routing
# ---------------------------------------------------------------------------


def _make_snowflake_config(**kwargs: Any) -> SnowflakeConnectionProperties:
    defaults = dict(account="acct", user="usr", database="db", warehouse="wh")
    return SnowflakeConnectionProperties(**{**defaults, **kwargs}, auth=SnowflakePasswordAuth(password="pw"))


def _make_domain_mock(dbs: dict[str, Any]) -> MagicMock:
    """Build a mock _Domain with the given db sources."""
    from databao.agent.core.domain import _Domain

    domain = MagicMock(spec=_Domain)
    db_sources: dict[str, DBDataSource] = {}
    for name, config in dbs.items():
        src = MagicMock(spec=DBDataSource)
        src.config = config
        db_sources[name] = src
    domain.sources = Sources(dbs=db_sources, dfs={}, dbts={}, additional_description=[])
    return domain


def test_init_sources_stores_engine_when_created() -> None:
    executor = SeparateExecutor()
    config = _make_snowflake_config()
    domain = _make_domain_mock({"mydb": config})
    fake_engine = MagicMock()

    with patch(
        "databao.agent.executors.separate.separate_executor.try_create_sqlalchemy_engine",
        return_value=fake_engine,
    ):
        executor._init_sources_from_domain(domain)

    assert "mydb" in executor._sa_engines
    assert executor._sa_engines["mydb"] is fake_engine


def test_init_sources_logs_warning_when_engine_is_none() -> None:
    executor = SeparateExecutor()
    config = _make_snowflake_config()
    domain = _make_domain_mock({"mydb": config})

    with (
        patch(
            "databao.agent.executors.separate.separate_executor.try_create_sqlalchemy_engine",
            return_value=None,
        ),
        patch(
            "databao.agent.executors.separate.separate_executor.get_db_type",
            return_value=MagicMock(full_type="snowflake"),
        ),
        patch("databao.agent.executors.separate.separate_executor._LOGGER") as mock_logger,
    ):
        executor._init_sources_from_domain(domain)

    assert "mydb" not in executor._sa_engines
    assert "mydb" in executor._registered_dbs
    mock_logger.warning.assert_called_once()


def test_init_sources_skips_already_registered_dbs() -> None:
    executor = SeparateExecutor()
    config = _make_snowflake_config()
    domain = _make_domain_mock({"mydb": config})
    fake_engine = MagicMock()

    with patch(
        "databao.agent.executors.separate.separate_executor.try_create_sqlalchemy_engine",
        return_value=fake_engine,
    ) as mock_create:
        executor._init_sources_from_domain(domain)
        executor._init_sources_from_domain(domain)

    # Should only be called once — second call skips already-registered db
    assert mock_create.call_count == 1


# ---------------------------------------------------------------------------
# SeparateExecutor._inspect_database_schema — error handling
# ---------------------------------------------------------------------------


def test_inspect_schema_returns_no_tables_when_inspection_fails() -> None:
    executor = SeparateExecutor()
    config = _make_snowflake_config()

    # Manually register a db and engine
    src = MagicMock(spec=DBDataSource)
    src.config = config
    executor._registered_dbs["mydb"] = src
    executor._sa_engines["mydb"] = MagicMock()

    with patch(
        "databao.agent.executors.separate.separate_executor.inspect_sqlalchemy_schema",
        side_effect=RuntimeError("connection refused"),
    ):
        result = executor._inspect_database_schema()

    assert result == "(no tables found)"


def test_inspect_schema_prefixes_tables_with_datasource_name() -> None:
    executor = SeparateExecutor()

    src = MagicMock(spec=DBDataSource)
    executor._registered_dbs["sales_db"] = src
    executor._sa_engines["sales_db"] = MagicMock()

    table = TableInfo(
        table_catalog="ORIGINAL_CAT",
        columns_catalog="ORIGINAL_CAT",
        schema="PUBLIC",
        name="ORDERS",
        columns=[ColumnInfo(name="id", data_type="NUMBER")],
    )

    with patch(
        "databao.agent.executors.separate.separate_executor.inspect_sqlalchemy_schema",
        return_value=[table],
    ):
        result = executor._inspect_database_schema()

    # The schema summary should use the datasource name, not the original catalog
    assert "sales_db" in result
    assert "ORDERS" in result


# ---------------------------------------------------------------------------
# _summarize helpers
# ---------------------------------------------------------------------------


def test_summarize_empty_returns_placeholder() -> None:
    assert _summarize([]) == "(no tables found)"


def test_summarize_non_empty_contains_table_name() -> None:
    tables = [
        TableInfo(
            table_catalog="cat",
            columns_catalog="cat",
            schema="PUBLIC",
            name="MY_TABLE",
            columns=[ColumnInfo(name="col1", data_type="TEXT")],
        )
    ]
    result = _summarize(tables)
    assert "MY_TABLE" in result
