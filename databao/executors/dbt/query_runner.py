from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable

import duckdb
import pandas as pd
from sqlalchemy import Engine, text

from databao.executors.dbt.utils import db_introspect


class QueryRunner(ABC):
    """Database-agnostic SQL executor for the dbt agent tools."""

    @abstractmethod
    def execute_to_df(self, sql: str) -> pd.DataFrame:
        """Execute a SQL query and return results as a DataFrame."""

    @abstractmethod
    def introspect(self) -> pd.DataFrame:
        """Return schema metadata as a DataFrame.

        Expected columns: catalog, schema, table, column_name, data_type,
        is_nullable, column_default, column_index, is_primary_key, fully_qualified_name.
        """

    @abstractmethod
    def close(self) -> None:
        """Release resources held by this executor."""


class DuckDbQueryRunner(QueryRunner):
    """SqlExecutor backed by a short-lived DuckDB connection."""

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn

    def execute_to_df(self, sql: str) -> pd.DataFrame:
        return self._conn.execute(sql).fetchdf()

    def introspect(self) -> pd.DataFrame:
        return db_introspect(self._conn)

    def close(self) -> None:
        self._conn.close()


class SqlAlchemyQueryRunner(QueryRunner):
    """SqlExecutor backed by a SQLAlchemy engine."""

    _INTROSPECT_SQL = """\
    SELECT
        table_catalog AS catalog,
        table_schema AS "schema",
        table_name AS "table",
        table_catalog || '.' || table_schema || '.' || table_name AS fully_qualified_name,
        column_name,
        data_type,
        is_nullable,
        column_default,
        ordinal_position AS column_index,
        FALSE AS is_primary_key
    FROM information_schema.columns
    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
    ORDER BY table_catalog, table_schema, table_name, ordinal_position
    """

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def execute_to_df(self, sql: str) -> pd.DataFrame:
        with self._engine.connect() as conn:
            return pd.read_sql(text(sql), conn)

    def introspect(self) -> pd.DataFrame:
        with self._engine.connect() as conn:
            return pd.read_sql(text(self._INTROSPECT_SQL), conn)

    def close(self) -> None:
        pass  # Engine manages its own pool


QueryRunnerFactory = Callable[[], QueryRunner]
