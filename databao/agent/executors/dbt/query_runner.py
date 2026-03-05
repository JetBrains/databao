from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable

import duckdb
import pandas as pd
from sqlalchemy import Engine, text


class QueryRunner(ABC):
    """Database-agnostic SQL executor for the dbt agent tools."""

    @abstractmethod
    def execute_to_df(self, sql: str) -> pd.DataFrame:
        """Execute a SQL query and return results as a DataFrame."""

    @abstractmethod
    def close(self) -> None:
        """Release resources held by this executor."""


class DuckDbQueryRunner(QueryRunner):
    """SqlExecutor backed by a short-lived DuckDB connection."""

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn

    def execute_to_df(self, sql: str) -> pd.DataFrame:
        return self._conn.execute(sql).fetchdf()

    def close(self) -> None:
        self._conn.close()


class SqlAlchemyQueryRunner(QueryRunner):
    """SqlExecutor backed by a SQLAlchemy engine."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def execute_to_df(self, sql: str) -> pd.DataFrame:
        with self._engine.connect() as conn:
            return pd.read_sql(text(sql), conn)

    def close(self) -> None:
        pass  # Engine manages its own pool


QueryRunnerFactory = Callable[[], QueryRunner]
