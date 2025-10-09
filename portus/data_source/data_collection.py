from typing import Any

import duckdb
import pandas as pd
from duckdb import DuckDBPyConnection
from sqlalchemy import Engine

from portus.data_source.data_source import DataSource
from portus.data_source.database_source import DatabaseSource
from portus.data_source.dataframe_source import DataFrameSource


class DataCollection:
    """Collection of data sources.
    Provides a single SQL connection to all sources."""

    def __init__(self) -> None:
        self._sources: list[DataSource] = []

    def add_df(self, df: pd.DataFrame, name: str | None = None) -> None:
        df_name = name or f"df_{len(self._sources) + 1}"
        source = DataFrameSource(df, df_name)
        self._sources.append(source)

    def add_db(self, engine: Engine, name: str | None = None) -> None:
        name = name or f"db_{len(self._sources) + 1}"
        source = DatabaseSource(engine, name)
        self._sources.append(source)

    def get_connection(self) -> DuckDBPyConnection:
        con = duckdb.connect(database=":memory:", read_only=False)
        for source in self._sources:
            source.register(con)
        return con

    def get_sources(self) -> list[DataSource]:
        return self._sources

    def get_context(self) -> dict[str, Any]:
        return {}
