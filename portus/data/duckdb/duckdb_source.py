from abc import ABC, abstractmethod

import pandas as pd
from _duckdb import DuckDBPyConnection
from sqlalchemy import Connection, Engine

from portus.data.duckdb.utils import register_sqlalchemy


class DuckDBSource(ABC):
    """A data source that can be natively attached/registered to a DuckDB connection."""

    def __init__(self, name: str, *, additional_context: str | None = None) -> None:
        self._name = name
        self._additional_context = additional_context

    @abstractmethod
    def register(self, connection: DuckDBPyConnection | Connection) -> None:
        raise NotImplementedError

    @property
    def name(self) -> str:
        return self._name

    @property
    def additional_context(self) -> str | None:
        return self._additional_context


class DatabaseSource(DuckDBSource):
    def __init__(self, name: str, sqlalchemy_engine: Engine, additional_context: str | None = None):
        super().__init__(name, additional_context=additional_context)
        self._engine = sqlalchemy_engine

    def register(self, connection: DuckDBPyConnection | Connection) -> None:
        register_sqlalchemy(connection, self._engine, self._name)

    @property
    def engine(self) -> Engine:
        return self._engine


class DataFrameSource(DuckDBSource):
    def __init__(self, name: str, df: pd.DataFrame, additional_context: str | None = None):
        super().__init__(name, additional_context=additional_context)
        self._df = df

    def register(self, connection: DuckDBPyConnection | Connection) -> None:
        if isinstance(connection, DuckDBPyConnection):
            connection.register(self._name, self._df)
        else:
            # TODO avoid materializing df
            # sqlalchemy inspection doesn't work for registered data frames, so we have to materialize them
            # connection.execute(sa.text("register(:name, :df)"), {"name": self.name, "df": self.df})
            self._df.to_sql(self._name, connection, index=False, if_exists="replace")

    @property
    def df(self) -> pd.DataFrame:
        return self._df
