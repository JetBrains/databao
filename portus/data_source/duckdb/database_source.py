from typing import Any

import sqlalchemy as sa
from duckdb import DuckDBPyConnection

from portus.data_source.duckdb.duckdb_source import DuckDBSource
from portus.data_source.duckdb.utils import register_sqlalchemy


class DatabaseSource(DuckDBSource):
    def __init__(self, sqlalchemy_engine: sa.Engine, name: str):
        self._engine = sqlalchemy_engine
        self._name = name

    def register(self, connection: DuckDBPyConnection | sa.Connection) -> None:
        register_sqlalchemy(connection, self._engine, self._name)

    def get_context(self) -> dict[str, Any]:
        return {}
