from typing import Any

from duckdb import DuckDBPyConnection
from sqlalchemy import Engine

from portus.agent.utils import register_sqlalchemy
from portus.data_source.data_source import DataSource


class DatabaseSource(DataSource):
    def __init__(self, sqlalchemy_engine: Engine, name: str):
        self.engine = sqlalchemy_engine
        self.name = name

    def register(self, connection: DuckDBPyConnection) -> None:
        register_sqlalchemy(connection, self.engine, self.name)

    def get_context(self) -> dict[str, Any]:
        return {}

    def get_schema(self) -> dict[str, str]:
        raise NotImplementedError
