from typing import Any

import pandas as pd
from duckdb import DuckDBPyConnection

from portus.data_source.data_source import DataSource


class DataFrameSource(DataSource):
    def __init__(self, df: pd.DataFrame, name: str):
        self.df = df
        self.name = name

    def register(self, connection: DuckDBPyConnection) -> None:
        connection.register(self.name, self.df)

    def get_context(self) -> dict[str, Any]:
        return {}

    def get_schema(self) -> dict[str, str]:
        raise NotImplementedError
