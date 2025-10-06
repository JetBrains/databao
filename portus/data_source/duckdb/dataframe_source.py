from typing import Any

import pandas as pd
import sqlalchemy as sa
from duckdb import DuckDBPyConnection

from portus.data_source.duckdb.duckdb_source import DuckDBSource


class DataFrameSource(DuckDBSource):
    def __init__(self, df: pd.DataFrame, name: str):
        self.df = df
        self.name = name

    def register(self, connection: DuckDBPyConnection | sa.Connection) -> None:
        if isinstance(connection, DuckDBPyConnection):
            connection.register(self.name, self.df)
        else:
            # sqlalchemy inspection doesn't work for registered data frames, so we have to materialize it
            # connection.execute(sa.text("register(:name, :df)"), {"name": self.name, "df": self.df})
            self.df.to_sql(self.name, connection, index=False, if_exists="replace")

    def get_context(self) -> dict[str, Any]:
        return {}
