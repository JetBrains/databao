from dataclasses import dataclass

import pandas as pd

from databao.databases import DBConnection


@dataclass
class DataSource:
    name: str
    context: str


@dataclass
class DFDataSource(DataSource):
    df: pd.DataFrame


@dataclass
class DBDataSource(DataSource):
    db_connection: DBConnection


@dataclass
class Sources:
    dfs: dict[str, DFDataSource]
    dbs: dict[str, DBDataSource]
    additional_context: list[str]
