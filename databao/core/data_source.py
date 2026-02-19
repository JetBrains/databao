from dataclasses import dataclass

import pandas as pd

from databao.databases import DBConnectionConfig


@dataclass
class DataSource:
    name: str
    context: str


@dataclass
class DFDataSource(DataSource):
    df: pd.DataFrame


@dataclass
class DBDataSource(DataSource):
    config: DBConnectionConfig
    connectable: bool = True


@dataclass
class Sources:
    dfs: dict[str, DFDataSource]
    dbs: dict[str, DBDataSource]
    additional_context: list[str]

    def contains(self, name: str) -> bool:
        return name in self.dfs or name in self.dbs

    @property
    def is_empty(self) -> bool:
        return not self.dfs and not self.dbs and not self.additional_context
