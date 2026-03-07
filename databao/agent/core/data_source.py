from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from databao.agent.databases import DBConnectionConfig


@dataclass
class DataSource:
    name: str
    description: str


@dataclass
class DBTDataSource(DataSource):
    dir: Path


@dataclass
class DFDataSource(DataSource):
    df: pd.DataFrame


@dataclass
class DBDataSource(DataSource):
    config: DBConnectionConfig


@dataclass
class Sources:
    dfs: dict[str, DFDataSource]
    dbs: dict[str, DBDataSource]
    dbts: dict[str, DBTDataSource]
    additional_description: list[str]

    def contains(self, name: str) -> bool:
        return name in self.dfs or name in self.dbs or name in self.dbts

    @property
    def is_empty(self) -> bool:
        return not self.dfs and not self.dbs and not self.dbts and not self.additional_description
