import abc
from abc import ABC
from typing import Optional, Any

from pandas import DataFrame

from portus.pipe import Pipe


class Session(ABC):
    @abc.abstractmethod
    def add_db(self, connection: Any, *, name: Optional[str] = None) -> None:
        pass

    @abc.abstractmethod
    def add_df(self, df: DataFrame, *, name: Optional[str] = None) -> None:
        pass

    @abc.abstractmethod
    def ask(self, query: str) -> Pipe:
        pass

    @property
    @abc.abstractmethod
    def dbs(self) -> dict[str, Any]:
        pass

    @property
    @abc.abstractmethod
    def dfs(self) -> dict[str, DataFrame]:
        pass

    @property
    @abc.abstractmethod
    def name(self) -> str:
        pass

