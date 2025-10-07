import abc
from abc import ABC
from typing import TYPE_CHECKING, Any

from pandas import DataFrame

from portus.data_source.data_source import DataSource
from portus.pipe.base_pipe import BasePipe

if TYPE_CHECKING:
    from portus.vizualizer import Visualizer


class BaseSession(ABC):
    """Session is a factory of Pipes. New Pipe is created after each 'ask' method call."""

    @abc.abstractmethod
    def add_db(self, connection: Any, *, name: str | None = None) -> None:
        pass

    @abc.abstractmethod
    def add_df(self, df: DataFrame, *, name: str | None = None) -> None:
        pass

    @abc.abstractmethod
    def ask(self, query: str) -> BasePipe:
        pass

    @property
    @abc.abstractmethod
    def sources(self) -> list[DataSource]:
        pass

    @property
    @abc.abstractmethod
    def name(self) -> str:
        pass

    @property
    @abc.abstractmethod
    def visualizer(self) -> "Visualizer":
        pass
