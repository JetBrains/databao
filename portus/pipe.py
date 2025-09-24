import abc
from abc import ABC
from typing import Optional, Any

from pandas import DataFrame


class Pipe(ABC):
    @abc.abstractmethod
    def df(self, *, rows_limit: Optional[int] = None) -> Optional[DataFrame]:
        pass

    @abc.abstractmethod
    def plot(self, request: str = "visualize data", *, rows_limit: Optional[int] = None) -> Optional[Any]:
        pass

    @abc.abstractmethod
    def text(self) -> str:
        pass

    @abc.abstractmethod
    def ask(self, query: str) -> "Pipe":
        pass

    @property
    @abc.abstractmethod
    def meta(self) -> dict[str, Any]:
        pass

    @property
    @abc.abstractmethod
    def code(self) -> Optional[str]:
        pass
