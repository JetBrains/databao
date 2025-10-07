import abc
from abc import ABC
from typing import TYPE_CHECKING, Any

from langchain_core.language_models.chat_models import BaseChatModel
from pandas import DataFrame

from portus.pipe import Pipe

if TYPE_CHECKING:
    from portus.executor import Executor
    from portus.vizualizer import Visualizer


class Session(ABC):
    @abc.abstractmethod
    def add_db(self, connection: Any, *, name: str | None = None) -> None:
        pass

    @abc.abstractmethod
    def add_df(self, df: DataFrame, *, name: str | None = None) -> None:
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

    @property
    @abc.abstractmethod
    def llm(self) -> BaseChatModel:
        pass

    @property
    @abc.abstractmethod
    def executor(self) -> "Executor":
        pass

    @property
    @abc.abstractmethod
    def visualizer(self) -> "Visualizer":
        pass
