import abc
from abc import ABC
from typing import Optional, Any

from pandas import DataFrame

from portus.result import Result, LazyResult
from portus.data_executor import DataExecutor
from portus.vizualizer import Visualizer, DumbVisualizer
from portus.duckdb.agent import SimpleDuckDBAgenticExecutor
from langchain_core.language_models.chat_models import BaseChatModel


class Session(ABC):
    @abc.abstractmethod
    def add_db(self, connection: "Engine", *, name: Optional[str] = None) -> None:
        pass

    @abc.abstractmethod
    def add_df(self, df: DataFrame, *, name: Optional[str] = None) -> None:
        pass

    @abc.abstractmethod
    def ask(self, query: str, *, rows_limit: int = 100) -> Result:
        pass


class SessionImpl(Session):
    def __init__(
            self,
            llm: BaseChatModel,
            *,
            data_executor: DataExecutor = SimpleDuckDBAgenticExecutor(),
            visualizer: Visualizer = DumbVisualizer()
    ):
        self.__dbs: dict[str, Any] = {}
        self.__dfs: dict[str, DataFrame] = {}
        self.__llm = llm
        self.__data_executor = data_executor
        self.__visualizer = visualizer

    def add_db(self, connection: "Engine", *, name: Optional[str] = None) -> None:
        conn_name = name or f"db{len(self.__dbs) + 1}"
        self.__dbs[conn_name] = connection

    def add_df(self, df: DataFrame, *, name: Optional[str] = None) -> None:
        df_name = name or f"df{len(self.__dfs) + 1}"
        self.__dfs[df_name] = df

    def ask(self, query: str, *, rows_limit: int = 100) -> Result:
        return LazyResult(query, self.__llm, self.__data_executor, self.__visualizer, self.__dbs, self.__dfs,
                          rows_limit=rows_limit)
