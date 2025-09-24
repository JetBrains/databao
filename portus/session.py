import abc
from abc import ABC
from typing import Optional, Any

from pandas import DataFrame

from portus.pipe import Pipe, LazyPipe
from portus.executor import Executor
from portus.vizualizer import Visualizer, DumbVisualizer
from portus.duckdb.agent import SimpleDuckDBAgenticExecutor
from langchain_core.language_models.chat_models import BaseChatModel


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


class SessionImpl(Session):
    def __init__(
            self,
            llm: BaseChatModel,
            *,
            data_executor: Executor = SimpleDuckDBAgenticExecutor(),
            visualizer: Visualizer = DumbVisualizer(),
            default_rows_limit: int = 1000
    ):
        self.__dbs: dict[str, Any] = {}
        self.__dfs: dict[str, DataFrame] = {}
        self.__llm = llm
        self.__data_executor = data_executor
        self.__visualizer = visualizer
        self.__default_rows_limit = default_rows_limit

    def add_db(self, connection: Any, *, name: Optional[str] = None) -> None:
        conn_name = name or f"db{len(self.__dbs) + 1}"
        self.__dbs[conn_name] = connection

    def add_df(self, df: DataFrame, *, name: Optional[str] = None) -> None:
        df_name = name or f"df{len(self.__dfs) + 1}"
        self.__dfs[df_name] = df

    def ask(self, query: str) -> Pipe:
        return LazyPipe(self.__llm, self.__data_executor, self.__visualizer, self.__dbs, self.__dfs,
                        default_rows_limit=self.__default_rows_limit).ask(query)
