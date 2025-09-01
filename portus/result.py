import abc
from abc import ABC
from typing import Optional, Any

from pandas import DataFrame
from langchain_core.language_models.chat_models import BaseChatModel

from portus.vizualizer import Visualizer, VisualisationResult
from portus.data_executor import DataExecutor, DataResult


class Result(ABC):
    @abc.abstractmethod
    def df(self) -> DataFrame:
        pass

    @abc.abstractmethod
    def plot(self, *, request: str = "visualize data"):
        pass

    @abc.abstractmethod
    def meta(self) -> dict[str, Any]:
        pass


class LazyResult(Result):
    def __init__(
            self,
            query: str,
            llm: BaseChatModel,
            data_executor: DataExecutor,
            visualizer: Visualizer,
            dbs: dict[str, Any],
            dfs: dict[str, DataFrame],
            *,
            rows_limit: int = 100
    ):
        self.__query = query
        self.__llm = llm
        self.__dbs = dict(dbs)
        self.__dfs = dict(dfs)
        self.__data_executor = data_executor
        self.__visualizer = visualizer
        self.__rows_limit = rows_limit

        self.__data_materialized = False
        self.__data_result: Optional[DataResult] = None
        self.__visualization_materialized = False
        self.__visualization_result: Optional[VisualisationResult] = None

    def __materialize_data(self) -> DataResult:
        if not self.__data_materialized:
            self.__data_result = self.__data_executor.execute(self.__query, self.__llm, self.__dbs, self.__dfs,
                                                              rows_limit=self.__rows_limit)
            self.__data_materialized = True
        return self.__data_result

    def __materialize_visualization(self, request: str) -> VisualisationResult:
        self.__materialize_data()
        if not self.__visualization_materialized:
            self.__visualization_result = self.__visualizer.visualize(request, self.__llm, self.__data_result)
            self.__visualization_materialized = True
        return self.__visualization_result

    def df(self) -> DataFrame:
        return self.__materialize_data().df

    def plot(self, *, request: str = "visualize data"):
        return self.__materialize_visualization(request).plot

    def meta(self) -> dict[str, Any]:
        if self.__data_materialized:
            return self.__materialized_meta
        raise ValueError("Result is not materialized")

    def __str__(self):
        return self.__materialize_data().text
