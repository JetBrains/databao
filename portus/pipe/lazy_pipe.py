from typing import Any

from pandas import DataFrame

from portus.agent.base_agent import ExecutionResult
from portus.opa import Opa
from portus.pipe.base_pipe import BasePipe
from portus.session.base_session import BaseSession
from portus.vizualizer import VisualisationResult


class LazyPipe(BasePipe):
    def __init__(self, session: BaseSession, *, default_rows_limit: int = 1000):
        self.__session = session
        self.__default_rows_limit = default_rows_limit

        self.__data_materialized = False
        self.__data_materialized_rows: int | None = None
        self.__data_result: ExecutionResult | None = None
        self.__visualization_materialized = False
        self.__visualization_result: VisualisationResult | None = None
        self.__opas: list[Opa] = []
        self.__meta: dict[str, Any] = {}

    def __materialize_data(self, rows_limit: int | None) -> ExecutionResult | None:
        rows_limit = rows_limit if rows_limit else self.__default_rows_limit
        if not self.__data_materialized or rows_limit != self.__data_materialized_rows:
            # This needs to be implemented properly based on actual session interface
            # For now, return None as placeholder
            self.__data_result = None
            self.__data_materialized = True
            self.__data_materialized_rows = rows_limit
            if self.__data_result and self.__data_result.meta:
                self.__meta.update(self.__data_result.meta)
        return self.__data_result

    def __materialize_visualization(self, request: str, rows_limit: int | None) -> VisualisationResult | None:
        self.__materialize_data(rows_limit)
        if not self.__visualization_materialized:
            # This needs to be implemented properly based on actual session interface
            # For now, return None as placeholder
            self.__visualization_result = None
            self.__visualization_materialized = True
            if self.__visualization_result:
                self.__meta.update(self.__visualization_result.meta)
                self.__meta["plot_code"] = self.__visualization_result.code  # maybe worth to expand as a property later
        return self.__visualization_result

    @property
    def df(self) -> DataFrame | None:
        result = self.__materialize_data(self.__data_materialized_rows)
        return result.df if result else None

    @property
    def plot(self) -> Any | None:
        result = self.__materialize_visualization("visualize data", self.__data_materialized_rows)
        return result.plot if result else None

    @property
    def text(self) -> str:
        result = self.__materialize_data(self.__data_materialized_rows)
        return result.text if result else ""

    def __str__(self) -> str:
        return self.text

    def ask(self, query: str) -> "LazyPipe":
        self.__opas.append(Opa(query=query))
        return self

    @property
    def meta(self) -> dict[str, Any]:
        return self.__meta

    @property
    def code(self) -> str | None:
        # ExecutionResult doesn't have a code attribute, return from meta instead
        return self.__meta.get("plot_code")
