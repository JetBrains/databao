from typing import Any

from pandas import DataFrame

from portus.core import ExecutionResult, Opa, Pipe, Session, VisualisationResult


class LazyPipe(Pipe):
    def __init__(self, session: Session, *, default_rows_limit: int = 1000):
        self.__session = session
        self.__default_rows_limit = default_rows_limit

        self.__data_materialized = False
        self.__data_materialized_rows: int | None = None
        self.__data_result: ExecutionResult | None = None
        self.__visualization_materialized = False
        self.__visualization_result: VisualisationResult | None = None
        self.__opas: list[Opa] = []
        self.__meta: dict[str, Any] = {}

    def __materialize_data(self, rows_limit: int | None) -> ExecutionResult:
        rows_limit = rows_limit if rows_limit else self.__default_rows_limit
        if not self.__data_materialized or rows_limit != self.__data_materialized_rows:
            self.__data_result = self.__session.executor.execute(
                self.__session, self.__opas, self.__session.llm, rows_limit=rows_limit
            )
            self.__data_materialized = True
            self.__data_materialized_rows = rows_limit
            self.__meta.update(self.__data_result.meta)
        if self.__data_result is None:
            raise RuntimeError("__data_result is None after materialization")
        return self.__data_result

    def __materialize_visualization(self, request: str, rows_limit: int | None) -> VisualisationResult:
        self.__materialize_data(rows_limit)
        if self.__data_result is None:
            raise RuntimeError("__data_result is None after materialization")
        if not self.__visualization_materialized:
            self.__visualization_result = self.__session.visualizer.visualize(
                request, self.__session.llm, self.__data_result
            )
            self.__visualization_materialized = True
            self.__meta.update(self.__visualization_result.meta)
            self.__meta["plot_code"] = self.__visualization_result.code  # maybe worth to expand as a property later
        if self.__visualization_result is None:
            raise RuntimeError("__visualization_result is None after materialization")
        return self.__visualization_result

    def df(self, *, rows_limit: int | None = None) -> DataFrame | None:
        return self.__materialize_data(rows_limit if rows_limit else self.__data_materialized_rows).df

    def plot(self, request: str = "visualize data", *, rows_limit: int | None = None) -> Any | None:
        return self.__materialize_visualization(
            request, rows_limit if rows_limit else self.__data_materialized_rows
        ).plot

    def text(self) -> str:
        return self.__materialize_data(self.__data_materialized_rows).text

    def __str__(self) -> str:
        return self.text()

    def ask(self, query: str) -> Pipe:
        self.__opas.append(Opa(query=query))
        return self

    @property
    def meta(self) -> dict[str, Any]:
        return self.__meta

    @property
    def code(self) -> str | None:
        return self.__materialize_data(self.__data_materialized_rows).code
