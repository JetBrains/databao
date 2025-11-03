import uuid
from typing import TYPE_CHECKING, Any

from pandas import DataFrame

from databao.core.opa import Opa

if TYPE_CHECKING:
    from databao.core.executor import ExecutionResult
    from databao.core.session import Session
    from databao.core.visualizer import VisualisationResult


class Pipe:
    """Pipe is one chat with an LLM. It contains a history of messages.
    It has no access to messages from other pipes even if they are in the same session.
    It returns the last obtained answer from the LLM.
    """

    def __init__(self, session: "Session", *, default_rows_limit: int = 1000):
        self._session = session
        self._default_rows_limit = default_rows_limit

        self._streaming_enabled = True

        self._data_materialized_rows: int | None = None
        self._data_result: ExecutionResult | None = None

        self._visualization_materialized = False
        self._visualization_result: VisualisationResult | None = None
        self._visualization_request: str | None = None

        # N.B. Pipes/Threads are currently append-only and cannot be "forked".
        self._opas_processed_count = 0
        self._opas: list[Opa] = []
        self._meta: dict[str, Any] = {}

        self._cache_scope = f"{self._session.name}/{uuid.uuid4()}"

    def _materialize_data(self, rows_limit: int | None) -> "ExecutionResult":
        # TODO Recompute on rows_limit change without recomputing the last Opa
        rows_limit = rows_limit if rows_limit else self._default_rows_limit
        new_opas = self._opas[self._opas_processed_count :]
        if len(new_opas) > 0 or rows_limit != self._data_materialized_rows:
            for opa in new_opas:
                self._data_result = self._session.executor.execute(
                    self._session,
                    opa,
                    rows_limit=rows_limit,
                    cache_scope=self._cache_scope,
                    stream=self._streaming_enabled,
                )
                self._meta.update(self._data_result.meta)
            self._opas_processed_count += len(new_opas)
            self._data_materialized_rows = rows_limit
        if self._data_result is None:
            raise RuntimeError("_data_result is None after materialization")
        return self._data_result

    def _materialize_visualization(self, request: str | None, rows_limit: int | None) -> "VisualisationResult":
        data = self._materialize_data(rows_limit)
        if not self._visualization_materialized or request != self._visualization_request:
            # TODO Cache visualization results as in Executor.execute()?
            self._visualization_result = self._session.visualizer.visualize(request, data)
            self._visualization_materialized = True
            self._visualization_request = request
            self._meta.update(self._visualization_result.meta)
            self._meta["plot_code"] = self._visualization_result.code  # maybe worth to expand as a property later
        if self._visualization_result is None:
            raise RuntimeError("_visualization_result is None after materialization")
        return self._visualization_result

    def df(self, *, rows_limit: int | None = None) -> DataFrame | None:
        return self._materialize_data(rows_limit if rows_limit else self._data_materialized_rows).df

    def plot(self, request: str | None = None, *, rows_limit: int | None = None) -> "VisualisationResult":
        # TODO Currently, we can't chain calls or maintain a "plot history": pipe.plot("red").plot("blue").
        #  We have to do pipe.plot("red"), but then pipe.plot("blue") is independent of the first call.
        return self._materialize_visualization(request, rows_limit if rows_limit else self._data_materialized_rows)

    def text(self) -> str:
        return self._materialize_data(self._data_materialized_rows).text

    def __str__(self) -> str:
        return self.text()

    def ask(self, query: str, *, stream: bool = True) -> "Pipe":
        self._opas.append(Opa(query=query))
        self._visualization_materialized = False
        self._streaming_enabled = stream
        return self

    @property
    def meta(self) -> dict[str, Any]:
        return self._meta

    @property
    def code(self) -> str | None:
        return self._materialize_data(self._data_materialized_rows).code
