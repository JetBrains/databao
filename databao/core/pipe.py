import uuid
from typing import TYPE_CHECKING, Any, Self

from pandas import DataFrame

from databao.core.opa import Opa

if TYPE_CHECKING:
    from databao.core.executor import ExecutionResult
    from databao.core.session import Session
    from databao.core.visualizer import VisualisationResult


class Pipe:
    """A single conversational thread within a session.

    - Maintains its own message history (isolated from other pipes).
    - Materializes data and visualizations lazily on demand and caches results per pipe.
    - Exposes helpers to get the latest dataframe/text/plot/code.
    """

    def __init__(self, session: "Session", *, default_rows_limit: int = 1000, stream: bool = True):
        self._session = session
        self._default_rows_limit = default_rows_limit

        self._streaming_enabled = stream

        self._data_materialized_rows: int | None = None
        self._data_result: ExecutionResult | None = None

        self._visualization_materialized = False
        self._visualization_result: VisualisationResult | None = None
        self._visualization_request: str | None = None

        # N.B. Pipes/Threads are currently append-only and cannot be "forked".
        self._opas_processed_count = 0
        self._opas: list[Opa] = []
        self._meta: dict[str, Any] = {}

        # A unique cache scope so agents can store per-thread state (e.g., message history)
        self._cache_scope = f"{self._session.name}/{uuid.uuid4()}"

    def enable_streaming(self, value: bool) -> Self:
        """Enable streaming for this pipe (if supported by the executor)."""
        self._streaming_enabled = value
        return self

    def _materialize_data(self, rows_limit: int | None) -> "ExecutionResult":
        """Materialize latest data state by executing pending OPAs if needed.

        Reuses cached result unless new OPAs were added or the row limit changed.
        """
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
        """Materialize latest visualization for the given request and current data."""
        data = self._materialize_data(rows_limit)
        if not self._visualization_materialized or request != self._visualization_request:
            # TODO Cache visualization results as in Executor.execute()?
            self._visualization_result = self._session.visualizer.visualize(
                request, data, stream=self._streaming_enabled
            )
            self._visualization_materialized = True
            self._visualization_request = request
            self._meta.update(self._visualization_result.meta)
            self._meta["plot_code"] = self._visualization_result.code  # maybe worth to expand as a property later
        if self._visualization_result is None:
            raise RuntimeError("_visualization_result is None after materialization")
        return self._visualization_result

    def code(self) -> str | None:
        """Return the latest generated code."""
        return self._materialize_data(self._data_materialized_rows).code

    def df(self, *, rows_limit: int | None = None) -> DataFrame | None:
        """Return the latest dataframe, materializing data as needed.

        Args:
            rows_limit: Optional override for the number of rows to materialize.
        """
        return self._materialize_data(rows_limit if rows_limit else self._data_materialized_rows).df

    def plot(self, request: str | None = None, *, rows_limit: int | None = None) -> "VisualisationResult":
        """Generate or return the latest visualization for the current data.

        Args:
            request: Optional natural-language plotting request.
            rows_limit: Optional row limit for data materialization.
        """
        # TODO Currently, we can't chain calls or maintain a "plot history": pipe.plot("red").plot("blue").
        #  We have to do pipe.plot("red"), but then pipe.plot("blue") is independent of the first call.
        return self._materialize_visualization(request, rows_limit if rows_limit else self._data_materialized_rows)

    def text(self) -> str:
        """Return the latest textual answer from the executor/LLM."""
        return self._materialize_data(self._data_materialized_rows).text

    def __str__(self) -> str:
        if self._data_result is not None:
            return (
                f"Materialized {self.__class__.__name__} with "
                f"{len(self._data_result.df) if self._data_result.df is not None else 0} data rows."
            )
        else:
            return f"Unmaterialized {self.__class__.__name__}."

    def ask(self, query: str) -> Self:
        """Append a new user query to this pipe.

        Returns self to allow chaining (e.g., pipe.ask("..."))
        """
        self._opas.append(Opa(query=query))
        self._visualization_materialized = False
        return self

    @property
    def meta(self) -> dict[str, Any]:
        """Aggregated metadata from executor/visualizer for this pipe."""
        return self._meta
