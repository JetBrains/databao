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
    - Materializes data and visualizations eagerly or lazily and caches results per pipe.
    - Exposes helpers to get the latest dataframe/text/plot/code.
    """

    def __init__(
        self,
        session: "Session",
        *,
        default_rows_limit: int = 1000,
        default_stream_ask: bool = True,
        default_stream_plot: bool = False,
        lazy: bool = False,
    ):
        self._session = session
        self._default_rows_limit = default_rows_limit
        self._lazy_mode = lazy

        self._stream_ask: bool | None = None
        self._stream_plot: bool | None = None
        self._default_stream_ask: bool = default_stream_ask
        self._default_stream_plot: bool = default_stream_plot

        self._data_materialized_rows: int | None = None
        self._data_result: ExecutionResult | None = None

        self._visualization_result: VisualisationResult | None = None
        self._visualization_request: str | None = None

        # N.B. Pipes/Threads are currently append-only and cannot be "forked".
        self._opas_processed_count = 0
        self._opas: list[Opa] = []
        self._meta: dict[str, Any] = {}

        # A unique cache scope so agents can store per-thread state (e.g., message history)
        self._cache_scope = f"{self._session.name}/{uuid.uuid4()}"

    def _materialize_data(self, rows_limit: int | None) -> "ExecutionResult":
        """Materialize latest data state by executing pending OPAs if needed.

        Reuses cached result unless new OPAs were added or the row limit changed.
        """
        # TODO Recompute on rows_limit change without recomputing the last Opa
        rows_limit = rows_limit if rows_limit else self._default_rows_limit
        new_opas = self._opas[self._opas_processed_count :]
        if len(new_opas) > 0 or rows_limit != self._data_materialized_rows:
            stream = self._stream_ask if self._stream_ask is not None else self._default_stream_ask
            for opa in new_opas:
                self._data_result = self._session.executor.execute(
                    self._session,
                    opa,
                    rows_limit=rows_limit,
                    cache_scope=self._cache_scope,
                    stream=stream,
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
        if self._visualization_result is None or request != self._visualization_request:
            # TODO Cache visualization results as in Executor.execute()?
            stream = self._stream_plot if self._stream_plot is not None else self._default_stream_plot
            self._visualization_result = self._session.visualizer.visualize(request, data, stream=stream)
            self._visualization_request = request
            self._meta.update(self._visualization_result.meta)
            self._meta["plot_code"] = self._visualization_result.code  # maybe worth to expand as a property later
        if self._visualization_result is None:
            raise RuntimeError("_visualization_result is None after materialization")
        return self._visualization_result

    def code(self) -> str | None:
        """Return the latest generated code."""
        return self._materialize_data(self._data_materialized_rows).code

    def meta(self) -> dict[str, Any]:
        """Aggregated metadata from executor/visualizer for this pipe."""
        self._materialize_data(self._data_materialized_rows)
        return self._meta

    def df(self, *, rows_limit: int | None = None) -> DataFrame | None:
        """Return the latest dataframe, materializing data as needed.

        Args:
            rows_limit: Optional override for the number of rows to materialize.
        """
        return self._materialize_data(rows_limit if rows_limit else self._data_materialized_rows).df

    def plot(
        self, request: str | None = None, *, rows_limit: int | None = None, stream: bool | None = None
    ) -> "VisualisationResult":
        """Generate or return the latest visualization for the current data.

        Args:
            request: Optional natural-language plotting request.
            rows_limit: Optional row limit for data materialization.
        """
        # TODO Currently, we can't chain calls or maintain a "plot history": pipe.plot("red").plot("blue").
        #  We have to do pipe.plot("red"), but then pipe.plot("blue") is independent of the first call.
        # Plotting is always computed eagerly.
        self._stream_plot = stream
        return self._materialize_visualization(request, rows_limit if rows_limit else self._data_materialized_rows)

    def text(self) -> str:
        """Return the latest textual answer from the executor/LLM."""
        return self._materialize_data(self._data_materialized_rows).text

    def ask(self, query: str, *, rows_limit: int | None = None, stream: bool | None = None) -> Self:
        """Append a new user query to this pipe.

        Returns self to allow chaining (e.g., pipe.ask("..."))
        """
        # NB. A new Opa is created even if it's identical to the previous one.
        self._opas.append(Opa(query=query))

        # Invalidate old results so they are not used by repr methods
        self._data_result = None
        self._visualization_result = None

        # If multiple .asks are chained, the last setting takes precedence.
        # Tracking the stream setting for each ask in a chain would not work with "opa-collocation".
        self._stream_ask = stream

        if not self._lazy_mode:
            self._materialize_data(rows_limit)

        return self

    def __repr__(self) -> str:
        if self._data_result is not None:
            return (
                f"Materialized {self.__class__.__name__} with "
                f"{len(self._data_result.df) if self._data_result.df is not None else 0} data rows."
            )
        else:
            return f"Unmaterialized {self.__class__.__name__}."

    def _repr_mimebundle_(self, include: Any = None, exclude: Any = None) -> Any:
        """Return MIME bundle for rendering in notebooks.

        No materialization is performed in this method. If using lazy mode, you must trigger materialization manually.
        """
        # See docs for the behavior of magic methods https://ipython.readthedocs.io/en/stable/config/integrating.html#custom-methods

        # Prioritize plots over other modalities
        if self._visualization_result is not None:
            plot_mimebundle = self._visualization_result._repr_mimebundle_(include, exclude)
            if plot_mimebundle is not None:
                return plot_mimebundle

        # If None is returned, IPython will fall back to repr()
        if self._data_result is None:
            return None

        text_parts = [self._data_result.text]
        if self._data_result.code is not None:
            text_parts.append(self._data_result.code)

        mimebundle = {}
        if (df := self._data_result.df) is not None:
            if hasattr(df, "_repr_html_") and callable(df._repr_html_):
                # Use _repr_html_ to get the exact same output as if evaluating `df` in a notebook.
                # NB. PyCharm notebooks have special rendering if the output type is a pd.DataFrame,
                #  so returning just the correct mimetype is not enough to "unlock" all features, but
                #  it's the best we can do for now.
                mimebundle["text/html"] = df._repr_html_()
            text_parts.append(df.to_string())

        # Fallback text-only representation
        mimebundle["text/plain"] = "\n".join(text_parts)
        return mimebundle
