import uuid
from typing import TYPE_CHECKING, Any

from edaplot.data_utils import spec_add_data
from pandas import DataFrame
from typing_extensions import Self

from databao.core.executor import ExecutionResult, OutputModalityHints
from databao.core.opa import Opa

if TYPE_CHECKING:
    from databao.core.agent import Agent
    from databao.core.visualizer import VisualisationResult
    from databao.multimodal import MultimodalWidget


class Thread:
    """A single conversational thread within an agent.

    - Maintains its own message history (isolated from other threads).
    - Materializes data and visualizations eagerly or lazily and caches results per thread.
    - Exposes helpers to get the latest dataframe/text/plot/code.
    """

    def __init__(
        self,
        agent: "Agent",
        *,
        rows_limit: int = 1000,
        stream_ask: bool = True,
        stream_plot: bool = False,
        lazy: bool = False,
        auto_output_modality: bool = True,
    ):
        self._agent = agent
        self._default_rows_limit = rows_limit

        self._lazy_mode = lazy

        self._auto_output_modality = auto_output_modality
        """Automatically detect the appropriate modality to output based on the user's input. If False, you must
        manually call the appropriate ask/plot method.

        This allows .ask to be used for plotting, i.e. `ask("show a bar chart")` will result in a plot being generated.
        """

        self._stream_ask: bool | None = None
        self._stream_plot: bool | None = None
        self._default_stream_ask: bool = stream_ask
        self._default_stream_plot: bool = stream_plot

        self._data_materialized_rows: int | None = None
        self._data_result: ExecutionResult | None = None

        self._visualization_result: VisualisationResult | None = None
        self._visualization_request: str | None = None

        self._opas_processed_count: int = 0
        self._opas: list[list[Opa]] = []
        """Opas are grouped. Each group is processed independently."""

        self._meta: dict[str, Any] = {}

        # A unique cache scope so executors can store per-thread state (e.g., message history)
        self._cache_scope = f"{self._agent.name}/{uuid.uuid4()}"

    def _materialize_data(self, rows_limit: int | None) -> "ExecutionResult":
        """Materialize the latest data state by executing pending OPAs if needed."""
        new_opas = self._opas[self._opas_processed_count :]
        if len(new_opas) > 0:
            rows_limit = rows_limit if rows_limit else self._default_rows_limit
            stream = self._stream_ask if self._stream_ask is not None else self._default_stream_ask
            for opa in new_opas:
                self._data_result = self._agent.executor.execute(
                    opa,
                    cache=self._agent.cache.scoped(self._cache_scope),
                    llm_config=self._agent.llm_config,
                    sources=self._agent.sources,
                    rows_limit=rows_limit,
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
            self._visualization_result = self._agent.visualizer.visualize(request, data, stream=stream)
            self._visualization_request = request
            self._meta.update(self._visualization_result.meta)
            self._meta["plot_code"] = self._visualization_result.code  # maybe worth to expand as a property later
        if self._visualization_result is None:
            raise RuntimeError("_visualization_result is None after materialization")
        return self._visualization_result

    def _materialize(self, rows_limit: int | None) -> None:
        data_result = self._materialize_data(rows_limit)

        if not self._auto_output_modality:
            return

        # The Executor can provide output modality hints
        hints = data_result.meta.get(OutputModalityHints.META_KEY, OutputModalityHints())
        if not hints.should_visualize:
            return

        # Let the Visualizer recommend a plot based on the df if no prompt is provided (None)
        self.plot(hints.visualization_prompt)

    def text(self) -> str:
        """Return the latest textual answer from the executor/LLM."""
        return self._materialize_data(self._data_materialized_rows).text

    def code(self) -> str | None:
        """Return the latest generated code."""
        return self._materialize_data(self._data_materialized_rows).code

    def meta(self) -> dict[str, Any]:
        """Aggregated metadata from executor/visualizer for this thread."""
        self._materialize_data(self._data_materialized_rows)
        return self._meta

    def df(self, *, rows_limit: int | None = None) -> DataFrame | None:
        """Return the latest dataframe, materializing data as needed.

        Args:
            rows_limit: Optional override for the number of rows to materialize in lazy mode.
        """
        df = self._materialize_data(rows_limit if rows_limit else self._data_materialized_rows).df
        # Copy the dataframe to avoid state mutation from outside
        return df.copy() if df is not None else None

    def plot(
        self, request: str | None = None, *, rows_limit: int | None = None, stream: bool | None = None
    ) -> "VisualisationResult":
        """Generate or return the latest visualization for the current data.

        Args:
            request: Optional natural-language plotting request.
            rows_limit: Optional row limit for data materialization in lazy mode.
        """
        self._stream_plot = stream
        return self._materialize_visualization(request, rows_limit if rows_limit else self._data_materialized_rows)

    def html(self) -> str:
        """Generate HTML and open it in the browser.

        This method creates a standalone HTML file with the Vega-Lite chart, dataframe, and text
        and opens it in the default web browser.

        Returns:
            The URL that was opened in the browser.

        Raises:
            ValueError: If visualization generation fails.
        """
        from databao.visualizers.vega_chat import VegaChatResult

        plot = self.plot()

        if not isinstance(plot, VegaChatResult):
            raise ValueError(f"html() requires VegaChatVisualizer, got {type(plot).__name__}")

        if plot.spec is None or plot.spec_df is None:
            raise ValueError("Failed to generate visualization")

        from databao.multimodal import open_html_content

        df = self.df()
        df_html = df.to_html() if df is not None else "<i>No data</i>"
        visualizaton_text = self.text()
        spec_with_data = spec_add_data(plot.spec.copy(), plot.spec_df)

        return open_html_content(spec_with_data, df_html, visualizaton_text)

    def jupiter(self) -> "MultimodalWidget":
        """Generate jupiter widget.

        Returns:
            MultimodalWidget

        Raises:
            ValueError: If visualization generation fails.
        """
        from databao.multimodal import create_jupiter_widget

        return create_jupiter_widget(self)

    def ask(self, query: str, *, rows_limit: int | None = None, stream: bool | None = None) -> Self:
        """Append a new user query to this thread.

        Returns self to allow chaining (e.g., thread.ask("...")).

        Setting rows_limit has no effect in lazy mode.
        """
        # NB. A new Opa is created even if it's identical to the previous one.
        if self._opas_processed_count < len(self._opas):
            assert self._lazy_mode
            self._opas[-1].append(Opa(query=query))
        else:
            # Add new Opa group
            self._opas.append([Opa(query=query)])

        # Invalidate old results so they are not used by repr methods
        self._data_result = None
        self._visualization_result = None

        # If multiple .asks are chained, the last setting takes precedence.
        # Tracking the stream setting for each ask in a chain would not work with "opa-collocation".
        self._stream_ask = stream

        if not self._lazy_mode:
            self._materialize(rows_limit)

        return self

    def drop(self, n: int = 1) -> None:
        """Remove N last user queries from this thread along with the answer it produced."""
        sum_, n_groups = 0, 0
        for group in reversed(self._opas):
            sum_ += len(group)
            n_groups += 1
            if sum_ >= n:
                break

        n_materialized_group = n_groups - (len(self._opas) - self._opas_processed_count)

        # We need to drop `n` individual opas, combined into `n_groups` groups,
        # `n_materialized_group` of which are materialized.
        if sum_ == n:
            # Full drop of groups
            self._opas = self._opas[:-n_groups]
        else:
            full_groups = n_groups - 1
            if full_groups > 0:
                self._opas = self._opas[:-full_groups]
            self._opas[-1] = self._opas[-1][: -(sum_ - n)]

        self._agent.executor.drop_last_opa_group(self._agent.cache.scoped(self._cache_scope), n=n_materialized_group)
        self._opas_processed_count -= n_materialized_group

        if self._opas:
            print(
                f"Dropped last {n} operation{'s' if n > 1 else ''}. Last remaining operation:"
                f"\n{self._opas[-1][-1].query}"
            )
        else:
            print("Dropped all operations.")

    def __str__(self) -> str:
        if self._data_result is not None:
            bundle = self._data_result._repr_mimebundle_()
            if bundle is not None:
                if (text_markdown := bundle.get("text/markdown")) is not None:
                    return text_markdown  # type: ignore[no-any-return]
                elif (text_plain := bundle.get("text/plain")) is not None:
                    return text_plain  # type: ignore[no-any-return]
        return repr(self)

    def __repr__(self) -> str:
        if self._data_result is not None:
            return (
                f"Materialized {self.__class__.__name__} with "
                f"{len(self._data_result.df) if self._data_result.df is not None else 0} data rows."
            )
        else:
            return f"Unmaterialized {self.__class__.__name__}."

    def _repr_mimebundle_(self, include: Any = None, exclude: Any = None) -> dict[str, Any] | None:
        """Return MIME bundle for rendering in notebooks.

        No materialization is performed in this method. If using lazy mode, you must trigger materialization manually.
        """
        # See docs for the behavior of magic methods https://ipython.readthedocs.io/en/stable/config/integrating.html#custom-methods
        # If None is returned, IPython will fall back to repr()
        if self._data_result is None:
            return None
        modality_hints = self._data_result.meta.get(OutputModalityHints.META_KEY, OutputModalityHints())
        plot_bundle: dict[str, Any] | None = None
        if modality_hints.should_visualize and self._visualization_result is not None:
            plot_bundle = self._visualization_result._repr_mimebundle_(include, exclude)
        bundle = self._data_result._repr_mimebundle_(include, exclude, plot_mimebundle=plot_bundle)
        return bundle
