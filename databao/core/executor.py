import base64
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from pandas import DataFrame
from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from databao.core.opa import Opa
    from databao.core.session import Agent


class OutputModalityHints(BaseModel):
    """Hints on how to present the execution results.

    The Executor can optionally provide hints to influence how the execution results will be presented.
    The frontend code is responsible for adhering to these hints at the best effort level.
    """

    META_KEY: ClassVar[Literal["output_modality_hints"]] = "output_modality_hints"

    # Currently, the only modality that makes sense to request outside the Executor is visualization.
    # If Executor was responsible for plotting as well (instead of Visualizer), then we could fully control and
    # customize rendering in ExecutionResult._repr_mimebundle_.
    # But now we need hints to tell Pipe how to handle plotting.

    should_visualize: bool = False
    """Whether the execution results can be visualized."""
    visualization_prompt: str | None = None
    """Optional visualization prompt to be used by a Visualizer to generate a plot."""


class ExecutionResult(BaseModel):
    """Immutable result of a single agent/executor step.

    Attributes:
        text: Human-readable response to the last user query.
        meta: Arbitrary metadata collected during execution (debug info, timings, etc.).
        code: Text of generated code when applicable.
        df: Optional dataframe materialized by the executor.
    """

    text: str
    meta: dict[str, Any]
    code: str | None = None
    df: DataFrame | None = None

    # Pydantic v2 configuration: make the model immutable and allow pandas DataFrame
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    def _to_markdown(self) -> str:
        text_parts = []
        text_parts.append(self.text)
        if self.code is not None:
            text_parts.append(f"```\n{self.code}\n```")
        if self.df is not None:
            text_parts.append(self.df.head(10).to_markdown())
        return "\n\n".join(text_parts)

    def _to_html(self, *, plot_mimebundle: dict[str, Any] | None = None) -> str:
        import html

        modality_hints = self.meta.get(OutputModalityHints.META_KEY, OutputModalityHints())
        html_parts = {}

        text_html = f"<pre>{html.escape(self.text.strip())}</pre>"  # TODO markdown to HTML
        html_parts["text"] = text_html
        if self.code is not None:
            code = self.code.strip()
            if len(code) > 0:
                code_html = f"<pre><code>{html.escape(code)}</code></pre>"
                html_parts["code"] = code_html

        if self.df is not None and hasattr(self.df, "_repr_html_") and callable(self.df._repr_html_):
            # Use _repr_html_ to get the exact same output as if evaluating `df` in a notebook.
            # NB. PyCharm notebooks have special rendering if the output type is a pd.DataFrame,
            #  so returning just the correct mimetype is not enough to "unlock" all features, but
            #  it's the best we can do for now.
            df_html = self.df._repr_html_()
            html_parts["df"] = df_html

        if modality_hints.should_visualize and plot_mimebundle is not None:
            vis_html: str | None = None
            if (plot_html := plot_mimebundle.get("text/html")) is not None:
                vis_html = plot_html
            elif (png_bytes := plot_mimebundle.get("image/png")) is not None:
                png_base64 = base64.b64encode(png_bytes).decode("utf-8")
                vis_html = f'<img src="data:image/png;base64,{png_base64}" alt="Plot"/>'
            elif (jpeg_bytes := plot_mimebundle.get("image/jpeg")) is not None:
                jpeg_base64 = base64.b64encode(jpeg_bytes).decode("utf-8")
                vis_html = f'<img src="data:image/jpeg;base64,{jpeg_base64}" alt="Plot"/>'
            if vis_html is not None:
                html_parts["visualization"] = vis_html

        # Determine which section should be expanded by default
        expand_keys = ["visualization"] if "visualization" in html_parts else ["df", "text"]
        section_names = {"text": "Response", "df": "Data", "visualization": "Visualization", "code": "Code"}
        html_parts = {
            k: f"<details{' open' if k in expand_keys else ''}><summary>{section_names[k]}</summary>{v}</details>"
            for k, v in html_parts.items()
        }

        return "\n\n".join(html_parts.values())

    def _repr_mimebundle_(
        self, include: Any = None, exclude: Any = None, *, plot_mimebundle: dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        """Subclass ExecutionResult with its own _repr_mimebundle_ method to fully customize displaying outputs."""
        markdown = self._to_markdown()
        html_ = self._to_html(plot_mimebundle=plot_mimebundle)
        mimebundle = {
            "text/markdown": markdown,
            "text/html": html_,
        }
        return mimebundle


class Executor(ABC):
    """
    Defines the Executor interface as an abstract base class for execution of
    operations within a given agent.

    Methods:
        execute: Abstract method to execute a single OPA within an agent.
    """

    @abstractmethod
    def register_db(self, name: str, connection: Any) -> None:
        pass

    @abstractmethod
    def register_df(self, name: str, df: DataFrame) -> None:
        pass

    @abstractmethod
    def execute(
        self,
        session: "Agent",
        opa: "Opa",
        *,
        rows_limit: int = 100,
        cache_scope: str = "common_cache",
        stream: bool = True,
    ) -> ExecutionResult:
        """Execute a single OPA within an agent.

        Args:
            session: Active agent providing LLM, data connections, cache, etc.
            opa: User intent/query to process.
            rows_limit: Preferred row limit for data materialization (may be ignored by executors).
            cache_scope: Logical scope for caching per chat/thread.
            stream: Stream LLM output to stdout.
        """
        pass
