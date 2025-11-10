import logging
from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel, ConfigDict

from databao.core.executor import ExecutionResult

_logger = logging.getLogger(__name__)


class VisualisationResult(BaseModel):
    """Result of turning data into a visualization.

    Attributes:
        text: Short description produced alongside the plot.
        meta: Additional details from the visualizer (debug info, quality flags, etc.).
        plot: Backend-specific plot object (Altair, matplotlib, etc.) or None if not drawable.
        code: Optional code used to generate the plot (e.g., Vega-Lite spec JSON).
    """

    text: str
    meta: dict[str, Any]
    plot: Any | None
    code: str | None

    # Immutable model; allow arbitrary plot types (e.g., matplotlib objects)
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    def _repr_mimebundle_(self, include: Any = None, exclude: Any = None) -> Any:
        """Return MIME bundle for IPython notebooks."""
        # See docs for the behavior of magic methods https://ipython.readthedocs.io/en/stable/config/integrating.html#custom-methods
        # If None is returned, IPython will fall back to repr()
        if self.plot is None:
            return None

        # Altair uses _repr_mimebundle_ as per: https://altair-viz.github.io/user_guide/custom_renderers.html
        if hasattr(self.plot, "_repr_mimebundle_"):
            return self.plot._repr_mimebundle_(include, exclude)

        mimebundle = {}
        if (plot_html := self._get_plot_html()) is not None:
            mimebundle["text/html"] = plot_html

        # TODO Handle all _repr_*_ methods
        # These are mostly for fallback representations
        if hasattr(self.plot, "_repr_png_"):
            mimebundle["image/png"] = self.plot._repr_png_()
        if hasattr(self.plot, "_repr_jpeg_"):
            mimebundle["image/jpeg"] = self.plot._repr_jpeg_()

        if len(mimebundle) > 0:
            return mimebundle
        return None

    def _get_plot_html(self) -> str | None:
        """Convert plot to HTML representation."""
        if self.plot is None:
            return None

        html_text: str | None = None
        if hasattr(self.plot, "_repr_mimebundle_"):
            bundle = self.plot._repr_mimebundle_()
            if isinstance(bundle, tuple):
                format_dict, _metadata_dict = bundle
            else:
                format_dict = bundle
            if format_dict is not None and "text/html" in format_dict:
                html_text = format_dict["text/html"]

        if html_text is None and hasattr(self.plot, "_repr_html_"):
            html_text = self.plot._repr_html_()

        if html_text is None and "matplotlib" not in str(type(self.plot)):
            # Don't warn for matplotlib as matplotlib has some magic that automatically displays plots in notebooks
            logging.warning(f"Failed to get a HTML representation for: {type(self.plot)}")

        return html_text


class Visualizer(ABC):
    """Abstract interface for converting data into plots/text.

    Implementations may ignore the request and choose an appropriate visualization.
    """

    @abstractmethod
    def visualize(self, request: str | None, data: ExecutionResult, *, stream: bool = True) -> VisualisationResult:
        """Produce a visualization for the given data and optional user request."""
        pass
