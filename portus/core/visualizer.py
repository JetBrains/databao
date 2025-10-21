import logging
from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel, ConfigDict

from portus.core.executor import ExecutionResult

_logger = logging.getLogger(__name__)


class VisualisationResult(BaseModel):
    text: str
    meta: dict[str, Any]
    plot: Any | None
    code: str | None

    # Immutable model; allow arbitrary plot types (e.g., matplotlib objects)
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    def _repr_html_(self) -> str | None:
        """Return HTML representation for IPython notebooks."""
        # See docs for the behavior of magic methods https://ipython.readthedocs.io/en/stable/config/integrating.html#custom-methods
        # If None is returned, IPython will fall back to repr()
        return self._get_plot_html()

    def _get_plot_html(self) -> str | None:
        """Convert plot to HTML representation."""
        if self.plot is None:
            return None

        html_text: str | None = None
        if hasattr(self.plot, "_repr_mimebundle_"):
            # Altair always uses _repr_mimebundle_ as per: https://altair-viz.github.io/user_guide/custom_renderers.html
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
    @abstractmethod
    def visualize(self, request: str | None, data: ExecutionResult) -> VisualisationResult:
        pass
