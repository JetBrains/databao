import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any

from langchain_core.messages import BaseMessage
from pydantic import BaseModel, ConfigDict, Field

from databao.core.executor import ExecutionResult

_logger = logging.getLogger(__name__)


class HistoryMode(str, Enum):
    """Controls how much conversation history is passed to the visualization agent."""

    NONE = "none"
    """No history: only the current visualization request (default)."""
    LAST = "last"
    """Messages from the last HumanMessage to the end of the conversation."""
    FULL = "full"
    """All messages (excluding the SystemMessage)."""


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

    visualizer: "Visualizer | None" = Field(exclude=True)
    """Reference to the Visualizer that produced this result. Not serializable."""

    # Immutable model; allow arbitrary plot types (e.g., matplotlib objects)
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    def edit(self, request: str, *, stream: bool = False) -> "VisualisationResult":
        """Edit this visualization with a natural language request.

        Syntactic sugar for the `Visualizer.edit` method.
        """
        if self.visualizer is None:
            # Forbid using `.edit` after deserialization
            raise RuntimeError("Visualizer is not set")
        return self.visualizer.edit(request, self, stream=stream)

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
    """Abstract interface for converting data into plots using natural language."""

    def __init__(self, *, history_mode: HistoryMode = HistoryMode.NONE):
        self.history_mode = history_mode

    def visualize(self, request: str | None, data: ExecutionResult, *, stream: bool = False) -> VisualisationResult:
        """Produce a visualization for the given data and optional user request.

        Extracts conversation history from *data* based on :attr:`history_mode` and
        delegates to :meth:`_visualize`.
        """
        history = self._extract_history(data)
        return self._visualize(request, data, history=history, stream=stream)

    @abstractmethod
    def _visualize(
        self,
        request: str | None,
        data: ExecutionResult,
        *,
        history: list[BaseMessage],
        stream: bool = False,
    ) -> VisualisationResult:
        """Produce a visualization for the given data and optional user request.

        Args:
            request: Natural-language visualization request.
            data: The execution result containing the dataframe and metadata.
            history: Conversation history as a list of LangChain ``BaseMessage``
                objects, already filtered according to :attr:`history_mode`.
            stream: Whether to stream LLM output.
        """
        pass

    @abstractmethod
    def edit(self, request: str, visualization: VisualisationResult, *, stream: bool = False) -> VisualisationResult:
        """Refine a prior visualization with a natural language request."""
        pass

    def _extract_history(self, data: ExecutionResult) -> list[BaseMessage]:
        """Extract conversation messages from executor results according to :attr:`history_mode`.

        Returns a (possibly empty) list of LangChain ``BaseMessage`` objects.

        - ``NONE``  -- empty list
        - ``LAST``  -- messages from the last ``HumanMessage`` to the end
        - ``FULL``  -- all messages, excluding the ``SystemMessage``
        """
        if self.history_mode == HistoryMode.NONE:
            return []

        raw_messages: list[Any] = data.meta.get("messages", [])
        if not raw_messages:
            return []

        # Lazy import to avoid a hard dependency on langchain in core
        from langchain_core.messages import HumanMessage, SystemMessage

        # Filter out SystemMessage (there might be none)
        messages: list[BaseMessage] = [m for m in raw_messages if not isinstance(m, SystemMessage)]

        if self.history_mode == HistoryMode.FULL:
            return messages

        # LAST: from the last HumanMessage to the end
        last_human_idx = None
        for i in range(len(messages) - 1, -1, -1):
            if isinstance(messages[i], HumanMessage):
                last_human_idx = i
                break

        if last_human_idx is not None:
            return messages[last_human_idx:]

        return messages
