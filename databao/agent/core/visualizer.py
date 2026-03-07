import logging
from abc import ABC, abstractmethod
from enum import StrEnum
from typing import Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field

from databao.agent.core.executor import ExecutionResult

_logger = logging.getLogger(__name__)


class HistoryMode(StrEnum):
    """Controls how much conversation history is prepended to the visualization request.

    The DataFrame and the visualization instructions are always passed to the Visualizer.
    This setting controls how much preceding conversation context is added to the request.
    """

    NONE = "none"
    """No history — only the visualization instructions."""

    LAST_QUESTION = "last_question"
    """Only the last user question."""

    LAST_QUESTION_ANSWER = "last_question_answer"
    """Last user question and the executor's final answer."""

    ALL_QUESTIONS = "all_questions"
    """All user questions from the conversation."""


class VisualisationResult(BaseModel):
    """Result of turning data into a visualization.

    Attributes:
        text: Short description produced alongside the plot.
        meta: Additional details from the visualizer (debug info, quality flags, etc.).
        plot: Backend-specific plot object (Altair, matplotlib, etc.) or None if not drawable.
        code: Optional code used to generate the plot (e.g., Vega-Lite spec JSON).
    """

    META_PLOT_MESSAGES_KEY: ClassVar[Literal["plot_messages"]] = "plot_messages"
    """Key in `meta` that stores the visualizer's internal message history."""

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

    def __init__(self, *, history_mode: HistoryMode = HistoryMode.LAST_QUESTION):
        self.history_mode = history_mode

    DEFAULT_REQUEST = "I don't know what the data is about. Show me an interesting plot."

    def visualize(
        self,
        request: str | None,
        data: ExecutionResult,
        *,
        questions: list[str] | None = None,
        stream: bool = False,
    ) -> VisualisationResult:
        """Produce a visualization for the given data and optional user request.

        If *request* is ``None``, :attr:`DEFAULT_REQUEST` is used.  The request
        is then enriched with conversation history according to :attr:`history_mode`
        before being forwarded to :meth:`_visualize`.

        Args:
            request: Natural-language visualization request, or None for default.
            data: The execution result containing the dataframe and metadata.
            questions: Pre-extracted user question strings for history enrichment.
                When provided, these are used directly instead of extracting from
                ``data.meta``. Useful when the caller already has the conversation
                history (e.g. from the cache).
            stream: Whether to stream LLM output.
        """
        if request is None or request.strip() == "":
            request = self.DEFAULT_REQUEST
        enriched = self._enrich_with_history_context(request, data, questions=questions)
        return self._visualize(enriched, data, stream=stream)

    @abstractmethod
    def _visualize(
        self,
        request: str,
        data: ExecutionResult,
        *,
        stream: bool = False,
    ) -> VisualisationResult:
        """Produce a visualization for the given data and optional user request.

        Args:
            request: Visualization request — either :attr:`DEFAULT_REQUEST` or
                the caller's request enriched with conversation history.
            data: The execution result containing the dataframe and metadata.
            stream: Whether to stream LLM output.
        """
        pass

    @abstractmethod
    def edit(self, request: str, visualization: VisualisationResult, *, stream: bool = False) -> VisualisationResult:
        """Refine a prior visualization with a natural language request."""
        pass

    def _enrich_with_history_context(
        self,
        request: str,
        data: ExecutionResult,
        *,
        questions: list[str] | None = None,
    ) -> str:
        """Prepend conversation history to the visualization request.

        Returns the original *request* unchanged for ``NONE``, or a new string
        with a ``User question history:`` / ``Instructions:`` structure for other modes.

        Args:
            request: The visualization request to enrich.
            data: The execution result (used as fallback source for questions).
            questions: Pre-extracted user question strings. When ``None``,
                questions are extracted from ``data.meta`` via
                :meth:`_collect_human_questions`.
        """
        if self.history_mode == HistoryMode.NONE:
            return request

        if questions is None:
            questions = self._collect_human_questions(data)
        if not questions:
            return request

        match self.history_mode:
            case HistoryMode.LAST_QUESTION:
                history_block = questions[-1]

            case HistoryMode.LAST_QUESTION_ANSWER:
                history_block = f"{questions[-1]}\n{data.text}" if data.text else questions[-1]

            case HistoryMode.ALL_QUESTIONS:
                history_block = "\n".join(questions)

        return "\n".join(["User question history:", history_block, "Instructions:", request])

    @staticmethod
    def _collect_human_questions(data: ExecutionResult) -> list[str]:
        """Return the text content of every ``HumanMessage`` in the executor history."""
        from langchain_core.messages import HumanMessage

        raw_messages: list[Any] = data.meta.get(ExecutionResult.META_MESSAGES_KEY, [])
        return [str(m.content) for m in raw_messages if isinstance(m, HumanMessage)]
