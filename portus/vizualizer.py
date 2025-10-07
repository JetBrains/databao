from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from langchain_core.language_models.chat_models import BaseChatModel

from portus.executor import ExecutionResult


@dataclass(frozen=True)
class VisualisationResult:
    text: str
    meta: dict[str, Any]
    plot: Any | None
    code: str | None


class Visualizer(ABC):
    @abstractmethod
    def visualize(self, request: str, llm: BaseChatModel, data: ExecutionResult) -> VisualisationResult:
        pass


class DumbVisualizer(Visualizer):
    def visualize(self, request: str, llm: BaseChatModel, data: ExecutionResult) -> VisualisationResult:
        plot = data.df.plot(kind="bar") if data.df is not None else None
        return VisualisationResult("", {}, plot, "")
