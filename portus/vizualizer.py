from dataclasses import dataclass
from portus.data_executor import DataResult
from abc import abstractmethod, ABC
from typing import Any
from langchain_core.language_models.chat_models import BaseChatModel


@dataclass(frozen=True)
class VisualisationResult:
    raw: str
    plot: Any
    meta: dict[str, Any]


class Visualizer(ABC):
    @abstractmethod
    def visualize(self, request: str, llm: BaseChatModel, data: DataResult) -> VisualisationResult:
        pass


class DumbVisualizer(Visualizer):
    def visualize(self, request: str, llm: BaseChatModel, data: DataResult) -> VisualisationResult:
        return VisualisationResult("", data.df.plot(kind="bar"), {})
