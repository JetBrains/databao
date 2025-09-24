from dataclasses import dataclass
from portus.executor import ExecutionResult
from abc import abstractmethod, ABC
from typing import Any, TypedDict
from langchain_core.language_models.chat_models import BaseChatModel


class MetaBase(TypedDict):
    plot_code: str


class Meta(MetaBase, total=False):
    __extra__: dict[str, Any]  # marker for type checkers


@dataclass(frozen=True)
class VisualisationResult:
    plot: Any
    meta: Meta


class Visualizer(ABC):
    @abstractmethod
    def visualize(self, request: str, llm: BaseChatModel, data: ExecutionResult) -> VisualisationResult:
        pass


class DumbVisualizer(Visualizer):
    def visualize(self, request: str, llm: BaseChatModel, data: ExecutionResult) -> VisualisationResult:
        return VisualisationResult(data.df.plot(kind="bar"), {"plot_code": ""})
