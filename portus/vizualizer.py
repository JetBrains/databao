from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from portus.agent.base_agent import ExecutionResult


@dataclass(frozen=True)
class VisualisationResult:
    text: str
    meta: dict[str, Any]
    plot: Any | None
    code: str | None


class Visualizer(ABC):
    @abstractmethod
    def visualize(self, request: str, data: ExecutionResult) -> VisualisationResult:
        pass


class DumbVisualizer(Visualizer):
    def visualize(self, request: str, data: ExecutionResult) -> VisualisationResult:
        plot = data.df.plot(kind="bar") if data.df is not None else None
        return VisualisationResult("", {}, plot, "")
