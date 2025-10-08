from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel, ConfigDict

from portus.agent import ExecutionResult


class VisualisationResult(BaseModel):
    text: str
    meta: dict[str, Any]
    plot: Any | None
    code: str | None

    # Immutable model; allow arbitrary plot types (e.g., matplotlib objects)
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class Visualizer(ABC):
    @abstractmethod
    def visualize(self, request: str, data: ExecutionResult) -> VisualisationResult:
        pass
