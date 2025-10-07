from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from langchain_core.messages import BaseMessage
from pandas import DataFrame


@dataclass(kw_only=True, frozen=True)
class ExecutionResult:
    text: str
    meta: dict[str, Any] | None = None
    sql: str | None = None
    visualization_prompt: str | None = None
    df: DataFrame | None = None
    messages: list[BaseMessage] | None = None
    """Full history of messages"""


class BaseAgent(ABC):
    """Agent contains everything needed to process user's query."""

    @abstractmethod
    def execute(self, messages: list[BaseMessage]) -> ExecutionResult:
        raise NotImplementedError
