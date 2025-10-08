from abc import ABC, abstractmethod
from typing import Any

from langchain_core.messages import BaseMessage
from pandas import DataFrame
from pydantic import BaseModel, ConfigDict


class ExecutionResult(BaseModel):
    text: str
    meta: dict[str, Any] | None = None
    sql: str | None = None
    visualization_prompt: str | None = None
    df: DataFrame | None = None
    messages: list[BaseMessage] | None = None
    """Full history of messages"""

    # Pydantic v2 configuration: make the model immutable and allow pandas DataFrame
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class BaseAgent(ABC):
    """Agent contains everything needed to process user's query."""

    @abstractmethod
    def execute(self, messages: list[BaseMessage]) -> ExecutionResult:
        raise NotImplementedError
