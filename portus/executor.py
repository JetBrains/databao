from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from langchain_core.language_models.chat_models import BaseChatModel
from pandas import DataFrame

from portus.opa import Opa
from portus.session import Session


@dataclass(frozen=True)
class ExecutionResult:
    text: str
    meta: dict[str, Any]
    code: str | None = None
    df: DataFrame | None = None


class Executor(ABC):
    @abstractmethod
    def execute(
        self, session: Session, opas: list[Opa], llm: BaseChatModel, *, rows_limit: int = 100
    ) -> ExecutionResult:
        pass
