from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional

from langchain_core.language_models.chat_models import BaseChatModel
from pandas import DataFrame

from portus.opa import Opa
from portus.session import Session


@dataclass(frozen=True)
class ExecutionResult:
    text: str
    meta: dict[str, Any]
    code: Optional[str] = None
    df: Optional[DataFrame] = None


class Executor(ABC):
    @abstractmethod
    def execute(
            self,
            session: Session,
            opas: list[Opa],
            llm: BaseChatModel,
            *,
            rows_limit: int = 100
    ) -> ExecutionResult:
        pass
