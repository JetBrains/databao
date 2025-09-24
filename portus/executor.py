from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TypedDict, Any, Optional

from pandas import DataFrame
from langchain_core.language_models.chat_models import BaseChatModel
from portus.opa import Opa


class MetaBase(TypedDict):
    code: str


class Meta(MetaBase, total=False):
    __extra__: dict[str, Any]  # marker for type checkers


@dataclass(frozen=True)
class ExecutionResult:
    text: str
    meta: Meta
    df: Optional[DataFrame] = None


class Executor(ABC):
    @abstractmethod
    def execute(
            self,
            opas: list[Opa],
            llm: BaseChatModel,
            dbs: dict[str, Any],
            dfs: dict[str, DataFrame],
            *,
            rows_limit: int = 100
    ) -> ExecutionResult:
        pass
