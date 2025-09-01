from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TypedDict, Any

from pandas import DataFrame
from langchain_core.language_models.chat_models import BaseChatModel


class MetaBase(TypedDict):
    query: str


class Meta(MetaBase, total=False):
    __extra__: dict[str, Any]  # marker for type checkers


@dataclass(frozen=True)
class DataResult:
    text: str
    df: DataFrame
    meta: Meta


class DataExecutor(ABC):
    @abstractmethod
    def execute(
            self,
            query: str,
            llm: BaseChatModel,
            dbs: dict[str, Any],
            dfs: dict[str, DataFrame],
            *,
            rows_limit: int = 100
    ) -> DataResult:
        pass
