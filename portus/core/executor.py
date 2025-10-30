from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame
from pydantic import BaseModel, ConfigDict

from portus.core.opa import Opa
from portus.core.session import Session


class ExecutionResult(BaseModel):
    text: str
    meta: dict[str, Any]
    code: str | None = None
    df: DataFrame | None = None

    # Pydantic v2 configuration: make the model immutable and allow pandas DataFrame
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class Executor(ABC):
    @abstractmethod
    def execute(
        self,
        session: Session,
        opa: Opa,
        *,
        rows_limit: int = 100,
        cache_scope: str = "common_cache",
        stream: bool = True,
    ) -> ExecutionResult:
        pass
