from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame
from pydantic import BaseModel, ConfigDict

from databao.core.opa import Opa
from databao.core.session import Session


class ExecutionResult(BaseModel):
    """Immutable result of a single agent/executor step.

    Attributes:
        text: Human-readable response to the last user query.
        meta: Arbitrary metadata collected during execution (debug info, timings, etc.).
        code: Text of generated code when applicable.
        df: Optional dataframe materialized by the executor.
    """

    text: str
    meta: dict[str, Any]
    code: str | None = None
    df: DataFrame | None = None

    # Pydantic v2 configuration: make the model immutable and allow pandas DataFrame
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class Executor(ABC):
    """
    Defines the Executor interface as an abstract base class for execution of
    operations within a given session.

    Methods:
        execute: Abstract method to execute a single OPA within a session.
    """

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
        """Execute a single OPA within a session.

        Args:
            session: Active session providing LLM, data connections, cache, etc.
            opa: User intent/query to process.
            rows_limit: Preferred row limit for data materialization (may be ignored by executors).
            cache_scope: Logical scope for caching per chat/thread.
        """
        pass
