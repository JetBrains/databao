from abc import ABC, abstractmethod
from typing import Any

from duckdb import DuckDBPyConnection


class DataSource(ABC):
    @abstractmethod
    def register(self, connection: DuckDBPyConnection) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_schema(self) -> dict[str, str]:
        raise NotImplementedError

    @abstractmethod
    def get_context(self) -> dict[str, Any]:
        raise NotImplementedError
