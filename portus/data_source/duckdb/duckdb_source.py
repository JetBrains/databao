from abc import ABC, abstractmethod
from typing import Any

import sqlalchemy as sa
from duckdb import DuckDBPyConnection


class DuckDBSource(ABC):
    @abstractmethod
    def register(self, connection: DuckDBPyConnection | sa.Connection) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_context(self) -> dict[str, Any]:
        raise NotImplementedError
