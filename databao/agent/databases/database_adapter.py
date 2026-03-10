from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from _duckdb import DuckDBPyConnection
from databao_context_engine import DatasourceType
from databao_context_engine.pluginlib.build_plugin import AbstractConfigFile

from databao.agent.databases.database_connection import (
    DBConnection,
    DBConnectionConfig,
    DBConnectionRuntime,
)

if TYPE_CHECKING:
    from sqlalchemy import Engine


class DatabaseAdapter(ABC):
    @classmethod
    @abstractmethod
    def type(cls) -> DatasourceType: ...

    @classmethod
    @abstractmethod
    def accept(cls, conn: DBConnection) -> bool: ...

    @classmethod
    @abstractmethod
    def create_config_file(cls, config: DBConnectionConfig, name: str) -> AbstractConfigFile: ...

    @classmethod
    @abstractmethod
    def create_config_from_runtime(cls, run_conn: DBConnectionRuntime) -> DBConnectionConfig: ...

    @classmethod
    @abstractmethod
    def create_config_from_content(cls, content: dict[str, Any]) -> DBConnectionConfig: ...

    @classmethod
    @abstractmethod
    def register_in_duckdb(cls, shared_conn: DuckDBPyConnection, config: DBConnectionConfig, name: str) -> None: ...

    @classmethod
    def create_sqlalchemy_engine(cls, config: DBConnectionConfig) -> "Engine | None":
        """Create a SQLAlchemy engine from a connection config, or return None if not supported."""
        return None
