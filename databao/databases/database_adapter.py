from abc import ABC, abstractmethod

from _duckdb import DuckDBPyConnection
from databao_context_engine import DatasourceType

from databao.databases.database_connection import DBConnection, DBConnectionConfig, DBConnectionRuntime


# TODO (dce): extract the common part of the implementations
class DatabaseAdapter(ABC):
    @classmethod
    @abstractmethod
    def type(cls) -> DatasourceType: ...

    @classmethod
    @abstractmethod
    def main_property_keys(cls) -> set[str]: ...

    @classmethod
    @abstractmethod
    def accept(cls, conn: DBConnection) -> bool: ...

    @classmethod
    @abstractmethod
    def convert_to_config(cls, run_conn: DBConnectionRuntime) -> DBConnectionConfig | None: ...

    @classmethod
    @abstractmethod
    def register_in_duckdb(cls, shared_conn: DuckDBPyConnection, config: DBConnectionConfig, name: str) -> None: ...
