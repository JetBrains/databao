from dataclasses import dataclass
from typing import Any

from _duckdb import DuckDBPyConnection
from databao_context_engine import DatasourceType
from sqlalchemy import Connection, Engine


@dataclass(frozen=True)
class DBConnectionConfig:
    type: DatasourceType
    content: dict[str, Any]


DBConnectionRuntime = DuckDBPyConnection | Engine | Connection


DBConnection = DBConnectionConfig | DBConnectionRuntime
