import abc

import pandas as pd

from databao.data.configs.data_source_config import DataSourceConfig
from databao.data.configs.schema_inspection_config import InspectionOptions
from databao.data.database_schema_types import DatabaseSchema


class DataSource[T: DataSourceConfig](abc.ABC):
    def __init__(self, config: T):
        self._config = config

    @property
    @abc.abstractmethod
    def config(self) -> T:
        pass

    @property
    def name(self) -> str:
        return self.config.name

    @abc.abstractmethod
    async def close(self) -> None:
        pass

    @abc.abstractmethod
    def close_sync(self) -> None:
        pass

    @abc.abstractmethod
    async def execute(self, query: str) -> pd.DataFrame | Exception:
        pass

    @abc.abstractmethod
    def execute_sync(self, query: str) -> pd.DataFrame | Exception:
        pass

    @abc.abstractmethod
    async def inspect_schema(self, options: InspectionOptions) -> DatabaseSchema:
        """Inspect the schema of the data source."""
        pass

    @abc.abstractmethod
    def inspect_schema_sync(self, options: InspectionOptions) -> DatabaseSchema:
        pass
