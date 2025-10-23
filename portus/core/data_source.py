import abc
from hashlib import sha256
from typing import Any, Literal

import pandas as pd

from portus.data.configs.data_source_config import DataSourceConfig
from portus.data.configs.schema_inspection_config import InspectionOptions
from portus.data.database_schema_types import DatabaseSchema

type SemanticDict = dict[str, Any] | Literal["full"]  # TODO rename and make a pydantic model


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

    @property
    def hash(self) -> str:
        return sha256(self.name.encode()).hexdigest()[:16]

    @abc.abstractmethod
    async def close(self) -> None:
        pass

    @abc.abstractmethod
    async def execute(self, query: str) -> pd.DataFrame | Exception:
        pass

    @abc.abstractmethod
    async def inspect_schema(
        self,
        semantic_dict: SemanticDict,
        options: InspectionOptions,
    ) -> DatabaseSchema:
        """Inspect the schema of the data source.

        The following representation of the semantic_dict is expected::

            {
              "tables": {
                <table_name>: {
                  "description": str,
                  "columns": {
                    <column_name>: <description>
                  }
                },
                <table_name>: "all", # to select all columns automatically
              }
            }

        All tables and columns not listed in semantic_dict will be omitted.
        """
        # TODO "semantic_dict" pydantic model!
        # TODO semantic dict should also have schema information for each table
        # TODO <table_name> should be fully qualified. For now, it's not.
        pass
