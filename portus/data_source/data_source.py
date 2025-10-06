import abc
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Self, overload

import pandas as pd
from pydantic import BaseModel, ConfigDict

from portus.data_source.database_schema_types import DatabaseSchema
from portus.data_source.schema_inspection_config import InspectionOptions
from portus.utils import read_config_file

if TYPE_CHECKING:
    from portus.data_source.sqlalchemy_source import SqlAlchemyConfig, SqlAlchemyDataSource


type SemanticDict = dict[str, Any] | Literal["full"]  # TODO rename and make a pydantic model


class DataSourceConfig(BaseModel):
    source_type: str
    name: str

    model_config = ConfigDict(extra="forbid")

    @classmethod
    def from_file(cls, path: Path) -> Self:
        d = read_config_file(path, parse_env_vars=True)
        return cls.model_validate(d)


# TODO async API: a-prefix methods or AsyncDataSource
class DataSource(abc.ABC):
    @property
    @abc.abstractmethod
    def config(self) -> DataSourceConfig:
        pass

    @property
    def name(self) -> str:
        return self.config.name

    @abc.abstractmethod
    def execute(self, query: str) -> pd.DataFrame | Exception:
        pass

    @abc.abstractmethod
    async def aexecute(self, query: str) -> pd.DataFrame | Exception:
        pass

    @abc.abstractmethod
    def inspect_schema(
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
        pass

    @abc.abstractmethod
    async def ainspect_schema(
        self,
        semantic_dict: SemanticDict,
        options: InspectionOptions,
    ) -> DatabaseSchema:
        pass

    @abc.abstractmethod
    def close(self) -> None:
        pass

    @abc.abstractmethod
    async def aclose(self) -> None:
        pass


def read_data_source_config(path: Path) -> DataSourceConfig:
    d = read_config_file(path, parse_env_vars=True)
    if d["source_type"] == "sqlalchemy":
        from portus.data_source.sqlalchemy_source import SqlAlchemyConfig

        return SqlAlchemyConfig.model_validate(d)
    else:
        raise ValueError(f"Unsupported data source config type {d['source_type']}.")


@overload
async def get_data_source(config: "SqlAlchemyConfig") -> "SqlAlchemyDataSource": ...
@overload
async def get_data_source(config: DataSourceConfig) -> DataSource | Sequence[DataSource]: ...
@overload
async def get_data_source(config: Path) -> DataSource | Sequence[DataSource]: ...
async def get_data_source(config: DataSourceConfig | Path) -> DataSource | Sequence[DataSource]:
    """Create a data source or multiple data sources based on the config.

    Some configs are data source providers (e.g., metabase), while others represent single connections.
    """
    if isinstance(config, Path):
        config = read_data_source_config(config)
    match config.source_type:
        case "sqlalchemy":
            from portus.data_source.sqlalchemy_source import SqlAlchemyConfig, SqlAlchemyDataSource

            assert isinstance(config, SqlAlchemyConfig)
            return SqlAlchemyDataSource.from_config(config)
        case _:
            raise ValueError(f"Unsupported data source config {config}.")
