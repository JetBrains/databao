import abc
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar, overload

import pandas as pd

from portus.data_source.config_classes.data_source_config import DataSourceConfig
from portus.data_source.config_classes.schema_inspection_config import InspectionOptions
from portus.data_source.database_schema_types import DatabaseSchema
from portus.utils import import_plugin, read_config_file

if TYPE_CHECKING:
    from portus.data_source.config_classes.sql_alchemy_data_source_config import SqlAlchemyDataSourceConfig
    from portus.data_source.sqlalchemy_source import SqlAlchemyDataSource

DataSourceConfigT = TypeVar("DataSourceConfigT", bound=DataSourceConfig)

type SemanticDict = dict[str, Any] | Literal["full"]  # TODO rename and make a pydantic model


class DataSource(Generic[DataSourceConfigT], abc.ABC):
    def __init__(self, config: DataSourceConfigT):
        self._config = config

    @property
    @abc.abstractmethod
    def config(self) -> DataSourceConfigT:
        pass

    @property
    def name(self) -> str:
        return self.config.name

    @abc.abstractmethod
    def close(self) -> None:
        pass

    @abc.abstractmethod
    async def aclose(self) -> None:
        pass

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
        pass

    @abc.abstractmethod
    async def ainspect_schema(
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


def read_data_source_config(path: Path) -> DataSourceConfig:
    d = read_config_file(path, parse_env_vars=True)
    if d.get("source_type") == "sqlalchemy":
        from portus.data_source.config_classes.sql_alchemy_data_source_config import SqlAlchemyDataSourceConfig

        return SqlAlchemyDataSourceConfig.model_validate(d)
    elif (source_class_config_import_path := d.get("source_class_config_import_path")) is not None:
        config_class_ = import_plugin(source_class_config_import_path, DataSourceConfig)

        return config_class_.model_validate(d)
    else:
        raise ValueError(f"Unsupported data source config type {d['source_type']}.")


@overload
async def get_data_source(config: "SqlAlchemyDataSourceConfig") -> "SqlAlchemyDataSource": ...
@overload
async def get_data_source(
    config: DataSourceConfig,
) -> DataSource | Sequence[DataSource]: ...
@overload
async def get_data_source(config: Path) -> DataSource | Sequence[DataSource]: ...
async def get_data_source(
    config: DataSourceConfig | Path,
) -> DataSource | Sequence[DataSource]:
    """Create a data source or multiple data sources based on the config.

    Some configs are data source providers (e.g., metabase), while others represent single connections.
    """
    if isinstance(config, Path):
        config = read_data_source_config(config)
    if config.source_class_import_path is not None:
        # ignore source_type if source_class_import_path is provided
        class_ = import_plugin(config.source_class_import_path, DataSource)
        return class_(config)
    match config.source_type:
        case "sqlalchemy":
            from portus.data_source.config_classes.sql_alchemy_data_source_config import SqlAlchemyDataSourceConfig
            from portus.data_source.sqlalchemy_source import SqlAlchemyDataSource

            assert isinstance(config, SqlAlchemyDataSourceConfig)
            return SqlAlchemyDataSource.from_config(config)
        case _:
            raise ValueError(f"Unsupported data source config {config}.")
