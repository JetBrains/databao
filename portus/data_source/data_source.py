import abc
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, overload

import pandas as pd

from portus._config_utils import import_plugin, read_config_file
from portus.data_source.configs.data_source_config import DataSourceConfig
from portus.data_source.configs.schema_inspection_config import InspectionOptions
from portus.data_source.database_schema_types import DatabaseSchema

if TYPE_CHECKING:
    from portus.data_source.configs.sqlalchemy_data_source_config import SqlAlchemyDataSourceConfig
    from portus.data_source.sqlalchemy_source import SqlAlchemyDataSource

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


def read_data_source_config(path: Path) -> DataSourceConfig:
    d = read_config_file(path, parse_env_vars=True)
    if d.get("source_type") == "sqlalchemy":
        from portus.data_source.configs.sqlalchemy_data_source_config import SqlAlchemyDataSourceConfig

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
) -> DataSource[DataSourceConfig] | Sequence[DataSource[DataSourceConfig]]: ...
@overload
async def get_data_source(config: Path) -> DataSource[DataSourceConfig] | Sequence[DataSource[DataSourceConfig]]: ...
async def get_data_source(
    config: DataSourceConfig | Path,
) -> DataSource[DataSourceConfig] | Sequence[DataSource[DataSourceConfig]]:
    """Create a data source or multiple data sources based on the config.

    Some configs are data source providers (e.g., metabase), while others represent single connections.
    """
    if isinstance(config, Path):
        config = read_data_source_config(config)
    if config.source_class_import_path is not None:
        # ignore source_type if source_class_import_path is provided
        class_ = import_plugin(config.source_class_import_path, DataSource[Any])  # type: ignore[type-abstract]
        return class_(config)
    match config.source_type:
        case "sqlalchemy":
            from portus.data_source.configs.sqlalchemy_data_source_config import SqlAlchemyDataSourceConfig
            from portus.data_source.sqlalchemy_source import SqlAlchemyDataSource

            assert isinstance(config, SqlAlchemyDataSourceConfig)
            return SqlAlchemyDataSource(config)
        case _:
            raise ValueError(f"Unsupported data source config {config}.")
