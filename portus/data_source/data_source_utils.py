from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, overload

from portus._config_utils import import_plugin, read_config_file
from portus.core.data_source import DataSource
from portus.data_source.configs.data_source_config import DataSourceConfig

if TYPE_CHECKING:
    from portus.data_source.configs.sqlalchemy_data_source_config import SqlAlchemyDataSourceConfig
    from portus.data_source.sqlalchemy_source import SqlAlchemyDataSource


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
