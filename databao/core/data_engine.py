import asyncio
from collections.abc import Sequence
from pathlib import Path
from typing import Self

import pandas as pd

from databao.core.data_source import DataSource
from databao.data.configs.data_source_config import DataSourceConfig
from databao.data.configs.schema_inspection_config import SchemaInspectionConfig
from databao.data.data_source_utils import get_data_source
from databao.data.database_schema_types import DatabaseSchema
from databao.data.schema_summary import summarize_schemas


class DataEngine:
    """Main data access point that coordinates between all provided data sources."""

    def __init__(self, sources: Sequence[DataSource[DataSourceConfig]] | None = None):
        sources = sources or []
        sources_dict = {s.name: s for s in sources}
        assert len(sources_dict) == len(sources), "Duplicate data source names are not allowed."

        self._sources = sources_dict
        self._default_source_name = sources[0].name if len(sources) > 0 else None

        self._source_schemas: dict[str, DatabaseSchema] = {}

    @property
    def sources(self) -> dict[str, DataSource[DataSourceConfig]]:
        return self._sources

    def add_source(self, source: DataSource[DataSourceConfig]) -> None:
        if source.name in self._sources:
            raise KeyError(f"Data source with name {source.name} already exists.")
        self._sources[source.name] = source
        if self._default_source_name is None:
            self._default_source_name = source.name

    def _get_source(self, name: str | None) -> DataSource[DataSourceConfig]:
        if name is None and self._default_source_name is None:
            raise ValueError("No data sources have been added yet!")
        name = name if name is not None else self._default_source_name
        assert name is not None
        return self._sources[name]

    async def execute(self, query: str, *, source: str | None = None) -> pd.DataFrame | Exception:
        src = self._get_source(source)
        return await src.execute(query)

    def execute_sync(self, query: str, *, source: str | None = None) -> pd.DataFrame | Exception:
        src = self._get_source(source)
        return src.execute_sync(query)

    async def get_source_schema(
        self,
        source: str,
        inspection_config: SchemaInspectionConfig,
    ) -> DatabaseSchema:
        ds = self._get_source(source)
        if source in self._source_schemas:
            return self._source_schemas[source]
        schema = await ds.inspect_schema(inspection_config.inspection_options)
        self._source_schemas[source] = schema
        return schema

    def get_source_schema_sync(
        self,
        source: str,
        inspection_config: SchemaInspectionConfig,
    ) -> DatabaseSchema:
        ds = self._get_source(source)
        if source in self._source_schemas:
            return self._source_schemas[source]
        schema = ds.inspect_schema_sync(inspection_config.inspection_options)
        self._source_schemas[source] = schema
        return schema

    async def get_source_schemas_summarization(
        self,
        inspection_config: SchemaInspectionConfig,
    ) -> str:
        schemas = await asyncio.gather(
            *(
                self.get_source_schema(source=source_name, inspection_config=inspection_config)
                for source_name in self._sources
            )
        )
        return summarize_schemas(schemas, inspection_config.summary_type)

    def get_source_schemas_summarization_sync(
        self,
        inspection_config: SchemaInspectionConfig,
    ) -> str:
        schemas = [
            self.get_source_schema_sync(source=source_name, inspection_config=inspection_config)
            for source_name in self._sources
        ]
        return summarize_schemas(schemas, inspection_config.summary_type)

    async def close(self) -> None:
        await asyncio.gather(*(source.close() for source in self._sources.values()))

    def close_sync(self) -> None:
        for source in self._sources.values():
            source.close_sync()

    @classmethod
    async def from_configs(cls, source_configs: Sequence[DataSourceConfig | Path]) -> Self:
        sources: list[DataSource[DataSourceConfig]] = []
        for config in source_configs:
            source = await get_data_source(config)
            if isinstance(source, DataSource):
                sources.append(source)
            else:
                sources.extend(source)
        return cls(sources)
