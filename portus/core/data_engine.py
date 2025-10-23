import asyncio
from collections.abc import Sequence
from pathlib import Path
from typing import Self

import pandas as pd

from portus.core.data_source import DataSource
from portus.data.configs.data_source_config import DataSourceConfig
from portus.data.configs.schema_inspection_config import SchemaInspectionConfig
from portus.data.data_source_utils import get_data_source
from portus.data.database_schema_types import DatabaseSchema
from portus.data.schema_inspection import get_db_schema
from portus.data.schema_summary import summarize_schemas


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

    async def execute(self, query: str, *, source: str | None = None) -> pd.DataFrame | Exception:
        # For now, we use a single source only, so make selecting the source optional.
        if source is None and self._default_source_name is None:
            raise ValueError("No data sources have been added yet!")
        name = source if source is not None else self._default_source_name
        assert name is not None
        src = self._sources[name]
        result = await src.execute(query)
        return result

    async def get_source_schema(
        self,
        source: str,
        inspection_config: SchemaInspectionConfig,
    ) -> DatabaseSchema:
        ds = self._sources[source]
        if source in self._source_schemas:
            return self._source_schemas[source]
        schema = await get_db_schema(ds, inspection_config)
        self._source_schemas[source] = schema
        return schema

    async def get_source_schemas(
        self,
        inspection_config: SchemaInspectionConfig,
    ) -> dict[str, DatabaseSchema]:
        schemas = {}
        for source_name in self._sources:
            schemas[source_name] = await self.get_source_schema(source=source_name, inspection_config=inspection_config)
        return schemas

    async def get_source_schemas_summarization(
        self,
        inspection_config: SchemaInspectionConfig,
    ) -> str:
        db_schemas = await self.get_source_schemas(inspection_config)
        return summarize_schemas(db_schemas, inspection_config.summary_type)

    async def close(self) -> None:
        await asyncio.gather(*(source.close() for source in self._sources.values()))

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
