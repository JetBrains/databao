import time
from pathlib import Path
from typing import Self, Sequence

import pandas as pd

from portus.data_source.configs.data_source_config import DataSourceConfig
from portus.data_source.configs.schema_inspection_config import SchemaInspectionConfig
from portus.data_source.data_source_utils import get_data_source
from portus.core.data_source import DataSource
from portus.data_source.database_schema import get_db_schema, summarize_schemas
from portus.data_source.database_schema_types import DatabaseSchema
from portus.caches.disk_cache import DiskCache, DiskCacheConfig


class DataEngine:
    """Main data access point that coordinates between all provided data sources."""

    def __init__(self, sources: list[DataSource]):
        assert len(sources) > 0, "No data sources provided."
        sources_dict = {s.name: s for s in sources}
        assert len(sources_dict) == len(sources), "Duplicate data source names are not allowed."

        self.sources = sources_dict
        self.default_source_name = sources[0].name

        self._source_schemas: dict[str, DatabaseSchema] = {}

        self.cache: DiskCache | None = None

    def __getitem__(self, source: str) -> DataSource:
        return self.sources[source]

    async def enable_and_validate_query_cache(
            self, cache_config: DiskCacheConfig, cache_is_stale_query: str | None = None
    ) -> None:
        self.cache = DiskCache(cache_config)
        if cache_is_stale_query is not None:
            # For now, we assume a single data source, so we can just use the default source and cache validation query
            current_df = await self.execute(cache_is_stale_query, source=self.default_source_name, skip_cache=True)
            if isinstance(current_df, Exception):
                raise current_df
            self.cache.invalidate_source_if_stale_query(
                source_name=self.default_source_name,
                is_stale_query=cache_is_stale_query,
                is_stale_query_df=current_df,
            )

    async def execute(
            self, query: str, *, source: str | None = None, skip_cache: bool = False
    ) -> pd.DataFrame | Exception:
        # For now, we use a single source only, so make selecting the source optional.
        name = source if source is not None else self.default_source_name
        src = self.sources[name]

        if not skip_cache and self.cache is not None:
            cached = self.cache.get_sql(query, source_name=name)
            if cached is not None:
                return cached

        start = time.perf_counter()
        result = await src.execute(query)
        end = time.perf_counter()
        # N.B. The time can be overestimated depending on the async execution order, but it's a good enough estimate for us.
        exec_ms = (end - start) * 1000.0

        if not skip_cache and self.cache is not None:
            # Should we also cache errors?
            if isinstance(result, pd.DataFrame):
                try:
                    self.cache.set_sql(query, result, source_name=name)
                except Exception as e:
                    print(f"Error while caching query: {e}")

        return result

    async def get_source_schema(
            self,
            source: str,
            inspection_config: SchemaInspectionConfig,
    ) -> DatabaseSchema:
        ds = self.sources[source]
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
        for source_name in self.sources:
            schemas[source_name] = await self.get_source_schema(source=source_name, inspection_config=inspection_config)
        return schemas

    async def get_source_schemas_summarization(
            self,
            inspection_config: SchemaInspectionConfig,
    ) -> str:
        db_schemas = await self.get_source_schemas(inspection_config)
        return summarize_schemas(db_schemas, inspection_config.summary_type)

    async def close(self) -> None:
        if self.cache is not None:
            self.cache.close()
        for source in self.sources.values():
            await source.close()

    @classmethod
    async def from_configs(cls, source_configs: Sequence[DataSourceConfig | Path]) -> Self:
        sources: list[DataSource] = []
        for config in source_configs:
            source = await get_data_source(config)
            if isinstance(source, DataSource):
                sources.append(source)
            else:
                sources.extend(source)
        return cls(sources)
