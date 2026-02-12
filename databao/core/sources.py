from pathlib import Path
from typing import Any

from databao_context_engine import ConfiguredDatasource
from pandas import DataFrame

from databao.core.data_source import DBDataSource, DFDataSource, Sources
from databao.databases import DBConnectionConfig


class SourcesManager:
    def __init__(self, prepared_data_sources: list[ConfiguredDatasource] | None = None):
        self._sources: Sources = Sources(dfs={}, dbs={}, additional_context=[])
        self._add_prepared_ds(prepared_data_sources)

    def _add_prepared_ds(self, prepared_data_sources: list[ConfiguredDatasource] | None) -> None:
        if prepared_data_sources is None:
            return
        for ds in prepared_data_sources:
            content = ds.config
            if content:
                self.add_db(DBConnectionConfig(ds.datasource.type, content))
            else:
                raise ValueError("Only configurable datasources are supported")

    def add_db(
        self, config: DBConnectionConfig, *, name: str | None = None, context: str | Path | None = None
    ) -> DBDataSource:
        name = name or f"db{len(self._sources.dbs) + 1}"
        context_text = self._parse_context_arg(context) or ""

        source = DBDataSource(name=name, context=context_text, db_connection=config)
        self._sources.dbs[name] = source
        return source

    def add_df(self, df: DataFrame, *, name: str | None = None, context: str | Path | None = None) -> DFDataSource:
        name = name or f"df{len(self._sources.dfs) + 1}"

        context_text = self._parse_context_arg(context) or ""

        source = DFDataSource(name=name, context=context_text, df=df)
        self._sources.dfs[name] = source
        return source

    def add_context(self, context: str | Path) -> None:
        text = self._parse_context_arg(context)
        if text is None:
            raise ValueError("Invalid context provided.")
        self._sources.additional_context.append(text)

    @property
    def sources(self) -> Sources:
        return self._sources

    @staticmethod
    def _parse_context_arg(context: str | Path | None) -> str | None:
        if context is None:
            return None
        if isinstance(context, Path):
            return context.read_text()
        return context
