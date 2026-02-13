from pathlib import Path
from typing import Any

from databao_context_engine import ConfiguredDatasource
from pandas import DataFrame

from databao.core.data_source import DBDataSource, DFDataSource, Sources
from databao.databases import DBConnectionConfig
from databao.databases.databases import to_agent_config_content


class SourcesManager:
    def __init__(self, configured_data_sources: list[ConfiguredDatasource] | None = None):
        self._sources: Sources = Sources(dfs={}, dbs={}, additional_context=[])
        self._add_configured_ds(configured_data_sources)

    def _add_configured_ds(self, configured_data_sources: list[ConfiguredDatasource] | None) -> None:
        if configured_data_sources is None:
            return
        for configured_ds in configured_data_sources:
            if configured_ds.config is None:
                raise ValueError("Only configurable datasources are supported")
            type = configured_ds.datasource.type
            name = self._get_ds_name(configured_ds)
            content = self._get_config_content(configured_ds)
            self.add_db(DBConnectionConfig(type, content), name=name)

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

    # TODO (dce): should be provided by the DCE side
    @staticmethod
    def _get_ds_name(dce_ds: ConfiguredDatasource) -> str:
        id = dce_ds.datasource.id
        return str(id.datasource_path).split("/")[-1]

    @staticmethod
    def _get_config_content(dce_ds: ConfiguredDatasource) -> dict[str, Any]:
        return to_agent_config_content(dce_ds)

    @staticmethod
    def _parse_context_arg(context: str | Path | None) -> str | None:
        if context is None:
            return None
        if isinstance(context, Path):
            return context.read_text()
        return context
