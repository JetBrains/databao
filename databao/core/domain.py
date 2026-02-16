from __future__ import annotations

from abc import ABC
from pathlib import Path
from typing import Any, Protocol

from databao_context_engine import ConfiguredDatasource, ContextSearchResult, DatasourceId
from pandas import DataFrame

from databao.core.data_source import DataSource, DBDataSource, DFDataSource, Sources
from databao.core.sources import SourcesManager
from databao.databases import DBConnection, DBConnectionConfig, DBConnectionRuntime
from databao.databases.databases import convert_to_config, to_agent_config_content, to_dce_config_content
from databao.integrations.dce import DatabaoContextApi

DomainSource = DBConnection | DataFrame


class Domain(Protocol):
    def add_source(
        self, source: DomainSource, *, name: str | None = None, description: str | Path | None = None
    ) -> None: ...

    def add_description(self, description: str | Path | None = None) -> None: ...

    def build_context(self) -> None: ...


class _Domain(ABC, Domain):
    def __init__(self) -> None:
        self._sources_manager = SourcesManager()

    def add_source(
        self, source: DomainSource, *, name: str | None = None, description: str | Path | None = None
    ) -> None:
        self._add_source(source, name=name, context=description)

    def add_description(self, description: str | Path | None = None) -> None:
        self._sources_manager.add_context(description)

    def finalize_sources(self) -> None:
        self._sources_manager.finalize()

    @property
    def are_sources_finalized(self) -> bool:
        return self._sources_manager.is_finalized

    @property
    def sources(self) -> Sources:
        return self._sources_manager.sources

    def _add_source(
        self, source: DomainSource, *, name: str | None = None, context: str | Path | None = None
    ) -> DataSource | None:
        if isinstance(source, DBConnection):
            return self._add_db(source, name=name, context=context)
        elif isinstance(source, DataFrame):
            return self._add_df(source, name=name, context=context)
        else:
            raise ValueError("Source must be a DBConnection or DataFrame")

    def _add_db(
        self, conn: DBConnection, name: str | None = None, context: str | Path | None = None
    ) -> DBDataSource | None:
        if isinstance(conn, DBConnectionConfig):
            config = conn
        elif isinstance(conn, DBConnectionRuntime):
            config = self._convert_to_config(conn)
        else:
            raise ValueError("Connection must be a DBConnection")
        return self._sources_manager.add_db(config, name=name, context=context)

    def _add_df(self, df: DataFrame, name: str | None = None, context: str | Path | None = None) -> DFDataSource | None:
        return self._sources_manager.add_df(df, name=name, context=context)

    @staticmethod
    def _convert_to_config(run_conn: DBConnectionRuntime) -> DBConnectionConfig:
        return convert_to_config(run_conn)


class _InMemoryDomain(_Domain):
    def build_context(self) -> None:
        raise ValueError("Context cannot be built in memory mode.")


class _PersistentDomain(_Domain):
    def __init__(self, project_dir: Path):
        super().__init__()
        self._dce_project = DatabaoContextApi.init_or_get_dce_project(project_dir)
        self._dce = DatabaoContextApi.get_dce(project_dir)
        self._configured: dict[str, DatasourceId] = {}
        self._set_configured_data_sources()

    def add_source(
        self, source: DomainSource, *, name: str | None = None, description: str | Path | None = None
    ) -> None:
        ds = self._add_source(source, name=name, context=description)
        if isinstance(ds, DBDataSource):
            type = ds.config.type
            name = ds.name
            dce_content = self._get_dce_config_content(ds.config)
            configured_ds = self._dce_project.create_datasource_config(type, name, dce_content)
            self._add_configured(configured_ds.datasource.id, ds)

    def build_context(self) -> None:
        if not self.are_sources_finalized:
            raise ValueError("Sources are not finalized. Please finalize sources before building context.")
        if not self._dce.is_context_built():
            self._dce_project.build_context()

    def search_context(self, retrieve_text: str, datasource_name: str | None = None) -> list[ContextSearchResult]:
        if not self._dce.is_context_built():
            raise ValueError("Context is not built. Please build the context before searching.")
        datasource_ids = self._create_datasource_id_list(datasource_name)
        return self._dce.search_context(retrieve_text, datasource_ids=datasource_ids)

    def _set_configured_data_sources(self) -> None:
        configured_data_sources = self._dce_project.get_configured_datasource_list()
        if len(configured_data_sources) == 0:
            return
        for configured_ds in configured_data_sources:
            if configured_ds.config is None:
                raise ValueError("Only configurable datasources are supported")
            id = configured_ds.datasource.id
            type = configured_ds.datasource.type
            name = self._get_datasource_name(configured_ds)
            content = self._get_config_content(configured_ds)
            ds = self._add_source(DBConnectionConfig(type, content), name=name)
            if ds is None:
                raise ValueError(f'Failed to add duplicated configured datasource: "{name}"')
            self._add_configured(id, ds)
        self.finalize_sources()

    def _add_configured(self, id: DatasourceId, ds: DataSource) -> None:
        self._configured[ds.name] = id

    def _create_datasource_id_list(self, ds_name: str | None) -> list[DatasourceId] | None:
        datasource_id = self._configured.get(ds_name) if ds_name else None
        return [datasource_id] if datasource_id else None

    @staticmethod
    def _get_datasource_name(configured_ds: ConfiguredDatasource) -> str:
        id = configured_ds.datasource.id
        return DatabaoContextApi.get_datasource_name(id)

    @staticmethod
    def _get_config_content(configured_ds: ConfiguredDatasource) -> dict[str, Any]:
        return to_agent_config_content(configured_ds)

    @staticmethod
    def _get_dce_config_content(config: DBConnectionConfig) -> dict[str, Any]:
        return to_dce_config_content(config)
