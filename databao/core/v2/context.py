from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from databao_context_engine import ContextSearchResult
from pandas import DataFrame

from databao.core.data_source import Sources
from databao.core.v2.sources import SourcesManager
from databao.databases import DBConnection, DBConnectionConfig, DBConnectionRuntime, convert_to_config
from databao.integrations.dce import DatabaoContextApi, DatabaoContextEngineApi


@dataclass(frozen=True)
class Context:
    _dce: DatabaoContextEngineApi
    _sources: Sources

    def search_context(self, retrieve_text: str) -> list[ContextSearchResult]:
        return self._dce.search_context(retrieve_text)

    @property
    def sources(self) -> Sources:
        return self._sources

    @staticmethod
    def builder(project_dir: Path) -> ContextBuilder:
        return ContextBuilder(project_dir)

    @staticmethod
    def load(project_dir: Path) -> Context:
        dce_project = DatabaoContextApi.get_dce_project(project_dir)
        dce = DatabaoContextApi.get_dce(project_dir)
        prepared_data_sources = dce_project.get_prepared_datasource_list()
        sources_manager = SourcesManager(prepared_data_sources)
        return Context(_dce=dce, _sources=sources_manager.sources)


class ContextBuilder:
    def __init__(self, project_dir: Path):
        self._sources_manager = SourcesManager()
        self._dce_project = DatabaoContextApi.init_dce_project(project_dir)

    def add_db(self, conn: DBConnection, *, name: str | None = None, context: str | Path | None = None) -> None:
        if isinstance(conn, DBConnectionConfig):
            config = conn
        elif isinstance(conn, DBConnectionRuntime):
            config = self._convert_to_config(conn)
        else:
            raise ValueError("Connection must be a DBConnection")

        db_source = self._sources_manager.add_db(config, name=name, context=context)
        self._dce_project.create_datasource_config(config.type, db_source.name, config.content)

    def add_df(self, df: DataFrame, *, name: str | None = None, context: str | Path | None = None) -> None:
        self._sources_manager.add_df(df, name=name, context=context)
        # V0: don't pass it to DCE - only use it to initialize our DuckDB connection later

    def add_context(self, context: str | Path) -> None:
        self._sources_manager.add_context(context)

    def build(self) -> Context:
        self._dce_project.build_context()
        dce = DatabaoContextApi.get_dce(self._dce_project.project_dir)
        sources = self._sources_manager.sources
        return Context(_dce=dce, _sources=sources)

    @staticmethod
    def _convert_to_config(run_conn: DBConnectionRuntime) -> DBConnectionConfig:
        return convert_to_config(run_conn)
