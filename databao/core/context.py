from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from databao_context_engine import ContextSearchResult
from pandas import DataFrame

from databao.core.data_source import Sources
from databao.core.sources import SourcesManager
from databao.databases import DBConnection, DBConnectionConfig, DBConnectionRuntime, convert_to_config
from databao.integrations.dce import DatabaoContextApi, DatabaoContextEngineApi


@dataclass(frozen=True)
class Context:
    _dce: DatabaoContextEngineApi | None
    _sources: Sources

    def search_context(self, retrieve_text: str) -> list[ContextSearchResult]:
        if self._dce is not None:
            return self._dce.search_context(retrieve_text)
        return []

    @property
    def sources(self) -> Sources:
        return self._sources

    @property
    def is_static(self) -> bool:
        return self._dce is not None

    @staticmethod
    def builder(project_dir: Path | None = None) -> ContextBuilder:
        return ContextBuilder(project_dir)

    @staticmethod
    def load(project_dir: Path) -> Context:
        dce_project = DatabaoContextApi.get_dce_project(project_dir)
        dce = DatabaoContextApi.get_dce(project_dir)
        prepared_data_sources = dce_project.get_prepared_datasource_list()
        sources_manager = SourcesManager(prepared_data_sources)
        return Context(_dce=dce, _sources=sources_manager.sources)


class ContextBuilder:
    def __init__(self, project_dir: Path | None):
        self._sources_manager = SourcesManager()
        self._dce_project = DatabaoContextApi.init_dce_project(project_dir) if project_dir is not None else None

    def add_db(
        self, conn: DBConnection, *, name: str | None = None, context: str | Path | None = None
    ) -> ContextBuilder:
        """
        Add a database connection to the context and optionally associate it
        with a specific context for query execution. Supports ``DBConnectionConfig``
        as well as SQLAlchemy engines and direct DuckDB connections.

        Args:
            conn (DBConnection): The database connection to be added.
                Can be a ``DBConnectionConfig`` or a ``DBConnectionRuntime``
                (an SQLAlchemy engine or connection, or a native DuckDB connection).
            name (str | None): Optional name to assign to the database connection. If
                not provided, a default name such as 'db1', 'db2', etc., will be
                generated dynamically based on the collection size.
            context (str | Path | None): Optional context for the database connection. It can
                be either the path to a file whose content will be used as the context or
                the direct context as a string.
        """
        if isinstance(conn, DBConnectionConfig):
            config = conn
        elif isinstance(conn, DBConnectionRuntime):
            config = self._convert_to_config(conn)
        else:
            raise ValueError("Connection must be a DBConnection")

        db_source = self._sources_manager.add_db(config, name=name, context=context)

        if self._dce_project is not None:
            self._dce_project.create_datasource_config(config.type, db_source.name, config.content)
        return self

    def add_df(self, df: DataFrame, *, name: str | None = None, context: str | Path | None = None) -> ContextBuilder:
        """Register a DataFrame in the context.

        Args:
            df: DataFrame to expose to executors/SQL.
            name: Optional name; defaults to df1/df2/...
            context: Optional text or path to a file describing this dataset for the LLM.
        """
        self._sources_manager.add_df(df, name=name, context=context)
        # V0: don't pass it to DCE - only use it to initialize our DuckDB connection later

        return self

    def add_context(self, context: str | Path) -> ContextBuilder:
        """Add additional context to help models understand your data.

        Use this method to add general information that might not be associated with a specific data source.
        If the information is specific to a data source, use the ``context`` argument of ``add_db`` and ``add_df``.

        Args:
            context: The string or the path to a file containing the additional context.
        """
        self._sources_manager.add_context(context)

        return self

    def build(self) -> Context:
        if self._dce_project is not None:
            self._dce_project.build_context()
            dce = DatabaoContextApi.get_dce(self._dce_project.project_dir)
        else:
            dce = None
        sources = self._sources_manager.sources
        return Context(_dce=dce, _sources=sources)

    @staticmethod
    def _convert_to_config(run_conn: DBConnectionRuntime) -> DBConnectionConfig:
        return convert_to_config(run_conn)
