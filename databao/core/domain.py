from __future__ import annotations

from abc import ABC
from pathlib import Path
from typing import Any, Protocol

from databao_context_engine import (
    ConfiguredDatasource,
    ContextSearchResult,
    DatasourceId,
    DatasourceStatus,
    DatasourceType,
)
from pandas import DataFrame

from databao.core.data_source import DataSource, DBDataSource, DBTDataSource, DFDataSource, Sources
from databao.core.sources import SourcesManager
from databao.databases import (
    DBConnection,
    DBConnectionConfig,
    DBConnectionRuntime,
    create_db_config_file,
    create_db_config_from_runtime,
    try_create_db_config_from_content,
)
from databao.dbt import create_dbt_config_file, try_extract_dbt_dir_from_content
from databao.integrations.dce import DatabaoContextApi


class Domain(Protocol):
    """
    A domain contains a collection of data sources and optional metadata that can be used by one or more agents.

    Use it to:
    - register databases, data frames, and dbt projects as sources
    - attach source-specific and general descriptions
    - (if supported) build and provide context to improve agent quality and performance
    """

    def add_db(self, db: DBConnection, *, name: str | None = None, description: str | Path | None = None) -> None:
        """
        Register a database connection in the domain.

        Args:
            db (DBConnection): Database connection to be added.
                Can be a runtime connection (DBConnectionRuntime) or a connection config (DBConnectionConfig).
            name (str | None): Optional name to assign to the database connection.
                If not provided, a default name such as 'db1', 'db2', etc., will be generated dynamically.
            description (str | Path | None): Optional text or path to a file describing the database for an agent.
        """
        ...

    def add_df(self, df: DataFrame, *, name: str | None = None, description: str | Path | None = None) -> None:
        """
        Register a data frame in the domain.

        Args:
            df (DataFrame): Data frame to be added.
            name (str | None): Optional name to assign to the data frame.
                If not provided, a default name such as 'db1', 'db2', etc., will be generated dynamically.
            description (str | Path | None): Optional text or path to a file describing the data frame for an agent.
        """
        ...

    def add_dbt(self, dbt: str | Path, *, name: str | None = None, description: str | Path | None = None) -> None:
        """
        Register a dbt project in the domain.

        Args:
            dbt (str | Path): Path to dbt project to be added.
            name (str | None): Optional name to assign to the data frame.
                If not provided, a default name such as 'dbt1', 'dbt2', etc., will be generated dynamically.
            description (str | Path | None): Optional text or path to a file describing the dbt project for an agent.
        """
        ...

    def add_description(self, description: str | Path) -> None:
        """
        Add a general description in the domain to help an agent understand your data.

        Use this method to add information that might not be associated with a specific data source.
        If the information is specific to a data source, use the `description` argument of `add_db`, `add_df`, etc.

        Args:
            description (str | Path): Text or path to a file containing the description for an agent.
        """
        ...

    def build_context(self) -> None:
        """
        Builds context for the domain sources to be used by an agent during execution.

        - Available only if the domain supports context (`supports_context`).
        - May be a time-consuming operation.
        - If supported and not called explicitly, it will be invoked automatically by an agent when execution starts.
        - If the context is already built (`is_context_built`), the call has no effect.
        """
        ...

    def is_context_built(self) -> bool:
        """
        Indicates whether the context for the domain sources is already built and ready for use.
        """
        ...

    @property
    def supports_context(self) -> bool:
        """
        Indicates whether the domain supports building and providing context for its sources.

        Having context available can significantly improve an agent's quality and performance.
        """
        ...


class _Domain(ABC, Domain):
    def __init__(self) -> None:
        self._sources_manager = SourcesManager()

    def add_db(self, db: DBConnection, *, name: str | None = None, description: str | Path | None = None) -> None:
        self._add_db(db, name=name, description=description)

    def add_df(self, df: DataFrame, *, name: str | None = None, description: str | Path | None = None) -> None:
        self._add_df(df, name=name, description=description)

    def add_dbt(self, dbt: str | Path, *, name: str | None = None, description: str | Path | None = None) -> None:
        self._add_dbt(dbt, name=name, description=description)

    def add_description(self, description: str | Path | None = None) -> None:
        self._sources_manager.add_description(description)

    @property
    def sources(self) -> Sources:
        self._finalize_sources()
        return self._sources_manager.sources

    def _add_db(
        self, db: DBConnection, name: str | None = None, description: str | Path | None = None
    ) -> DBDataSource | None:
        if isinstance(db, DBConnectionConfig):
            db_config = db
        elif isinstance(db, DBConnectionRuntime):
            db_config = create_db_config_from_runtime(db)
        else:
            raise ValueError("Database connection must be an instance of DBConnection.")
        return self._sources_manager.add_db(db_config, name=name, description=description)

    def _add_df(
        self, df: DataFrame, name: str | None = None, description: str | Path | None = None
    ) -> DFDataSource | None:
        return self._sources_manager.add_df(df, name=name, description=description)

    def _add_dbt(
        self, dbt: str | Path, name: str | None = None, description: str | Path | None = None
    ) -> DBTDataSource | None:
        if isinstance(dbt, str):
            dbt = Path(dbt)
        return self._sources_manager.add_dbt(dbt, name=name, description=description)

    def _finalize_sources(self) -> None:
        self._sources_manager.finalize()


class _InMemoryDomain(_Domain):
    def build_context(self) -> None:
        raise ValueError("Context cannot be built in memory mode.")

    def is_context_built(self) -> bool:
        return False

    @property
    def supports_context(self) -> bool:
        return False


class _DCEProjectDomain(_Domain):
    def __init__(self, project_dir: Path):
        super().__init__()
        self._dce_project = DatabaoContextApi.init_or_get_dce_project(project_dir)
        self._dce = DatabaoContextApi.get_dce(project_dir)
        self._configured: dict[str, DatasourceId] = {}
        self._set_configured_data_sources()

    def add_db(self, db: DBConnection, *, name: str | None = None, description: str | Path | None = None) -> None:
        ds = self._add_db(db, name=name, description=description)
        if ds is None:
            return
        config_file = create_db_config_file(ds.config, ds.name)
        configured_ds = self._dce_project.create_datasource_config(config_file)
        self._register_configured(configured_ds.datasource.id, ds)

    def add_dbt(self, dbt: str | Path, *, name: str | None = None, description: str | Path | None = None) -> None:
        ds = self._add_dbt(dbt, name, description)
        if ds is None:
            return
        config_file = create_dbt_config_file(ds.dir, ds.name)
        configured_ds = self._dce_project.create_datasource_config(config_file)
        self._register_configured(configured_ds.datasource.id, ds)

    def build_context(self) -> None:
        if self.is_context_built():
            return
        self._finalize_sources()
        results = self._dce_project.build_context()
        failed = [r for r in results if r.status == DatasourceStatus.FAILED]
        if failed:
            messages = "\n".join(f"  - {r.datasource_id}: {r.error}" for r in failed)
            raise RuntimeError(f"{len(failed)} datasource(s) failed to build context:\n{messages}")

    def search_context(self, retrieve_text: str, datasource_name: str | None = None) -> list[ContextSearchResult]:
        if not self.is_context_built():
            raise ValueError("Context is not built. Call build_context() before searching.")
        datasource_ids = self._create_datasource_id_list(datasource_name)
        return self._dce.search_context(retrieve_text, datasource_ids=datasource_ids)

    def is_context_built(self) -> bool:
        if self._dce.is_context_built():
            return True
        # NOTE: (@gas) dbt datasources are configured in DCE but may not appear in the
        # introspected list; treat them as "built" when present.
        return len(self._configured) > 0

    @property
    def supports_context(self) -> bool:
        return True

    def _set_configured_data_sources(self) -> None:
        configured_data_sources = self._dce_project.get_configured_datasource_list()
        if len(configured_data_sources) == 0:
            return
        for configured_ds in configured_data_sources:
            id = configured_ds.datasource.id
            type = configured_ds.datasource.type
            content = configured_ds.config
            name = self._get_datasource_name(configured_ds)
            if content is None:
                raise ValueError("Only configurable sources from a DCE project are supported.")
            ds = self._add_source_by_configuration(type, content, name)
            if ds is None:
                raise ValueError(f'Failed to add configured source: "{name}" of type "{type.full_type}".')
            self._register_configured(id, ds)
        self._finalize_sources()

    def _add_source_by_configuration(
        self, type: DatasourceType, content: dict[str, Any], name: str
    ) -> DataSource | None:
        db_config = try_create_db_config_from_content(type, content)
        if db_config is not None:
            return self._add_db(db_config, name=name)
        dbt_dir = try_extract_dbt_dir_from_content(type, content)
        if dbt_dir is not None:
            return self._add_dbt(dbt_dir, name=name)
        return None

    def _register_configured(self, id: DatasourceId, ds: DataSource) -> None:
        self._configured[ds.name] = id

    def _create_datasource_id_list(self, ds_name: str | None) -> list[DatasourceId] | None:
        datasource_id = self._configured.get(ds_name) if ds_name else None
        return [datasource_id] if datasource_id else None

    @staticmethod
    def _get_datasource_name(configured_ds: ConfiguredDatasource) -> str:
        id = configured_ds.datasource.id
        return DatabaoContextApi.get_datasource_name(id)
