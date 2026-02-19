from pathlib import Path

from pandas import DataFrame

from databao.core.data_source import DBDataSource, DBTDataSource, DFDataSource, Sources
from databao.databases import DBConnectionConfig


class SourcesManager:
    def __init__(self) -> None:
        self._sources: Sources = Sources(dfs={}, dbs={}, dbts={}, additional_description=[])
        self._is_finalized = False

    def add_db(
        self, db_config: DBConnectionConfig, *, name: str | None = None, description: str | Path | None = None
    ) -> DBDataSource | None:
        for db in self._sources.dbs.values():
            if db.config == db_config:
                return None

        name = name or f"db{len(self._sources.dbs) + 1}"
        self._check_source_can_be_added(name)

        desc_text = self._parse_description_arg(description) or ""

        source = DBDataSource(name=name, description=desc_text, config=db_config)
        self._sources.dbs[name] = source
        return source

    def add_df(
        self, df: DataFrame, *, name: str | None = None, description: str | Path | None = None
    ) -> DFDataSource | None:
        name = name or f"df{len(self._sources.dfs) + 1}"
        self._check_source_can_be_added(name)

        desc_text = self._parse_description_arg(description) or ""

        source = DFDataSource(name=name, description=desc_text, df=df)
        self._sources.dfs[name] = source
        return source

    def add_dbt(
        self, dbt_dir: Path, *, name: str | None = None, description: str | Path | None = None
    ) -> DBTDataSource | None:
        name = name or f"dbt{len(self._sources.dbts) + 1}"
        self._check_source_can_be_added(name)

        desc_text = self._parse_description_arg(description) or ""

        source = DBTDataSource(name=name, description=desc_text, dir=dbt_dir)
        self._sources.dbts[name] = source
        return source

    def add_description(self, description: str | Path | None) -> None:
        text = self._parse_description_arg(description)
        if text is None:
            raise ValueError("Invalid description provided.")
        self._sources.additional_description.append(text)

    def finalize(self) -> None:
        if self._sources.is_empty:
            raise ValueError("No sources registered.")
        self._is_finalized = True

    @property
    def sources(self) -> Sources:
        if not self._is_finalized:
            raise ValueError("Sources are not finalized.")
        return self._sources

    def _check_source_can_be_added(self, name: str) -> None:
        if self._is_finalized:
            raise ValueError("Sources are finalized and cannot be modified.")
        if self._sources.contains(name):
            raise ValueError(f'Source with name "{name}" already exists.')

    @staticmethod
    def _parse_description_arg(description: str | Path | None) -> str | None:
        if description is None:
            return None
        if isinstance(description, Path):
            return description.read_text()
        return description
