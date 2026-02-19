from pathlib import Path

from pandas import DataFrame

from databao.core.data_source import DBDataSource, DFDataSource, Sources
from databao.databases import DBConnectionConfig, is_connectable


class SourcesManager:
    def __init__(self) -> None:
        self._sources: Sources = Sources(dfs={}, dbs={}, additional_context=[])
        self._is_finalized = False

    def add_db(
        self,
        config: DBConnectionConfig,
        *,
        name: str | None = None,
        context: str | Path | None = None,
    ) -> DBDataSource | None:
        for db in self._sources.dbs.values():
            if db.config == config:
                return None

        name = name or f"db{len(self._sources.dbs) + 1}"
        self._check_source_can_be_added(name)

        context_text = self._parse_context_arg(context) or ""

        source = DBDataSource(
            name=name,
            context=context_text,
            config=config,
            connectable=is_connectable(config.type),
        )
        self._sources.dbs[name] = source
        return source

    def add_df(
        self, df: DataFrame, *, name: str | None = None, context: str | Path | None = None
    ) -> DFDataSource | None:
        name = name or f"df{len(self._sources.dfs) + 1}"
        self._check_source_can_be_added(name)

        context_text = self._parse_context_arg(context) or ""

        source = DFDataSource(name=name, context=context_text, df=df)
        self._sources.dfs[name] = source
        return source

    def add_context(self, context: str | Path | None) -> None:
        text = self._parse_context_arg(context)
        if text is None:
            raise ValueError("Invalid context provided.")
        self._sources.additional_context.append(text)

    def finalize(self) -> None:
        if self._sources.is_empty:
            raise ValueError("No sources registered.")
        self._is_finalized = True

    @property
    def is_finalized(self) -> bool:
        return self._is_finalized

    @property
    def sources(self) -> Sources:
        return self._sources

    def _check_source_can_be_added(self, name: str) -> None:
        if self._is_finalized:
            raise ValueError("SourcesManager is finalized and cannot be modified.")
        if self._sources.contains(name):
            raise ValueError(f"Source with name {name} already exists.")

    @staticmethod
    def _parse_context_arg(context: str | Path | None) -> str | None:
        if context is None:
            return None
        if isinstance(context, Path):
            return context.read_text()
        return context
