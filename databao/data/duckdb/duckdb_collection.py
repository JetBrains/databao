from itertools import chain
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import pandas as pd
from duckdb import DuckDBPyConnection
from sqlalchemy import Engine, create_engine

from databao.core.data_source import DataSource, SemanticDict
from databao.data.configs.data_source_config import DataSourceConfig
from databao.data.configs.schema_inspection_config import InspectionOptions
from databao.data.configs.sqlalchemy_data_source_config import SqlAlchemyDataSourceConfig
from databao.data.database_schema_types import DatabaseSchema
from databao.data.duckdb.duckdb_source import DatabaseSource, DataFrameSource
from databao.data.duckdb.utils import inspect_duckdb_schema, list_inspectable_duckdb_tables
from databao.data.sqlalchemy_source import SqlAlchemyDataSource


class _DuckDBSqlAlchemySource(SqlAlchemyDataSource):
    def _inspect_database_schema(self, database_or_schema: str | None) -> DatabaseSchema:
        # Using sqlalchemy's inspection/reflection doesn't work with registered databases, as we get
        # sqlalchemy.exc.NoSuchTableError exceptions.
        with self.engine.connect() as con:
            return inspect_duckdb_schema(con, database_or_schema)


class DuckDBCollectionConfig(DataSourceConfig):
    use_in_memory_database: bool = False
    # Set to False when using asyncio with multiple threads!


class DuckDBCollection(DataSource[DuckDBCollectionConfig]):
    """A collection of data sources unified within a single DuckDB database.

    The sources are registered as "views" in the DuckDB database. Consequently, different
    sources can be JOIN-ed together in a single query.
    """

    def __init__(self, config: DuckDBCollectionConfig | None = None) -> None:
        super().__init__(config or DuckDBCollectionConfig(name="duckdb_collection"))
        self._sa_source: _DuckDBSqlAlchemySource
        self._db_sources: dict[str, DatabaseSource] = {}
        self._df_sources: dict[str, DataFrameSource] = {}
        self._data_changed = False
        self._tmp_dir: TemporaryDirectory[str] | None = None
        self._init_engine()

    @property
    def config(self) -> DuckDBCollectionConfig:
        return self._config

    def _init_engine(self) -> None:
        # TODO Support multiple threads with in-memory databases!!!
        # When using multiple threads, we need to materialize added dfs into a file-backed duckdb database.
        # Inspecting in-memory databases with multiple threads is broken in sqlalchemy:
        # https://github.com/Mause/duckdb_engine/issues/1110
        # In native duckdb, it should work fine if done correctly: https://duckdb.org/docs/stable/guides/python/multiple_threads
        # TODO Using the in-memory database doesn't work when streaming LLM responses.
        #  Presumably because streaming spawns a new thread.
        if self._config.use_in_memory_database:
            sa_engine = create_engine("duckdb:///:memory:", connect_args={"read_only": False})
        else:
            # We fallback to a file-backed database, where only added dataframes will get materialized.
            db_name = "duck.db"  # This name is used in the schema inspection, so it must be simple
            if self._tmp_dir is not None:
                self._tmp_dir.cleanup()
            self._tmp_dir = TemporaryDirectory()
            db_path = Path(self._tmp_dir.name) / db_name
            sa_engine = create_engine(f"duckdb:///{db_path}", connect_args={"read_only": False})
        sa_config = SqlAlchemyDataSourceConfig(source_type="sqlalchemy", name="duckdb_collection", db_type="duckdb")
        self._sa_source = _DuckDBSqlAlchemySource(sa_config, sa_engine)

    async def execute(self, query: str) -> pd.DataFrame | Exception:
        return await self._sa_source.execute(query)

    def execute_sync(self, query: str) -> pd.DataFrame | Exception:
        return self._sa_source.execute_sync(query)

    async def inspect_schema(self, semantic_dict: SemanticDict, options: InspectionOptions) -> DatabaseSchema:
        return await self._sa_source.inspect_schema(semantic_dict, options)

    def inspect_schema_sync(self, semantic_dict: SemanticDict, options: InspectionOptions) -> DatabaseSchema:
        return self._sa_source.inspect_schema_sync(semantic_dict, options)

    async def close(self) -> None:
        await self._sa_source.close()
        if self._tmp_dir is not None:
            self._tmp_dir.cleanup()

    def close_sync(self) -> None:
        self._sa_source.close_sync()
        if self._tmp_dir is not None:
            self._tmp_dir.cleanup()

    def add_df(self, df: pd.DataFrame, *, name: str | None = None, additional_context: str | None = None) -> None:
        df_name = name or f"df{len(self._df_sources) + 1}"
        source = DataFrameSource(
            df_name,
            df,
            additional_context=additional_context,
            materialize_df=not self._config.use_in_memory_database,
        )
        self._df_sources[df_name] = source
        self._data_changed = True

    def add_db(self, engine: Engine, *, name: str | None = None, additional_context: str | None = None) -> None:
        db_name = name or f"db{len(self._db_sources) + 1}"
        source = DatabaseSource(db_name, engine, additional_context=additional_context)
        self._db_sources[db_name] = source
        self._data_changed = True

    def register_data_sources(self) -> None:
        if not self._data_changed:
            return

        # Reset the engine and re-register all sources, as otherwise we get "Failed to attach database" errors.
        self._init_engine()

        with self._sa_source.engine.connect() as con:
            for source in chain(self._df_sources.values(), self._db_sources.values()):
                source.register(con)
            con.commit()
            inspectable_tables = list_inspectable_duckdb_tables(con)

        # Limit inspection only to registered schemas, excluding attached system tables
        schemas_to_inspect = sorted(set(f"{catalog}.{schema}" for catalog, schema, _ in inspectable_tables))
        self._config.database_or_schema = schemas_to_inspect
        self._sa_source.config.database_or_schema = schemas_to_inspect

        self._data_changed = False

    def make_duckdb_connection(self) -> DuckDBPyConnection:
        # This method is for temporary backwards compatibility only.
        con = duckdb.connect(database=":memory:", read_only=False)
        for source in chain(self._df_sources.values(), self._db_sources.values()):
            source.register(con)
        return con

    @property
    def db_sources(self) -> list[DatabaseSource]:
        return list(self._db_sources.values())

    @property
    def df_sources(self) -> list[DataFrameSource]:
        return list(self._df_sources.values())
