from itertools import chain
from pathlib import Path

import duckdb
import pandas as pd
from duckdb import DuckDBPyConnection
from sqlalchemy import Engine, create_engine

from portus.core.data_source import DataSource, SemanticDict
from portus.data.configs.data_source_config import DataSourceConfig
from portus.data.configs.schema_inspection_config import InspectionOptions
from portus.data.configs.sqlalchemy_data_source_config import SqlAlchemyDataSourceConfig
from portus.data.database_schema_types import DatabaseSchema
from portus.data.duckdb.duckdb_source import DatabaseSource, DataFrameSource
from portus.data.duckdb.utils import inspect_duckdb_schema, list_inspectable_duckdb_tables
from portus.data.sqlalchemy_source import SqlAlchemyDataSource


class _DuckDBSqlAlchemySource(SqlAlchemyDataSource):
    def _inspect_database_schema(self, database_or_schema: str | None) -> DatabaseSchema:
        # Using sqlalchemy's inspection/reflection doesn't work with registered databases, as we get
        # sqlalchemy.exc.NoSuchTableError exceptions.
        with self.engine.connect() as con:
            return inspect_duckdb_schema(con, database_or_schema)


class DuckDBCollectionConfig(DataSourceConfig):
    pass


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
        self._init_engine()

    @property
    def config(self) -> DuckDBCollectionConfig:
        return self._config

    def _init_engine(self) -> None:
        # Inspecting in-memory databases with multiple threads is broken:
        # https://github.com/Mause/duckdb_engine/issues/1110
        # sa_engine = create_engine("duckdb:///:memory:", connect_args={"read_only": False})
        # Therefore, we are falling back to a file-backed database, where only added dataframes will get materialized.
        # TODO Figure out a better solution.
        db_name = "_duckdb_collection.db"
        db_path = Path(db_name)
        if db_path.exists():
            db_path.unlink()

        sa_engine = create_engine(f"duckdb:///{db_name}", connect_args={"read_only": False})
        sa_config = SqlAlchemyDataSourceConfig(source_type="sqlalchemy", name="duckdb_collection", db_type="duckdb")
        self._sa_source = _DuckDBSqlAlchemySource(sa_config, sa_engine)

    async def execute(self, query: str) -> pd.DataFrame | Exception:
        return await self._sa_source.execute(query)

    async def inspect_schema(self, semantic_dict: SemanticDict, options: InspectionOptions) -> DatabaseSchema:
        return await self._sa_source.inspect_schema(semantic_dict, options)

    async def close(self) -> None:
        await self._sa_source.close()

    def add_df(self, df: pd.DataFrame, *, name: str | None = None, additional_context: str | None = None) -> None:
        df_name = name or f"df{len(self._df_sources) + 1}"
        source = DataFrameSource(df_name, df, additional_context=additional_context)
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
            # TODO register in add_* methods
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
