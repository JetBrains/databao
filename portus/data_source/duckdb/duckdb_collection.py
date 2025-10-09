from pathlib import Path

import duckdb
import pandas as pd
from duckdb import DuckDBPyConnection
from sqlalchemy import Engine, create_engine

from portus.caching.schema_inspection_cache import get_inspection_cache_key
from portus.data_source.config_classes.data_source_config import DataSourceConfig
from portus.data_source.config_classes.schema_inspection_config import InspectionOptions
from portus.data_source.config_classes.sql_alchemy_data_source_config import SqlAlchemyDataSourceConfig
from portus.data_source.data_source import DataSource, SemanticDict
from portus.data_source.database_schema_types import DatabaseSchema
from portus.data_source.duckdb.database_source import DatabaseSource
from portus.data_source.duckdb.dataframe_source import DataFrameSource
from portus.data_source.duckdb.duckdb_source import DuckDBSource
from portus.data_source.duckdb.utils import inspect_duckdb_schema, list_inspectable_duckdb_tables
from portus.data_source.sqlalchemy_source import SqlAlchemyDataSource


class _DuckDBSqlAlchemySource(SqlAlchemyDataSource):
    def _inspect_database_schema(self, database_or_schema: str) -> DatabaseSchema:
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
        self._sources: list[DuckDBSource] = []
        self._data_changed = False

        # TODO Better caching that is common to all sources
        # Cache to avoid inspecting the schema on every agent query
        self._schema_inspection_cache: dict[str, DatabaseSchema] = {}

        self._init_engine()

    @property
    def config(self) -> DuckDBCollectionConfig:
        return self._config

    @property
    def sources(self) -> list[DuckDBSource]:
        return self._sources

    def _init_engine(self) -> None:
        # Inspecting in-memory databases with multiple threads is broken:
        # https://github.com/Mause/duckdb_engine/issues/1110
        # sa_engine = create_engine("duckdb:///:memory:", connect_args={"read_only": False})
        # Therefore, we are falling back to a file-backed database, where only added dataframes will get materialized.
        db_name = "duck.db"
        db_path = Path(db_name)
        if db_path.exists():
            db_path.unlink()

        sa_engine = create_engine(f"duckdb:///{db_name}", connect_args={"read_only": False})
        sa_config = SqlAlchemyDataSourceConfig(source_type="sqlalchemy", name="duckdb_collection", db_type="duckdb")
        self._sa_source = _DuckDBSqlAlchemySource(sa_config, sa_engine)

    def execute(self, query: str) -> pd.DataFrame | Exception:
        return self._sa_source.execute(query)

    async def aexecute(self, query: str) -> pd.DataFrame | Exception:
        return await self._sa_source.aexecute(query)

    def inspect_schema(self, semantic_dict: SemanticDict, options: InspectionOptions) -> DatabaseSchema:
        cache_key = get_inspection_cache_key(semantic_dict, options)
        if cache_key in self._schema_inspection_cache:
            return self._schema_inspection_cache[cache_key]
        schema = self._sa_source.inspect_schema(semantic_dict, options)
        self._schema_inspection_cache[cache_key] = schema
        return schema

    async def ainspect_schema(self, semantic_dict: SemanticDict, options: InspectionOptions) -> DatabaseSchema:
        cache_key = get_inspection_cache_key(semantic_dict, options)
        if cache_key in self._schema_inspection_cache:
            return self._schema_inspection_cache[cache_key]
        schema = await self._sa_source.ainspect_schema(semantic_dict, options)
        self._schema_inspection_cache[cache_key] = schema
        return schema

    def close(self) -> None:
        self._sa_source.close()

    async def aclose(self) -> None:
        self.close()

    def add_df(self, df: pd.DataFrame, name: str | None = None) -> None:
        df_name = name or f"df_{len(self._sources) + 1}"
        source = DataFrameSource(df, df_name)
        self._sources.append(source)
        self._data_changed = True

    def add_db(self, engine: Engine, name: str | None = None) -> None:
        name = name or f"db_{len(self._sources) + 1}"
        source = DatabaseSource(engine, name)
        self._sources.append(source)
        self._data_changed = True

    def commit(self) -> None:
        if not self._data_changed:
            return

        # Reset the engine and re-register all sources, as otherwise we get "Failed to attach database" errors.
        self._init_engine()
        self._schema_inspection_cache = {}

        with self._sa_source.engine.connect() as con:
            for source in self._sources:
                source.register(con)
            con.commit()
            inspectable_tables = list_inspectable_duckdb_tables(con)

        # Limit inspection only to registered schemas, excluding attached system tables
        schemas_to_inspect = sorted(set(f"{catalog}.{schema}" for catalog, schema, _ in inspectable_tables))
        self._config.database_or_schema = schemas_to_inspect
        self._sa_source.config.database_or_schema = schemas_to_inspect

        self._data_changed = False

    def get_duckdb_connection(self) -> DuckDBPyConnection:
        # This function is for temporary backwards compatibility only.
        con = duckdb.connect(database=":memory:", read_only=False)
        for source in self._sources:
            source.register(con)
        return con
