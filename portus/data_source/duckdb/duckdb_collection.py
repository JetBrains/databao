import asyncio

import duckdb
import pandas as pd
from sqlalchemy import Engine

from portus.data_source.data_source import DataSource, DataSourceConfig, SemanticDict
from portus.data_source.database_schema_types import ColumnSchema, DatabaseSchema, TableSchema
from portus.data_source.duckdb.database_source import DatabaseSource
from portus.data_source.duckdb.dataframe_source import DataFrameSource
from portus.data_source.duckdb.duckdb_source import DuckDBSource
from portus.data_source.schema_inspection_config import InspectionOptions


class DuckDBCollection(DataSource):
    """A collection of data sources unified within a single DuckDB database.

    The sources are registered as "views" in the DuckDB database. Consequently, different
    sources can be JOIN-ed together in a single query.
    """

    def __init__(self) -> None:
        self._sources: list[DuckDBSource] = []

        # Using sqlalchemy's inspection/reflection doesn't work with registered databases, as we get
        # sqlalchemy.exc.NoSuchTableError exceptions.
        # Additionally, inspecting in-memory databases with multiple threads is also broken:
        # https://github.com/Mause/duckdb_engine/issues/1110
        # Consequently, we are using duckdb directly, without sqlalchemy.
        self._connection = duckdb.connect(database=":memory:", read_only=False)

    @property
    def config(self) -> DataSourceConfig:
        return DataSourceConfig(source_type="duckdb_collection", name="duckdb_collection")

    def execute(self, query: str) -> pd.DataFrame | Exception:
        try:
            return self._connection.execute(query).df()
        except Exception as e:  # TODO handle specific exceptions
            return e

    async def aexecute(self, query: str) -> pd.DataFrame | Exception:
        return await asyncio.to_thread(self.execute, query)

    def inspect_schema(self, semantic_dict: SemanticDict, options: InspectionOptions) -> DatabaseSchema:
        # TODO full inspection like in SqlAlchemyDataSource

        rows = self._connection.execute("""
            SELECT table_catalog, table_schema, table_name
            FROM information_schema.tables
            WHERE table_type IN ('BASE TABLE', 'VIEW')
              AND table_schema NOT IN ('pg_catalog', 'pg_toast', 'information_schema')
            ORDER BY table_schema, table_name
        """).fetchall()

        table_schemas = {}
        for db, schema, table in rows:
            col_rows = self._connection.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = ?
                  AND table_name = ?
                ORDER BY ordinal_position
                """,
                [schema, table],
            ).fetchall()

            col_schemas = {}
            for col_name, col_type in col_rows:
                col_schema = ColumnSchema(name=col_name, dtype=col_type)
                col_schemas[col_name] = col_schema
            table_schema = TableSchema(name=table, schema_name=f"{db}.{schema}", columns=col_schemas)
            table_schemas[table] = table_schema

        db_schema = DatabaseSchema(db_type="duckdb", tables=table_schemas)
        return db_schema

    async def ainspect_schema(self, semantic_dict: SemanticDict, options: InspectionOptions) -> DatabaseSchema:
        return await asyncio.to_thread(self.inspect_schema, semantic_dict, options)

    def close(self) -> None:
        self._connection.close()

    async def aclose(self) -> None:
        self.close()

    def add_df(self, df: pd.DataFrame, name: str | None = None) -> None:
        df_name = name or f"df_{len(self._sources) + 1}"
        source = DataFrameSource(df, df_name)
        self._sources.append(source)

    def add_db(self, engine: Engine, name: str | None = None) -> None:
        name = name or f"db_{len(self._sources) + 1}"
        source = DatabaseSource(engine, name)
        self._sources.append(source)

    def commit(self) -> None:
        for source in self._sources:
            source.register(self._connection)

    @property
    def sources(self) -> list[DuckDBSource]:
        return self._sources
