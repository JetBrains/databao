import asyncio
import itertools
import re
import warnings
from collections.abc import Collection
from typing import Any, Self

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine.interfaces import DBAPIConnection
from sqlalchemy.event import listen
from sqlalchemy.pool import ConnectionPoolEntry
from tqdm.asyncio import tqdm

from portus.caching.disk_cache import DiskCache, DiskCacheConfig
from portus.data_source.config_classes.schema_inspection_config import InspectionOptions, ValueSamplingStrategy
from portus.data_source.config_classes.sql_alchemy_data_source_config import SqlAlchemyDataSourceConfig
from portus.data_source.data_source import DataSource, SemanticDict
from portus.data_source.database_schema import format_values_list
from portus.data_source.database_schema_types import (
    ColumnSchema,
    ColumnValuesStats,
    DatabaseSchema,
    TableSchema,
    TopKValuesElement,
)
from portus.data_source.database_types import (
    is_aggregate_function,
    is_array_dtype,
    is_datetime_dtype,
    is_id_column,
    is_low_cardinality_dtype,
    is_numeric_dtype,
    is_string_dtype,
)
from portus.data_source.sqlalchemy_utils import (
    GeneralSchemaValueStats,
    execute_sql_query,
    execute_sql_query_sync,
    fetch_distinct_values,
    inspect_database_schema,
    retrieve_first_order_numeric_stats,
    retrieve_formal_string_stats,
    retrieve_general_stats,
    retrieve_top_k_values,
)


class SqlAlchemyDataSource(DataSource[SqlAlchemyDataSourceConfig]):
    def __init__(self, config: SqlAlchemyDataSourceConfig, engine: sa.Engine):
        super().__init__(config)
        self.engine = engine
        # TODO: a datasource should be able to have multiple databases and just loop
        #  through them at inspection time / theoreticaly to something with them at query time
        #  - e.g. have a hook to check if exactly they are called from a larger set of databases within a project
        #  - e.g. as is bigquery-public-data.*

        # Instance-level semaphore to limit concurrent async operations (queries, schema inspection)
        self._semaphore = asyncio.Semaphore(config.max_concurrent_requests)

    @classmethod
    def from_config(cls, config: SqlAlchemyDataSourceConfig) -> Self:
        engine = create_db_engine(config)
        return cls(config, engine)

    @property
    def config(self) -> SqlAlchemyDataSourceConfig:
        return self._config

    def preprocess_query_hook(self, query: str) -> str:
        if self.config.db_type == "ms_sqlserver" and self.config.limit_max_rows is not None:
            # Limit the amount of rows using SET ROWCOUNT as there is no connection-level
            # limit possible in SQL Server. Using TOP N would be better but more challenging to implement.
            # https://learn.microsoft.com/en-us/sql/t-sql/statements/set-rowcount-transact-sql?view=sql-server-ver17
            query = f"SET ROWCOUNT {self.config.limit_max_rows};\n{query};\nSET ROWCOUNT 0;"
        return query

    async def aexecute(self, query: str, *, enable_hooks: bool = True) -> pd.DataFrame | Exception:
        if enable_hooks:
            query = self.preprocess_query_hook(query)
        async with self._semaphore:
            return await execute_sql_query(self.engine, query)

    def execute(self, query: str, *, enable_hooks: bool = True) -> pd.DataFrame | Exception:
        if enable_hooks:
            query = self.preprocess_query_hook(query)
        return execute_sql_query_sync(self.engine, query)

    def inspect_schema(self, semantic_dict: SemanticDict, options: InspectionOptions) -> DatabaseSchema:
        # TODO Proper non-async version
        return asyncio.run(self.ainspect_schema(semantic_dict, options))

    # TODO: improve these names! inspect schema / inspect schema / inspect schema...
    async def ainspect_schema(
        self,
        semantic_dict: SemanticDict,
        options: InspectionOptions,
    ) -> DatabaseSchema:
        schema_names: list[str]
        if self.config.database_or_schema is None:
            schema_names = self._get_all_schema_names()
        elif isinstance(self.config.database_or_schema, str):
            schema_names = [self.config.database_or_schema]
        else:
            schema_names = self.config.database_or_schema

        if len(schema_names) == 0:
            raise RuntimeError("No schemas found!")

        database_schemas: list[DatabaseSchema] = await asyncio.gather(
            *[self._inspect_schema(semantic_dict, options, database_or_schema) for database_or_schema in schema_names]
        )
        return self._merge_database_schemas(database_schemas)

    def _merge_database_schemas(self, database_schemas: list[DatabaseSchema]) -> DatabaseSchema:
        """
        In case the data source has a group of schemas, we must merge the results of the schema inspection into a
        single DatabaseSchema object. The plugin classes will need to adjust this to their concrete use cases.
        """
        tables: list[TableSchema] = list(itertools.chain(*[schema.tables.values() for schema in database_schemas]))
        return DatabaseSchema(db_type=self.config.db_type, tables={table.qualified_name: table for table in tables})

    def _inspect_database_schema(self, database_or_schema: str) -> DatabaseSchema:
        """
        Wrapper around the inspect_database_schema function so that we can easily override it in plugins.
        """
        return inspect_database_schema(self.engine, database_or_schema)

    def _get_all_schema_names(self) -> list[str]:
        inspector = sa.inspect(self.engine)
        return inspector.get_schema_names()

    async def _inspect_schema(
        self,
        semantic_dict: SemanticDict,
        options: InspectionOptions,
        database_or_schema: str,
    ) -> DatabaseSchema:
        """
        Inspect a single database or schema.
        """
        # TODO move common logic to DataSource?
        if options.cache_intermediate_results:
            # TODO how/when to invalidate the cache?
            cache = DiskCache(DiskCacheConfig())
            # Storing json keys/values allows querying like `SELECT json_extract(tag, '$.source') FROM Cache;`
            cache_dict = {
                "type": "inspect_schema",
                "source_type": self.config.source_type,
                "source": self.name,
                "database_or_schema": database_or_schema,
                "options": options.model_dump_for_cache(),
            }
        else:
            cache = None

        out_schema = DatabaseSchema(db_type=self.config.db_type, name=self.name, description=None)

        raw_schema = self._inspect_database_schema(database_or_schema)
        raw_table_names = {t.name: t for t in raw_schema.tables.values()}

        # TODO handle fully qualified table names in semantic_dict?
        if semantic_dict == "full":
            semantic_dict = {"tables": {t.name: "all" for t in raw_schema.tables.values()}}

        if options.tables_regex is not None:
            tables_regex = re.compile(options.tables_regex)
            for table in raw_schema.tables.values():
                # semantic dict always takes precedence
                if table.name in semantic_dict["tables"]:
                    continue
                if tables_regex.fullmatch(table.name):
                    semantic_dict["tables"][table.name] = "__all__"  # to distinguish from a user provided "all"

        semantic_tables = semantic_dict["tables"]

        async def process_column(*, table_name: str, col_name: str, col_desc: str, col_dtype: str) -> ColumnSchema:
            async def _process() -> ColumnSchema:
                async with self._semaphore:
                    # Run the synchronous column inspection in a thread pool
                    # (using the default max_workers of the thread pool)
                    column_values, column_value_stats = await asyncio.to_thread(
                        self._inspect_column_values_helper,
                        database_or_schema=database_or_schema,
                        table_name=table_name,
                        col_name=col_name,
                        dtype=col_dtype,
                        options=options,
                    )

                return ColumnSchema(
                    name=col_name,
                    dtype=col_dtype,
                    description=col_desc,
                    values=column_values,
                    value_stats=column_value_stats,
                )

            if cache is not None:
                cache_tag = f"{self.name}/inspect_schema"
                cache_key = cache.make_json_key(
                    cache_dict | {"path": f"{cache_tag}/{database_or_schema}/{table_name}/{col_name}"}
                )
                if cache_key in cache:
                    return ColumnSchema.model_validate_json(cache.get(cache_key))
            column = await _process()
            if cache is not None:
                cache.set(cache_key, column.model_dump_json(), tag=cache_tag)
            return column

        async def process_table(table_name: str) -> TableSchema:
            if table_name not in raw_table_names:
                raise ValueError(f"Table {table_name} doesn't exist.")

            raw_table = raw_table_names[table_name]
            semantic_table: dict[str, Any]
            if semantic_tables[table_name] in ("all", "__all__"):
                semantic_table = {"description": "", "columns": {col_name: "" for col_name in raw_table.columns}}
            else:
                semantic_table = semantic_tables[table_name]

            # "all" takes precedence over regex filtering
            if semantic_tables[table_name] != "all" and options.columns_regex is not None:
                columns_regex = re.compile(options.columns_regex)
                for col_name in raw_table.columns:
                    if semantic_tables[table_name] != "__all__" and col_name in semantic_table["columns"]:
                        continue
                    if columns_regex.fullmatch(col_name):
                        semantic_table["columns"][col_name] = ""

            table_desc = semantic_table.get("description", "")
            table_schema = TableSchema(
                name=table_name, schema_name=raw_table.schema_name, description=table_desc, columns={}
            )

            # Process columns concurrently
            column_tasks = []
            for col_name, col_desc in semantic_table["columns"].items():
                if col_name not in raw_table.columns:
                    raise ValueError(
                        f"Column {table_name}.{col_name} doesn't exist. "
                        f"Available columns: {list(raw_table.columns.keys())}"
                    )
                col_dtype = raw_table.columns[col_name].dtype
                column_tasks.append(
                    process_column(table_name=table_name, col_name=col_name, col_desc=col_desc, col_dtype=col_dtype)
                )
            columns: Collection[ColumnSchema] = await asyncio.gather(*column_tasks)
            for col_schema in columns:
                table_schema.columns[col_schema.name] = col_schema
            return table_schema

        # Process all tables concurrently
        table_tasks = [process_table(table_name) for table_name in semantic_tables]
        tables: Collection[TableSchema] = await tqdm.gather(*table_tasks, desc="Inspecting schema")
        for table_schema in tables:
            out_schema.tables[table_schema.name] = table_schema

        if cache is not None:
            cache.close()
        return out_schema

    def _inspect_column_values_helper(
        self, *, database_or_schema: str, table_name: str, col_name: str, dtype: str, options: InspectionOptions
    ) -> tuple[list[str], ColumnValuesStats]:
        """
        Conduct column profiling.
        Returns:
            list[str]: All values (for low-cardinality columns only) - this option is kept for backward compatibility
                and in order to test if including column statistics improves or degrades performance against only
                returning the values from categorical columns.
            ColumnValuesStats: profiling information about a column (includes most common values from all types of
                columns (both numeric and categorical)).
        """
        # Synchronous version that creates its own connection, to be used in its own thread
        with self.engine.connect() as conn:
            return self._inspect_column_values(
                conn,
                database_or_schema=database_or_schema,
                table_name=table_name,
                col_name=col_name,
                dtype=dtype,
                options=options,
            )

    def _retrieve_general_stats(
        self, conn: sa.Connection, database_or_schema: str, table_name: str, col_name: str, *, dtype: str
    ) -> GeneralSchemaValueStats:
        return retrieve_general_stats(conn, database_or_schema, table_name, col_name)

    def _inspect_column_values(
        self,
        conn: sa.Connection,
        *,
        database_or_schema: str,
        table_name: str,
        col_name: str,
        dtype: str,
        options: InspectionOptions,
    ) -> tuple[list[str], ColumnValuesStats]:
        if options.value_sampling_strategy == ValueSamplingStrategy.NONE and not options.inspect_column_stats:
            return [], ColumnValuesStats()

        general_stats = self._retrieve_general_stats(conn, database_or_schema, table_name, col_name, dtype=dtype)

        low_cardinality_values: list[str] = []
        top_k_values: list[TopKValuesElement] | None = None

        if options.value_sampling_strategy == ValueSamplingStrategy.CATEGORICAL_ONLY:  # noqa: SIM102
            if (
                is_low_cardinality_dtype(dtype)
                or (general_stats["n_unique"] is not None and general_stats["n_unique"] < options.max_enum_values)
            ) and is_string_dtype(dtype):
                # N.B. There is duplication due to identical columns being in different tables.
                values = fetch_distinct_values(
                    conn, database_or_schema, table_name, col_name, limit=options.max_enum_values + 1
                )
                low_cardinality_values = format_values_list(values, max_values=options.max_enum_values)

        if options.value_sampling_strategy == ValueSamplingStrategy.TOP_P:  # noqa: SIM102
            # We don't need to sample id columns, as they are in most of the cases just long strings / sequences
            # of integers that don't have any meaning and just clutter the context. Same goes for dates and
            # other numerical columns where the uniqueness rate is high - there we don't need to get most
            # common values as these will again just be cluttering the context.
            if (
                (
                    not is_id_column(col_name)
                    and not is_datetime_dtype(dtype)
                    and not is_array_dtype(dtype)
                    and not is_aggregate_function(dtype)
                    and not general_stats["unique_rate"] > options.max_unique_rate
                )
                # safety net for the cases where we have small tables and the uniqueness rate is high
                # - in an industrial setting it would probably be important to provide these values
                or (general_stats["n_unique"] is not None and general_stats["n_unique"] < options.max_enum_values)
            ):
                top_k_values = retrieve_top_k_values(conn, database_or_schema, table_name, col_name)

        numeric_stats = (
            retrieve_first_order_numeric_stats(conn, database_or_schema, table_name, col_name)
            if (is_numeric_dtype(dtype) and not is_id_column(col_name)) and options.inspect_column_stats
            else None
        )

        string_stats = (
            retrieve_formal_string_stats(conn, database_or_schema, table_name, col_name)
            if is_string_dtype(dtype) and options.inspect_column_stats
            else None
        )

        return low_cardinality_values, ColumnValuesStats(
            **(numeric_stats or {}),  # type: ignore[arg-type]
            **((general_stats if options.inspect_column_stats else None) or {}),
            **(string_stats or {}),
            top_k_values_with_frequencies=top_k_values,
        )

    def close(self) -> None:
        self.engine.dispose()

    async def aclose(self) -> None:
        self.close()


def check_is_db_ready(engine: sa.Engine) -> None:
    try:
        table_names = sa.inspect(engine).get_table_names() + sa.inspect(engine).get_view_names()
    except Exception as e:  # e.g. connection error
        raise ValueError("Failed to connect to the database. Is the docker container running?") from e

    if len(table_names) == 0:
        raise ValueError("The database is empty. Populate it before running the benchmark.")


def create_db_engine(config: SqlAlchemyDataSourceConfig, *, check_if_ready: bool = False) -> sa.Engine:
    url = config.get_url()

    # N.B. connect_args will work also for metricflow, since the workflow is MF -> SQL -> DB
    connect_args: dict[str, Any] = {}
    kwargs: dict[str, Any] = {}
    if config.db_type == "clickhouse":
        # See https://clickhouse.com/docs/operations/settings/settings for available settings
        args: dict[str, Any] = {}

        # https://clickhouse.com/docs/operations/settings/settings#readonly
        args["readonly"] = "2"

        if config.limit_max_rows is not None:
            # 'max_result_rows' (+ 'result_overflow_mode') can return more than the limit rows,
            # and it also checks subqueries
            # 'limit' seems to work by adding a LIMIT clause to queries
            args["limit"] = config.limit_max_rows

        if config.query_timeout is not None:
            # N.B. ClickHouse can start sending results immediately, but the data transfer can take
            # more time than the timeout. In that case, no exception will be raised by ClickHouse!
            # E.g. `select * from customers` takes >10s with no exception even with a 1s timeout.
            # You can check that the timeout actually works with `select sleep(3)`
            args["receive_timeout"] = config.query_timeout
            args["http_receive_timeout"] = config.query_timeout
            args["connect_timeout"] = config.query_timeout
            connect_args["timeout"] = config.query_timeout

        # N.B. ch_settings is not documented anywhere but I found it in clickhouse_sqlalchemy/drivers/http/transport.py
        connect_args["ch_settings"] = args
    elif config.db_type == "ms_sqlserver":
        # Limit max rows per query.
        pass
    else:
        warnings.warn(f"Unverified support for database type {config.db_type}.", stacklevel=2)

    # pre-ping for dealing with disconnects: https://docs.sqlalchemy.org/en/20/core/pooling.html#dealing-with-disconnects
    engine = sa.create_engine(url, pool_pre_ping=True, connect_args=connect_args, **kwargs)
    if check_if_ready:
        check_is_db_ready(engine)

    if config.db_type == "ms_sqlserver" and config.query_timeout is not None:
        # Query timeout in seconds. See the comment for ClickHouse above (it works the same way here).
        # We can't set the timeout in connect_args: https://stackoverflow.com/questions/76543954/when-executing-queries-against-sql-server-via-sql-alchemy-and-pandas-how-do-i
        def mssql_connect_event_handler(conn: DBAPIConnection, record: ConnectionPoolEntry) -> None:
            conn.timeout = config.query_timeout

        listen(engine, "connect", mssql_connect_event_handler)

    return engine
