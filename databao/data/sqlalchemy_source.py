import asyncio
import itertools
import logging
import warnings
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

import pandas as pd
import sqlalchemy as sa
import tqdm
from sqlalchemy.engine.interfaces import DBAPIConnection
from sqlalchemy.event import listen
from sqlalchemy.pool import ConnectionPoolEntry
from tqdm.asyncio import tqdm_asyncio

from databao.caches.disk_cache import DiskCache, DiskCacheConfig
from databao.data.configs.schema_inspection_config import InspectionOptions, ValueSamplingStrategy
from databao.data.configs.sqlalchemy_data_source_config import SqlAlchemyDataSourceConfig
from databao.data.data_source import DataSource
from databao.data.database_schema_types import (
    ColumnSchema,
    ColumnValuesStats,
    DatabaseSchema,
    TableSchema,
    TopKValuesElement,
)
from databao.data.database_type_utils import (
    is_aggregate_function,
    is_array_dtype,
    is_datetime_dtype,
    is_id_column,
    is_low_cardinality_dtype,
    is_numeric_dtype,
    is_string_dtype,
)
from databao.data.schema_summary import format_values_list
from databao.data.sqlalchemy_utils import (
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

_LOGGER = logging.getLogger(__name__)


@dataclass(kw_only=True)
class ColumnInspectionTask:
    task_id: int
    table_name: str
    col_name: str
    col_dtype: str
    database_or_schema: str | None
    options: InspectionOptions
    cache_key: str | None = None


@dataclass(kw_only=True, frozen=True)
class ColumnInspectionResult:
    task_id: int
    column_schema: ColumnSchema


@dataclass(kw_only=True, frozen=True)
class TableInspectionTask:
    table_name: str
    schema_name: str | None
    column_tasks: list[ColumnInspectionTask]


class SqlAlchemyDataSource(DataSource[SqlAlchemyDataSourceConfig]):
    def __init__(self, config: SqlAlchemyDataSourceConfig, engine: sa.Engine | None = None) -> None:
        super().__init__(config)
        self.engine = engine or self._create_db_engine()
        # TODO: a datasource should be able to have multiple databases and just loop thought them at inspection time
        #  / theoreticaly to something with them at query time
        #  - e.g. have a hook to check if exactly they are called from a larger set of databases within a project
        #  - e.g. as is bigquery-public-data.*

        # Instance-level semaphore to limit concurrent async operations (queries, schema inspection)
        self._semaphore = asyncio.Semaphore(config.max_concurrent_requests)

    @property
    def config(self) -> SqlAlchemyDataSourceConfig:
        return self._config

    def preprocess_query_hook(self, query: str) -> str:
        if self.config.db_type == "ms_sqlserver" and self.config.limit_max_rows is not None:
            # Limit the amount of rows using SET ROWCOUNT as there is no connection-level limit possible in SQL Server.
            # Using TOP N would be better but much more challenging to implement.
            # https://learn.microsoft.com/en-us/sql/t-sql/statements/set-rowcount-transact-sql?view=sql-server-ver17
            query = f"SET ROWCOUNT {self.config.limit_max_rows};\n{query};\nSET ROWCOUNT 0;"
        return query

    async def execute(self, query: str, *, enable_hooks: bool = True) -> pd.DataFrame | Exception:
        if enable_hooks:
            query = self.preprocess_query_hook(query)
        async with self._semaphore:
            return await execute_sql_query(self.engine, query)

    def execute_sync(self, query: str, *, enable_hooks: bool = True) -> pd.DataFrame | Exception:
        if enable_hooks:
            query = self.preprocess_query_hook(query)
        return execute_sql_query_sync(self.engine, query)

    # TODO: improve these names! inspect schema / inspect schema / inspect schema...
    async def inspect_schema(self, options: InspectionOptions) -> DatabaseSchema:
        async def _inspect(database_or_schema: str | None) -> DatabaseSchema:
            generator = self._inspect_schema_helper(options, database_or_schema)
            column_inspection_tasks = generator.send(None)
            tasks = [self._run_column_inspection_task(task) for task in column_inspection_tasks]
            column_inspection_results = await tqdm_asyncio.gather(*tasks, desc="Inspecting columns")
            try:
                generator.send(column_inspection_results)
            except StopIteration as e:
                assert isinstance(e.value, DatabaseSchema)
                return e.value
            raise RuntimeError("inspect_schema didn't return")  # for mypy

        if self.config.database_or_schema is None or isinstance(self.config.database_or_schema, str):
            return await _inspect(self.config.database_or_schema)

        database_schemas: list[DatabaseSchema] = await asyncio.gather(
            *[_inspect(database_or_schema) for database_or_schema in self.config.database_or_schema]
        )

        return self._merge_database_schemas(database_schemas)

    def inspect_schema_sync(self, options: InspectionOptions) -> DatabaseSchema:
        def _inspect(database_or_schema: str | None) -> DatabaseSchema:
            generator = self._inspect_schema_helper(options, database_or_schema)
            column_inspection_tasks = generator.send(None)
            column_inspection_results = [
                self._run_column_inspection_task_sync(task)
                for task in tqdm.tqdm(column_inspection_tasks, desc=f"Inspecting '{database_or_schema or 'full'}'")
            ]
            try:
                generator.send(column_inspection_results)
            except StopIteration as e:
                assert isinstance(e.value, DatabaseSchema)
                return e.value
            raise RuntimeError("inspect_schema didn't return")  # for mypy

        if self.config.database_or_schema is None or isinstance(self.config.database_or_schema, str):
            return _inspect(self.config.database_or_schema)
        database_schemas = [_inspect(database_or_schema) for database_or_schema in self.config.database_or_schema]
        return self._merge_database_schemas(database_schemas)

    def _merge_database_schemas(self, database_schemas: list[DatabaseSchema]) -> DatabaseSchema:
        """
        In case the data source has a group of schemas, we must merge the results of the schema inspection into a
        single DatabaseSchema object. The plugin classes will need to adjust this to their concrete use cases.
        """
        tables = list(itertools.chain(*[schema.tables.values() for schema in database_schemas]))
        return DatabaseSchema(db_type=self.config.db_type, tables={table.qualified_name: table for table in tables})

    def _inspect_database_schema(self, database_or_schema: str | None) -> DatabaseSchema:
        """
        Wrapper around the inspect_database_schema function so that we can easily override it in plugins.
        """
        return inspect_database_schema(self.engine, database_or_schema)

    def _create_db_engine(self) -> sa.Engine:
        return create_db_engine(self.config)

    async def _run_column_inspection_task(self, task: ColumnInspectionTask) -> ColumnInspectionResult:
        async with self._semaphore:
            # Run the synchronous column inspection in a thread pool
            # (using the default max_workers of the thread pool)
            column_values, column_value_stats = await asyncio.to_thread(
                self._inspect_column_values_helper,
                database_or_schema=task.database_or_schema,
                table_name=task.table_name,
                col_name=task.col_name,
                dtype=task.col_dtype,
                options=task.options,
            )
            return ColumnInspectionResult(
                task_id=task.task_id,
                column_schema=ColumnSchema(
                    name=task.col_name,
                    dtype=task.col_dtype,
                    values=column_values,
                    value_stats=column_value_stats,
                ),
            )

    def _run_column_inspection_task_sync(self, task: ColumnInspectionTask) -> ColumnInspectionResult:
        column_values, column_value_stats = self._inspect_column_values_helper(
            database_or_schema=task.database_or_schema,
            table_name=task.table_name,
            col_name=task.col_name,
            dtype=task.col_dtype,
            options=task.options,
        )
        return ColumnInspectionResult(
            task_id=task.task_id,
            column_schema=ColumnSchema(
                name=task.col_name,
                dtype=task.col_dtype,
                values=column_values,
                value_stats=column_value_stats,
            ),
        )

    def _inspect_schema_helper(
        self,
        options: InspectionOptions,
        database_or_schema: str | None,
    ) -> Generator[list[ColumnInspectionTask], list[ColumnInspectionResult] | None, DatabaseSchema]:
        # TODO move common logic to DataSource?
        if options.cache_intermediate_results:
            # TODO how/when to invalidate the cache?
            # TODO support using Session's Cache and scope
            cache = DiskCache(DiskCacheConfig())
            # Storing json keys/values allows querying like `SELECT json_extract(tag, '$.source') FROM Cache;`
            cache_dict = {
                "type": "inspect_schema",
                "source_type": self.config.source_type,
                "source": self.name,
                "database_or_schema": database_or_schema,
                "options": options.model_dump_for_cache(),
            }
            cache_tag = f"{self.name}/inspect_schema"
        else:
            cache = None

        table_inspection_tasks = self._get_table_inspection_tasks(options, database_or_schema)
        all_column_tasks = []
        for table_task in table_inspection_tasks:
            all_column_tasks.extend(table_task.column_tasks)

        # Handle cached column tasks here to avoid duplicating the code in the coroutine caller.
        todo_column_tasks = []
        column_inspection_results = []
        for column_task in all_column_tasks:
            if cache is not None:
                cache_key = cache.make_json_key(
                    cache_dict
                    | {"path": f"{cache_tag}/{database_or_schema}/{column_task.table_name}/{column_task.col_name}"}
                )
                column_task.cache_key = cache_key
                if cache_key in cache:
                    column_schema = ColumnSchema.model_validate_json(cache.get_object(cache_key))
                    column_inspection_results.append(
                        ColumnInspectionResult(task_id=column_task.task_id, column_schema=column_schema)
                    )
                    continue
            todo_column_tasks.append(column_task)

        # Yield tasks to be processed and receive the results.
        # The coroutine caller can decide to process the tasks in parallel or sequentially.
        todo_column_inspection_results = yield todo_column_tasks
        assert todo_column_inspection_results is not None  # None is just to start a generator
        column_inspection_results.extend(todo_column_inspection_results)

        # Cache new column inspection results if needed
        if cache is not None:
            task_id_to_task = {t.task_id: t for t in todo_column_tasks}
            for column_inspection_result in todo_column_inspection_results:
                column_task = task_id_to_task[column_inspection_result.task_id]
                assert column_task.cache_key is not None
                cache.set_object(
                    column_task.cache_key, column_inspection_result.column_schema.model_dump_json(), tag=cache_tag
                )
            cache.close()

        table_schemas = {}
        task_id_to_result = {result.task_id: result for result in column_inspection_results}
        for table_task in table_inspection_tasks:
            table_columns = {}
            for column_task in table_task.column_tasks:
                column_schema = task_id_to_result[column_task.task_id].column_schema
                table_columns[column_schema.name] = column_schema
            table_schema = TableSchema(
                name=table_task.table_name,
                schema_name=table_task.schema_name,
                columns=table_columns,
            )
            table_schemas[table_schema.qualified_name] = table_schema

        db_schema = DatabaseSchema(db_type=self.config.db_type, name=self.name, description=None, tables=table_schemas)
        return db_schema

    def _get_table_inspection_tasks(
        self,
        options: InspectionOptions,
        database_or_schema: str | None,
    ) -> list[TableInspectionTask]:
        # Getting tasks is a _fast_ method.
        raw_schema = self._inspect_database_schema(database_or_schema)
        task_id = 0
        table_tasks = []
        for table in raw_schema.tables.values():
            column_tasks = []
            for col in table.columns.values():
                column_task = ColumnInspectionTask(
                    task_id=task_id,
                    table_name=table.name,
                    col_name=col.name,
                    col_dtype=col.dtype,
                    database_or_schema=database_or_schema,
                    options=options,
                )
                task_id += 1
                column_tasks.append(column_task)
            table_task = TableInspectionTask(
                table_name=table.name,
                schema_name=table.schema_name,
                column_tasks=column_tasks,
            )
            table_tasks.append(table_task)
        return table_tasks

    def _inspect_column_values_helper(
        self, *, database_or_schema: str | None, table_name: str, col_name: str, dtype: str, options: InspectionOptions
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
        self,
        conn: sa.Connection,
        database_or_schema: str | None,
        table_name: str,
        col_name: str,
        *,
        retrieval_set_limit: int | None = None,
    ) -> GeneralSchemaValueStats:
        return retrieve_general_stats(
            conn, database_or_schema, table_name, col_name, retrieval_set_limit=retrieval_set_limit
        )

    def _inspect_column_values(
        self,
        conn: sa.Connection,
        *,
        database_or_schema: str | None,
        table_name: str,
        col_name: str,
        dtype: str,
        options: InspectionOptions,
    ) -> tuple[list[str], ColumnValuesStats]:
        if options.value_sampling_strategy == ValueSamplingStrategy.NONE and not options.inspect_column_stats:
            return [], ColumnValuesStats()

        general_stats = self._retrieve_general_stats(
            conn, database_or_schema, table_name, col_name, retrieval_set_limit=options.retrieval_set_limit
        )

        low_cardinality_values: list[str] = []
        top_k_values: list[TopKValuesElement] | None = None

        if options.value_sampling_strategy == ValueSamplingStrategy.CATEGORICAL_ONLY:  # noqa: SIM102
            if (
                is_low_cardinality_dtype(dtype)
                or (general_stats["n_unique"] is not None and general_stats["n_unique"] < options.max_enum_values)
            ) and is_string_dtype(dtype):
                # N.B. There is duplication due to identical columns being in different tables.
                values = fetch_distinct_values(
                    conn,
                    database_or_schema,
                    table_name,
                    col_name,
                    limit=options.max_enum_values + 1,
                    retrieval_set_limit=options.retrieval_set_limit,
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
                    and not (
                        general_stats["unique_rate"] is not None
                        and general_stats["unique_rate"] > options.max_unique_rate
                    )
                )
                # safety net for the cases where we have small tables and the uniqueness rate is high
                # - in an industrial setting it would probably be important to provide these values
                or (general_stats["n_unique"] is not None and general_stats["n_unique"] < options.max_enum_values)
            ):
                top_k_values = retrieve_top_k_values(
                    conn,
                    database_or_schema,
                    table_name,
                    col_name,
                    retrieval_set_limit=options.retrieval_set_limit,
                )

        numeric_stats = (
            retrieve_first_order_numeric_stats(
                conn,
                database_or_schema,
                table_name,
                col_name,
                retrieval_set_limit=options.retrieval_set_limit,
            )
            if (is_numeric_dtype(dtype) and not is_id_column(col_name)) and options.inspect_column_stats
            else None
        )

        string_stats = (
            retrieve_formal_string_stats(
                conn,
                database_or_schema,
                table_name,
                col_name,
                retrieval_set_limit=options.retrieval_set_limit,
            )
            if is_string_dtype(dtype) and options.inspect_column_stats
            else None
        )

        return low_cardinality_values, ColumnValuesStats(
            **(numeric_stats or {}),  # type: ignore[arg-type]
            **((general_stats if options.inspect_column_stats else None) or {}),
            **(string_stats or {}),
            top_k_values_with_frequencies=top_k_values,
        )

    async def close(self) -> None:
        self.close_sync()

    def close_sync(self) -> None:
        self.engine.dispose()


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
            # 'max_result_rows' (+ 'result_overflow_mode') can return more than the limit rows
            #  and it also checks subqueries
            # 'limit' seems to work by adding a LIMIT clause to queries
            args["limit"] = config.limit_max_rows

        if config.query_timeout is not None:
            # N.B. ClickHouse can start sending results immediately, but the data transfer can
            #  take more time than the timeout.
            #  In that case, no exception will be raised by ClickHouse!
            #  E.g. `select * from customers` takes >10s with no exception even with a 1s timeout.
            #  You can check that the timeout actually works with `select sleep(3)`
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
