import asyncio
import re
import warnings
from collections.abc import Collection
from typing import Any, Literal, Self

import pandas as pd
import sqlalchemy as sa
from pydantic import SecretStr
from sqlalchemy import URL, make_url
from sqlalchemy.engine.interfaces import DBAPIConnection
from sqlalchemy.event import listen
from sqlalchemy.pool import ConnectionPoolEntry
from tqdm.asyncio import tqdm

from portus.caching.disk_cache import DiskCache, DiskCacheConfig
from portus.data_source.data_source import DataSource, DataSourceConfig, SemanticDict
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
from portus.data_source.schema_inspection_config import InspectionOptions, ValueSamplingStrategy
from portus.data_source.sqlalchemy_utils import (
    execute_sql_query,
    execute_sql_query_sync,
    fetch_distinct_values,
    inspect_database_schema,
    retrieve_first_order_numeric_stats,
    retrieve_formal_string_stats,
    retrieve_general_stats,
    retrieve_top_k_values,
)


class SqlAlchemyConfig(DataSourceConfig):
    source_type: Literal["sqlalchemy"]
    db_type: str

    url: SecretStr | None = None
    """URL to connect to the database. If None, the driver, user, password, host, port, schema, and db_options will be used to construct the URL."""

    driver: SecretStr | None = None
    user: SecretStr | None = None
    password: SecretStr | None = None
    host: SecretStr | None = None
    port: int | None = None
    database_or_schema: str | None = None
    """Database or schema to use for queries. 
    
    For some dialects (ClickHouse), this sets the default schema. 
    For others (PostgreSQL), it sets the default database, in which case you need to specify the default schema using `db_options`. 
    
    N.B. metricflow SQL queries will work regardless of which default schema is used because the 
    generated SQL contains fully qualified tables
    """
    db_options: SecretStr | None = None
    """Options to add to the url, without the initial question mark (?)."""

    limit_max_rows: int | None = 10000
    """Limit how many rows can be returned by the database for all queries. If None, all rows are returned."""
    query_timeout: int | None = 120
    """Database query result timeout in seconds. None means the DB default timeout (usually no timeout).
    
    A database can start sending results immediately, but the data transfer can take more time than the timeout.
    In that case, no timeout exception will be raised. E.g. `select * from customers` might take >10s with no exception 
    even with a 1s timeout. You can check that the timeout actually works with e.g., `select sleep(3)`.
    """

    max_concurrent_requests: int = 8
    """Maximum number of concurrent requests to the database."""

    def get_url(self) -> URL:
        if self.url is not None:
            return make_url(self.url.get_secret_value())
        assert self.driver is not None
        url = URL.create(
            drivername=self.driver.get_secret_value(),
            username=self.user.get_secret_value() if self.user is not None else None,
            password=self.password.get_secret_value() if self.password is not None else None,
            host=self.host.get_secret_value() if self.host is not None else None,
            port=self.port if self.port is not None else None,
            database=self.database_or_schema if self.database_or_schema is not None else None,
        )
        if self.db_options is not None:
            url = url.update_query_string(self.db_options.get_secret_value())
        return url


class SqlAlchemyDataSource(DataSource):
    def __init__(self, config: SqlAlchemyConfig, engine: sa.Engine) -> None:
        self._config = config
        self.engine = engine

        # Instance-level semaphore to limit concurrent async operations (queries, schema inspection)
        self._semaphore = asyncio.Semaphore(config.max_concurrent_requests)

    @classmethod
    def from_config(cls, config: SqlAlchemyConfig) -> Self:
        engine = create_db_engine(config)
        return cls(config, engine)

    @property
    def config(self) -> SqlAlchemyConfig:
        return self._config

    def preprocess_query_hook(self, query: str) -> str:
        if self.config.db_type == "ms_sqlserver" and self.config.limit_max_rows is not None:
            # Limit the amount of rows using SET ROWCOUNT as there is no connection-level limit possible in SQL Server.
            # Using TOP N would be better but much more challenging to implement.
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
        raise NotImplementedError

    async def ainspect_schema(
        self,
        semantic_dict: SemanticDict,
        options: InspectionOptions,
    ) -> DatabaseSchema:
        # TODO move common logic to DataSource?
        if options.cache_intermediate_results:
            # TODO how/when to invalidate the cache?
            cache = DiskCache(DiskCacheConfig())
            # Storing json keys/values allows querying like `SELECT json_extract(tag, '$.source') FROM Cache;`
            cache_dict = {
                "type": "inspect_schema",
                "source_type": self.config.source_type,
                "source": self.name,
                "options": options.model_dump_for_cache(),
            }
        else:
            cache = None

        out_schema = DatabaseSchema(db_type=self.config.db_type, name=self.name, description=None)
        raw_schema = inspect_database_schema(self.engine)

        if semantic_dict == "full":
            semantic_dict = {"tables": {table_name: "all" for table_name in raw_schema.tables}}

        if options.tables_regex is not None:
            tables_regex = re.compile(options.tables_regex)
            for table_name in raw_schema.tables:
                # semantic dict always takes precedence
                if table_name in semantic_dict["tables"]:
                    continue
                if tables_regex.fullmatch(table_name):
                    semantic_dict["tables"][table_name] = "__all__"  # to distinguish from a user provided "all"

        semantic_tables = semantic_dict["tables"]

        async def process_column(*, table_name: str, col_name: str, col_desc: str, col_dtype: str) -> ColumnSchema:
            async def _process() -> ColumnSchema:
                async with self._semaphore:
                    # Run the synchronous column inspection in a thread pool (using the default max_workers of the thread pool)
                    column_values, column_value_stats = await asyncio.to_thread(
                        self._inspect_column_values_helper,
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
                cache_key = cache.make_json_key(cache_dict | {"path": f"{cache_tag}/{table_name}/{col_name}"})
                if cache_key in cache:
                    return ColumnSchema.model_validate_json(cache.get(cache_key))
            column = await _process()
            if cache is not None:
                cache.set(cache_key, column.model_dump_json(), tag=cache_tag)
            return column

        async def process_table(table_name: str) -> TableSchema:
            if table_name not in raw_schema.tables:
                raise ValueError(f"Table {table_name} doesn't exist.")

            raw_table = raw_schema.tables[table_name]
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
                        f"Column {table_name}.{col_name} doesn't exist. Available columns: {list(raw_table.columns.keys())}"
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
        self, *, table_name: str, col_name: str, dtype: str, options: InspectionOptions
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
                conn, table_name=table_name, col_name=col_name, dtype=dtype, options=options
            )

    def _inspect_column_values(
        self, conn: sa.Connection, *, table_name: str, col_name: str, dtype: str, options: InspectionOptions
    ) -> tuple[list[str], ColumnValuesStats]:
        if options.value_sampling_strategy == ValueSamplingStrategy.NONE and not options.inspect_column_stats:
            return [], ColumnValuesStats()

        general_stats = retrieve_general_stats(conn, table_name, col_name)

        low_cardinality_values: list[str] = []
        top_k_values: list[TopKValuesElement] | None = None

        if options.value_sampling_strategy == ValueSamplingStrategy.CATEGORICAL_ONLY:  # noqa: SIM102
            if (
                is_low_cardinality_dtype(dtype) or general_stats["n_unique"] < options.max_enum_values
            ) and is_string_dtype(dtype):
                # N.B. There is duplication due to identical columns being in different tables.
                values = fetch_distinct_values(conn, table_name, col_name, options.max_enum_values + 1)
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
                or general_stats["n_unique"] < options.max_enum_values
            ):
                top_k_values = retrieve_top_k_values(conn, table_name, col_name)

        numeric_stats = (
            retrieve_first_order_numeric_stats(conn, table_name, col_name)
            if (is_numeric_dtype(dtype) and not is_id_column(col_name)) and options.inspect_column_stats
            else None
        )

        string_stats = (
            retrieve_formal_string_stats(conn, table_name, col_name)
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


def create_db_engine(config: SqlAlchemyConfig, *, check_if_ready: bool = False) -> sa.Engine:
    url = config.get_url()

    # N.B. connect_args will work also for metricflow, since the workflow is MF -> SQL -> DB
    connect_args: dict[str, Any] = {}
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
    engine = sa.create_engine(url, pool_pre_ping=True, connect_args=connect_args)
    if check_if_ready:
        check_is_db_ready(engine)

    if config.db_type == "ms_sqlserver" and config.query_timeout is not None:
        # Query timeout in seconds. See the comment for ClickHouse above (it works the same way here).
        # We can't set the timeout in connect_args: https://stackoverflow.com/questions/76543954/when-executing-queries-against-sql-server-via-sql-alchemy-and-pandas-how-do-i
        def mssql_connect_event_handler(conn: DBAPIConnection, record: ConnectionPoolEntry) -> None:
            conn.timeout = config.query_timeout

        listen(engine, "connect", mssql_connect_event_handler)

    return engine
