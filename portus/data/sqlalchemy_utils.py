import asyncio
import logging
from typing import Any, TypedDict

import pandas as pd
import requests
import sqlalchemy as sa
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.sql.elements import KeyedColumnElement
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_random_exponential

from portus.data.database_schema_types import ColumnSchema, DatabaseSchema, TableSchema, TopKValuesElement

_LOGGER = logging.getLogger(__name__)


class GeneralSchemaValueStats(TypedDict):
    n_total: int | None
    n_unique: int | None
    unique_rate: float | None
    null_rate: float | None


class FirstOrderNumericStats(TypedDict):
    min: float | None
    max: float | None
    mean: float | None
    std: float | None


class FormalStringStats(TypedDict):
    min_str_len: int | None
    max_str_len: int | None
    mean_str_len: float | None
    contains_punctuation: bool | None
    contains_whitespace: bool | None
    contains_numbers: bool | None
    all_lowercase_rate: float | None
    all_uppercase_rate: float | None
    min_token_count: int | None
    max_token_count: int | None
    mean_token_count: float | None


_WHITESPACE = [" ", "\n", "\t"]

# punctuation escaped as needed by the `like` query
_PUNCTUATION = [
    ".",  # Period
    ",",  # Comma
    "!",  # Exclamation mark
    "?",  # Question mark
    ":",  # Colon
    ";",  # Semicolon
    "-",  # Hyphen
    "—",  # Em dash
    "(",  # Left parenthesis
    ")",  # Right parenthesis
    "[",  # Left square bracket
    "]",  # Right square bracket
    "{",  # Left curly brace
    "}",  # Right curly brace
    "'",  # Single quotation mark
    '"',  # Double quotation mark
    "`",  # Backtick
    "~",  # Tilde
    "@",  # At symbol
    "#",  # Hash symbol
    "$",  # Dollar sign
    "\%",  # Percent sign (escaped)
    "^",  # Caret
    "&",  # Ampersand
    "\*",  # Asterisk (safe, but sometimes escaped)
    "\_",  # Underscore (escaped)
    "=",  # Equals sign
    "+",  # Plus sign
    "/",  # Forward slash
    "\\\\",  # Backslash (escaped itself)
    "|",  # Vertical bar
    "<",  # Less-than sign
    ">",  # Greater-than sign
]


async def execute_sql_query(engine: sa.Engine, query: str) -> pd.DataFrame | Exception:
    # TODO This function is not properly async: https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html
    return await asyncio.to_thread(execute_sql_query_sync, engine, query)


@retry(
    wait=wait_random_exponential(min=1, max=30),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type(requests.exceptions.ConnectionError),
)
def execute_sql_query_sync(engine: sa.Engine, query: str) -> pd.DataFrame | Exception:
    try:
        with engine.connect() as conn:
            res = conn.execute(sa.text(query))
            df = pd.DataFrame(res.fetchall())
            conn.commit()
            return df
    except requests.exceptions.ConnectionError as e:
        raise e
    except Exception as e:
        return e


def build_sa_column(
    database_or_schema: str | None,
    table_name: str,
    col_name: str,
    *,
    retrieval_set_limit: int | None = None,
    dialect_name: str,
) -> KeyedColumnElement[Any]:
    if "snowflake" in dialect_name:
        col_name = col_name.replace('"', "")
    if retrieval_set_limit:
        # sqlalchemy for snowflake is quite unpredictable when working with
        # aliases - whenever one expects to have a quoted alias it suddenly is not
        # setting quote=True forces it to always be quoted
        tbl = sa.Table(
            table_name,
            sa.MetaData(),
            sa.Column(col_name, quote=True),
            quote=False,
            quote_schema=False,
            schema=database_or_schema,  # <-- schema goes here
        )
        tbl = sa.select(tbl).limit(retrieval_set_limit).subquery()

    else:
        tbl = sa.Table(
            table_name,
            sa.MetaData(),
            sa.Column(col_name, quote=False),
            quote=False,
            quote_schema=False,
            schema=database_or_schema,  # <-- schema goes here
        )

    return tbl.c[col_name]


def inspect_database_schema(engine: sa.Engine, database_or_schema: str | None) -> DatabaseSchema:
    inspector = sa.inspect(engine)
    schema = DatabaseSchema(db_type=inspector.dialect.name)

    all_names = inspector.get_table_names(schema=database_or_schema) + inspector.get_view_names(
        schema=database_or_schema
    )
    for table_name in all_names:
        columns = {}
        for column in inspector.get_columns(table_name=table_name, schema=database_or_schema):
            columns[column["name"]] = ColumnSchema(name=column["name"], dtype=str(column["type"]))
        schema.tables[table_name] = TableSchema(name=table_name, schema_name=database_or_schema, columns=columns)
    return schema


def fetch_distinct_values(
    conn: sa.Connection,
    database_or_schema: str | None,
    table_name: str,
    col_name: str,
    limit: int | None = None,
    *,
    retrieval_set_limit: int | None = None,
) -> list[Any]:
    """Fetch distinct values for a column in a dialect-agnostic way. Quoting schema/table/column names as
    required by some dialects (e.g., snowflake where the qualification must be
    database_name.schema_name.table_name."column_name") is left to the schema inspection.
    """
    column = build_sa_column(
        database_or_schema,
        table_name,
        col_name,
        retrieval_set_limit=retrieval_set_limit,
        dialect_name=conn.dialect.name,
    )
    try:
        query = sa.select(column).distinct().order_by(column).limit(limit)
        return list(conn.execute(query).scalars().all())
    except Exception as e:
        raise ValueError(f"Error fetching values for {table_name}.{col_name}: {e}") from e


def retrieve_general_stats(
    conn: sa.Connection,
    database_or_schema: str | None,
    table_name: str,
    col_name: str,
    *,
    retrieval_set_limit: int | None = None,
) -> GeneralSchemaValueStats:
    """Counts the number of distinct values, rate of unique values, rate of null values in a dialect-agnostic way.
    Quoting schema/table/column names as required by some dialects (e.g., snowflake where the qualification must be
    database_name.schema_name.table_name."column_name") is left to the schema inspection."""
    column = build_sa_column(
        database_or_schema,
        table_name,
        col_name,
        retrieval_set_limit=retrieval_set_limit,
        dialect_name=conn.dialect.name,
    )

    query = sa.select(
        sa.func.count(sa.distinct(column)).label("n_unique"),
        sa.func.count("*").label("n_total"),  # include null values in the count
        sa.func.sum(sa.case((column.is_(None), 1), else_=0)).label("n_null"),
    )
    mapping = conn.execute(query).mappings().one()

    n_total = mapping["n_total"]
    return GeneralSchemaValueStats(
        n_total=n_total,
        n_unique=mapping["n_unique"],
        unique_rate=mapping["n_unique"] / n_total if n_total > 0 else 0,
        null_rate=mapping["n_null"] / n_total if n_total > 0 else 0,
    )


def retrieve_first_order_numeric_stats(
    conn: sa.Connection,
    database_or_schema: str | None,
    table_name: str,
    col_name: str,
    *,
    retrieval_set_limit: int | None = None,
) -> FirstOrderNumericStats:
    """Return the min, max, mean and std. Quoting schema/table/column names as required by some dialects
    (e.g., snowflake where the qualification must be database_name.schema_name.table_name."column_name") is left to the
    schema inspection."""
    column = build_sa_column(
        database_or_schema,
        table_name,
        col_name,
        retrieval_set_limit=retrieval_set_limit,
        dialect_name=conn.dialect.name,
    )

    # SQLServer throws a "Arithmetic overflow error converting expression to data type int"
    # when calculating averages so cast to a bigger type
    mean_query = (
        sa.func.avg(sa.cast(column, sa.DECIMAL)).label("mean")
        if conn.dialect.name == "mssql"
        else sa.func.avg(column).label("mean")
    )

    query = sa.select(
        sa.func.min(column).label("min"),
        sa.func.max(column).label("max"),
        mean_query,
    )
    try:
        mapping = conn.execute(query).mappings().one()
    except ProgrammingError as e:
        _LOGGER.warning(f"ERROR HERE: database: {database_or_schema}, table: {table_name}, col: {col_name}, ERROR: {e}")
        return FirstOrderNumericStats(min=None, max=None, mean=None, std=None)

    dialect_specific_mapping: dict[str, Any] = {}
    if conn.dialect.name == "clickhouse":
        query = sa.select(
            sa.func.stddevPop(column).label("std"),
        )
        dialect_specific_mapping = dict(conn.execute(query).mappings().one())
    else:
        _LOGGER.warning(f"Only clickhouse is supported for token counts, current dialect is: {conn.dialect.name}.")

    return FirstOrderNumericStats(
        min=mapping["min"], max=mapping["max"], mean=mapping["mean"], std=dialect_specific_mapping.get("std")
    )


def retrieve_formal_string_stats(
    conn: sa.Connection,
    database_or_schema: str | None,
    table_name: str,
    col_name: str,
    *,
    retrieval_set_limit: int | None = None,
) -> FormalStringStats:
    """
    Get the first order string stats like min/max character length / min/max token length.
    Quoting schema/table/column names as required by some dialects (e.g., snowflake where the qualification must be
    database_name.schema_name.table_name."column_name") is left to the schema inspection."""
    column = build_sa_column(
        database_or_schema,
        table_name,
        col_name,
        retrieval_set_limit=retrieval_set_limit,
        dialect_name=conn.dialect.name,
    )

    char_length_subquery = sa.func.length(column)

    # N.B. Use MAX(CASE WHEN ... THEN 1 ELSE 0 END) instead of EXISTS for SQL Server compatibility
    punctuation_pred = sa.or_(*[column.like(f"%{ch}%") for ch in _PUNCTUATION]).label("contains_punctuation")
    whitespace_pred = sa.or_(*[column.like(f"%{ch}%") for ch in _WHITESPACE]).label("contains_whitespace")
    number_pred = sa.or_(*[column.like(f"%{ch}%") for ch in range(10)]).label("contains_numbers")

    query = sa.select(
        sa.func.count(column).label("n_total"),  # here we actually want to exclude null values
        sa.func.min(char_length_subquery).label("min_str_len"),
        sa.func.max(char_length_subquery).label("max_str_len"),
        sa.func.avg(char_length_subquery).label("mean_str_len"),
        sa.func.max(sa.case((punctuation_pred, 1), else_=0)).label("contains_punctuation"),
        sa.func.max(sa.case((whitespace_pred, 1), else_=0)).label("contains_whitespace"),
        sa.func.max(sa.case((number_pred, 1), else_=0)).label("contains_numbers"),
        sa.func.sum(sa.case((column == sa.func.lower(column), 1), else_=0)).label("all_lowercase_count"),
        sa.func.sum(sa.case((column == sa.func.upper(column), 1), else_=0)).label("all_uppercase_count"),
    )
    try:
        mapping = conn.execute(query).mappings().one()
    except ProgrammingError as e:
        _LOGGER.warning(f"ERROR HERE: database: {database_or_schema}, table: {table_name}, col: {col_name}, ERROR: {e}")
        return FormalStringStats(
            min_str_len=None,
            max_str_len=None,
            mean_str_len=None,
            contains_punctuation=None,
            contains_whitespace=None,
            contains_numbers=None,
            all_lowercase_rate=None,
            all_uppercase_rate=None,
            min_token_count=None,
            max_token_count=None,
            mean_token_count=None,
        )

    dialect_specific_mapping: dict[str, Any] = {}
    if conn.dialect.name == "clickhouse":
        query = sa.select(
            sa.func.avg(sa.func.length(sa.func.splitByWhitespace(sa.func.coalesce(column, "")))).label(
                "mean_token_count"
            ),
            sa.func.max(sa.func.length(sa.func.splitByWhitespace(sa.func.coalesce(column, "")))).label(
                "max_token_count"
            ),
            sa.func.min(sa.func.length(sa.func.splitByWhitespace(sa.func.coalesce(column, "")))).label(
                "min_token_count"
            ),
        )
        dialect_specific_mapping = dict(conn.execute(query).mappings().one())
    else:
        _LOGGER.warning(f"Only clickhouse is supported for token counts, current dialect is: {conn.dialect.name}.")

    n_total = mapping["n_total"]
    return FormalStringStats(
        min_str_len=mapping["min_str_len"],
        max_str_len=mapping["max_str_len"],
        mean_str_len=mapping["mean_str_len"],
        contains_punctuation=mapping["contains_punctuation"],
        contains_whitespace=mapping["contains_whitespace"],
        contains_numbers=mapping["contains_numbers"],
        all_lowercase_rate=mapping["all_lowercase_count"] / n_total if n_total > 0 else 0.0,
        all_uppercase_rate=mapping["all_uppercase_count"] / n_total if n_total > 0 else 0.0,
        min_token_count=dialect_specific_mapping.get("min_token_count"),
        max_token_count=dialect_specific_mapping.get("max_token_count"),
        mean_token_count=dialect_specific_mapping.get("mean_token_count"),
    )


def retrieve_top_k_values(
    conn: sa.Connection,
    database_or_schema: str | None,
    table_name: str,
    col_name: str,
    *,
    top_k: int = 50,
    retrieval_set_limit: int | None = None,
) -> list[TopKValuesElement]:
    """Return a mapping of the top k values to their frequencies.
    Quoting schema/table/column names as required by some dialects (e.g., snowflake where the qualification must be
    database_name.schema_name.table_name."column_name") is left to the schema inspection."""
    column = build_sa_column(
        database_or_schema,
        table_name,
        col_name,
        retrieval_set_limit=retrieval_set_limit,
        dialect_name=conn.dialect.name,
    )

    query = (
        sa.select(
            column.label("value"),
            sa.func.count(column).label("count"),
        )
        .group_by(column)
        .order_by(sa.desc("count"), sa.desc("value"))
        .limit(top_k)
    )

    try:
        total_count = conn.execute(sa.select(sa.func.count(column))).mappings().one()["count"]

        result = conn.execute(query).mappings().all()
    except ProgrammingError as e:
        _LOGGER.warning(f"ERROR HERE: database: {database_or_schema}, table: {table_name}, col: {col_name}, ERROR: {e}")
        return []

    return [
        TopKValuesElement(**(dict(i) | {"frequency": i["count"] / total_count if total_count > 0 else 0}))
        for i in result
    ]
