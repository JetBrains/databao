import asyncio
import logging
from typing import Any, TypedDict

import pandas as pd
import requests
import sqlalchemy as sa
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_random_exponential

from portus.data_source.database_schema_types import ColumnSchema, DatabaseSchema, TableSchema, TopKValuesElement

_LOGGER = logging.getLogger(__name__)


class GeneralSchemaValueStats(TypedDict):
    n_total: int
    n_unique: int
    unique_rate: float
    null_rate: float


class FirstOrderNumericStats(TypedDict):
    min: float
    max: float
    mean: float
    std: float | None


class FormalStringStats(TypedDict):
    min_str_len: int
    max_str_len: int
    mean_str_len: float
    contains_punctuation: bool
    contains_whitespace: bool
    contains_numbers: bool
    all_lowercase_rate: float
    all_uppercase_rate: float
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


def inspect_database_schema(engine: sa.Engine) -> DatabaseSchema:
    inspector = sa.inspect(engine)
    schema = DatabaseSchema(db_type=inspector.dialect.name)
    all_names = inspector.get_table_names() + inspector.get_view_names()
    for table_name in all_names:
        columns = {}
        for column in inspector.get_columns(table_name):
            columns[column["name"]] = ColumnSchema(name=column["name"], dtype=str(column["type"]))
        schema.tables[table_name] = TableSchema(
            name=table_name, schema_name=inspector.default_schema_name, columns=columns
        )
    return schema


def fetch_distinct_values(conn: sa.Connection, table_name: str, col_name: str, limit: int | None = None) -> list[Any]:
    """Fetch distinct values for a column in a dialect-agnostic way.."""
    try:
        tbl = sa.table(table_name, sa.column(col_name))
        query = sa.select(tbl.c[col_name]).distinct().order_by(tbl.c[col_name]).limit(limit)
        return list(conn.execute(query).scalars().all())
    except Exception as e:
        raise ValueError(f"Error fetching values for {table_name}.{col_name}: {e}") from e


def retrieve_general_stats(conn: sa.Connection, table_name: str, col_name: str) -> GeneralSchemaValueStats:
    """Counts the number of distinct values, rate of unique values, rate of null values in a dialect-agnostic way."""
    tbl = sa.table(table_name, sa.column(col_name))
    column = tbl.c[col_name]
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


def retrieve_first_order_numeric_stats(conn: sa.Connection, table_name: str, col_name: str) -> FirstOrderNumericStats:
    """Return the min, max, mean and std."""
    tbl = sa.table(table_name, sa.column(col_name))

    # SQLServer throws an "Arithmetic overflow error converting expression to data type int"
    # when calculating averages so cast to a bigger type
    mean_query = (
        sa.func.avg(sa.cast(tbl.c[col_name], sa.DECIMAL)).label("mean")
        if conn.dialect.name == "mssql"
        else sa.func.avg(tbl.c[col_name]).label("mean")
    )

    query = sa.select(
        sa.func.min(tbl.c[col_name]).label("min"),
        sa.func.max(tbl.c[col_name]).label("max"),
        mean_query,
    )

    mapping = conn.execute(query).mappings().one()

    dialect_specific_mapping: dict[str, Any] = {}
    if conn.dialect.name == "clickhouse":
        query = sa.select(
            sa.func.stddevPop(tbl.c[col_name]).label("std"),
        )
        dialect_specific_mapping = dict(conn.execute(query).mappings().one())
    else:
        _LOGGER.warning(f"Only clickhouse is supported for token counts, current dialect is: {conn.dialect.name}.")

    return FirstOrderNumericStats(
        min=mapping["min"], max=mapping["max"], mean=mapping["mean"], std=dialect_specific_mapping.get("std")
    )


def retrieve_formal_string_stats(conn: sa.Connection, table_name: str, col_name: str) -> FormalStringStats:
    """
    Get the first order string stats like min/max character length / min/max token length
    """
    tbl = sa.table(table_name, sa.column(col_name))
    char_length_subquery = sa.func.length(tbl.c[col_name])

    column = tbl.c[col_name]

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

    mapping = conn.execute(query).mappings().one()

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
    conn: sa.Connection, table_name: str, col_name: str, *, top_k: int = 50
) -> list[TopKValuesElement]:
    """Return a mapping of the top k values to their frequencies."""
    tbl = sa.table(table_name, sa.column(col_name))
    column = tbl.c[col_name]
    query = (
        sa.select(
            column.label("value"),
            sa.func.count(column).label("count"),
        )
        .group_by(column)
        .order_by(sa.desc("count"))
        .limit(top_k)
    )

    total_count = conn.execute(sa.select(sa.func.count(column))).mappings().one()["count"]

    result = conn.execute(query).mappings().all()

    result = [
        TopKValuesElement(**(dict(i) | {"frequency": i["count"] / total_count if total_count > 0 else 0}))
        for i in result
    ]

    return result
