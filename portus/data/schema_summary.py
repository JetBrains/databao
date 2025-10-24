from collections.abc import Iterable
from typing import Any

from portus.data.configs.schema_inspection_config import SchemaSummaryType
from portus.data.database_schema_types import DatabaseSchema, TableSchema


def summarize_database_metadata(schema: DatabaseSchema, *, include_name: bool = False) -> str:
    s = ""
    if include_name and schema.name:
        # LLMs sometimes try to use the name in SELECT FROM statements, which can cause errors.
        s += f"{schema.name}"
        if schema.description:
            s += f": {schema.description}"
        s += "\n"
    s += f" Database type: {schema.db_type}\n"
    return s


def summarize_table_schema(table_schema: TableSchema) -> str:
    # TODO make it configurable whether to use fully qualified names or not
    # TODO general table stats: num rows, num columns
    s = f"\n## Table `{table_schema.qualified_name}`\n"
    if table_schema.description:
        s += f"{table_schema.description}\n"
    for column_name, column_schema in table_schema.columns.items():
        desc = "" if not column_schema.description else f": {column_schema.description}."
        if not column_schema.value_stats.is_empty():
            value_stats_summary = column_schema.value_stats.summarize()
            value_stats_summary = value_stats_summary.strip()
            desc = f"{desc} {value_stats_summary}"
        if column_schema.values:
            if desc:
                desc = f"{desc} Values: {', '.join(column_schema.values)}"
            else:
                desc = f": Values: [{', '.join(column_schema.values)}]"
        dtype = normalize_dtype(column_schema.dtype)
        s += f" - `{column_name}` ({dtype}){desc}\n"
    return s


def summarize_table_schemas(schema: DatabaseSchema, table_names: list[str]) -> str:
    s = ""
    for table_name in table_names:
        s += summarize_table_schema(schema.tables[table_name])
    return s


def summarize_full_schema(schema: DatabaseSchema) -> str:
    """
    The "Full" summary type summarizes the database by providing the summaries of each of the tables which (summaries)
    contain the summaries of all columns within a table.
    """
    s = summarize_database_metadata(schema)
    for table_schema in schema.tables.values():
        s += summarize_table_schema(table_schema)
    return s


def summarize_list_all_tables(
    schema: DatabaseSchema, *, include_descriptions: bool = True, max_description_length: int | None = None
) -> str:
    """
    The `List all tables` summary type summarizes the database schema by listing all ONLY the
    verbatim names of the tables and (if `include_descriptions` is True) their descriptions as extracted
    from the database metadata. (no column summaries are included)
    """
    # TODO Per-schema list. Currently we assume a single schema.
    s = summarize_database_metadata(schema)
    s += "Table names:\n"
    for table_schema in schema.tables.values():
        s += f"- `{table_schema.qualified_name}`"
        if include_descriptions and table_schema.description:
            s += f": {trim_string(table_schema.description, max_description_length)}"
        s += "\n"
    return s


def summarize_compact_schema(schema: DatabaseSchema, *, max_cols_per_table: int | None = None) -> str:
    lines = []
    for table_schema in schema.tables.values():
        cols = []
        for col_schema in table_schema.columns.values():
            cols.append((col_schema.name, col_schema.dtype))
        if max_cols_per_table is not None and len(cols) > max_cols_per_table:
            cols = cols[:max_cols_per_table]
            suffix = " ... (truncated)"
        else:
            suffix = ""
        col_desc = ", ".join(f"{c} {t}" for c, t in cols)
        lines.append(f"{table_schema.qualified_name}({col_desc}){suffix}")
    s = summarize_database_metadata(schema)
    s += "\n".join(lines)
    return s


def summarize_schema(schema: DatabaseSchema, summary_type: SchemaSummaryType) -> str:
    match summary_type:
        case SchemaSummaryType.FULL:
            return summarize_full_schema(schema)
        case SchemaSummaryType.LIST_ALL_TABLES:
            return summarize_list_all_tables(schema)
        case SchemaSummaryType.COMPACT:
            return summarize_compact_schema(schema)


def summarize_schemas(schemas: Iterable[DatabaseSchema], summary_type: SchemaSummaryType) -> str:
    return "\n\n".join(summarize_schema(schema, summary_type) for schema in schemas if schema.tables)


def normalize_dtype(dtype: str) -> str:
    # Remove collation from dtype (returned by sqlalchemy for some sqlserver types), e.g.,
    # VARCHAR(50) COLLATE "SQL_Latin1_General_CP1_CI_AS"
    only_dtype, _, _ = dtype.partition(" COLLATE ")
    return only_dtype


def format_values_list(values_list: list[Any], max_values: int = 10) -> list[str]:
    """Format and truncate a distinct values list."""
    if len(values_list) > max_values:
        values_list = [*values_list[: max_values // 2], "..."]
    return [str(s) for s in values_list]  # str() for None


def trim_string(s: str, max_length: int | None) -> str:
    if max_length is None:
        return s
    if len(s) > max_length:
        return s[: max_length - 3] + "..."
    return s
