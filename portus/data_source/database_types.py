import re

_ID_PREFIXES_AND_SUFFIXES = [
    # suffixes
    r"_id\b",
    r"_key\b",
    r"_pk\b",
    r"_fk\b",
    r"_code\b",
    # prefixes
    r"\bid_",
    r"\bkey_",
    r"\bpk_",
    r"\bfk_",
    r"\bcode_",
]

_ID_COL_REGEX = re.compile(r"(" + "|".join(_ID_PREFIXES_AND_SUFFIXES) + r")")


def is_id_column(col_name: str) -> bool:
    """
    Based on the column name determine if the column holds IDs. (in such case,
    it makes no sense to report certain numerical statistics such as min/max)
    """
    if col_name.lower() in ["id", "key", "pk", "fk", "code"]:
        return True
    return _ID_COL_REGEX.search(col_name) is not None


def is_array_dtype(dtype: str) -> bool:
    """Determine if a data type is an array type in a dialect-agnostic way."""
    dtype_lower = dtype.lower()
    return "array(" in dtype_lower


def is_aggregate_function(dtype: str) -> bool:
    """Determine if a data type is an aggregate bitmap function in a dialect-agnostic way."""
    dtype_lower = dtype.lower()
    # ClickHouse: AggregateFunction(groupBitmap, Int32)
    return any(t in dtype_lower for t in ["aggregatefunction(", "bitmap"])


def is_datetime_dtype(dtype: str) -> bool:
    """Determine if a data type is a date/time type in a dialect-agnostic way."""
    dtype_lower = dtype.lower().strip()
    if is_array_dtype(dtype) or is_aggregate_function(dtype):
        return False
    # N.B. Includes things like Nullable(Datetime64), timestamp with time zone, etc.
    return any(
        time_type in dtype_lower
        for time_type in [
            # General SQL
            "date",
            "datetime",
            "timestamp",
            "time",
            "year",
            # Variants across dialects
            "datetime2",  # SQL Server
            "smalldatetime",  # SQL Server
            "datetimeoffset",  # SQL Server
            "timestamptz",  # PostgreSQL
            "timetz",  # PostgreSQL
            "timestamp with time zone",  # PostgreSQL
            "timestamp without time zone",  # PostgreSQL
            # Oracle
            "interval",
            # BigQuery / Hive / Spark
            "datetime64",
            "timestamp_ntz",
            "timestamp_ltz",
            "timestamp_tz",
            # Snowflake / Redshift specific
            "date_ntz",
            "date_ltz",
            "date_tz",
        ]
    )


def is_numeric_dtype(dtype: str) -> bool:
    """Determine if a data type is a numeric type in a dialect-agnostic way."""
    dtype_lower = dtype.lower()
    if is_array_dtype(dtype) or is_aggregate_function(dtype):
        return False
    # N.B. Includes Nullable(Int32), Numeric(10,2), etc.
    return any(
        num_type in dtype_lower
        for num_type in [
            # Integers
            "int",
            "integer",
            "smallint",
            "bigint",
            "tinyint",
            "mediumint",
            # Exact numeric
            "numeric",
            "decimal",
            "number",
            # Approximate numeric
            "float",
            "double",
            "real",
            "double precision",
            # Special cases in some dialects
            "money",
            "smallmoney",
        ]
    )


def is_low_cardinality_dtype(dtype: str) -> bool:
    """Determine if a data type is inherently low cardinality in a dialect-agnostic way."""
    dtype_lower = dtype.lower()
    if is_array_dtype(dtype) or is_aggregate_function(dtype):
        return False
    # Include Uint8?
    return any(enum_type in dtype_lower for enum_type in ["enum", "lowcardinality"])


def is_string_dtype(dtype: str) -> bool:
    """Determine if a data type is a string type in a dialect-agnostic way."""
    dtype_lower = dtype.lower()
    if is_array_dtype(dtype) or is_aggregate_function(dtype):
        return False
    return any(
        string_type in dtype_lower
        for string_type in [
            "string",
            "varchar",
            "char",
            "nvarchar",
            "nchar",
            "text",
            "longtext",
            "mediumtext",
            "tinytext",
            "ntext",
            "clob",
            "nclob",
            "fixedstring",
            "lowcardinality(string)",
            "lowcardinality(fixedstring)",
            "character varying",
            "character",
        ]
    )
