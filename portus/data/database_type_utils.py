import re

_ID_PREFIXES_AND_SUFFIXES = [
    # suffixes
    r"_id\b",
    r"_uuid\b",
    r"_hash\b",
    r"_doi\b",
    r"_key\b",
    r"_pk\b",
    r"_fk\b",
    r"_code\b",
    # prefixes
    r"\bid_",
    r"\buuid_",
    r"\bhash_",
    r"\bdoi_",
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
    if col_name.lower() in ["id", "key", "pk", "fk", "code", "uuid", "hash", "doi"]:
        return True
    return _ID_COL_REGEX.search(col_name) is not None


def is_array_dtype(dtype: str) -> bool:
    """Determine if a data type is an array type in a dialect-agnostic way."""
    return any(
        t.lower() in dtype.lower()
        for t in [
            "ARRAY",  # BigQuery, PostgreSQL, Snowflake, Hive, Spark, DuckDB, Databricks, Presto, Trino, CockroachDB
            "VARRAY",  # Oracle (Variable-size array)
            "TABLE TYPE",  # Oracle, SQL Server
            "NESTED TABLE",  # Oracle
            "List",  # ClickHouse
            "SUPER",  # Redshift (supports arrays in semi-structured data)
            "JSON",  # MySQL, MariaDB (arrays stored as JSON)
            "MULTISET",  # Teradata
            "ARRAY TYPE",  # DB2
            "REPEATED",  # BigQuery (legacy/alternative syntax for arrays)
        ]
    )


def is_nested_dtype(dtype: str) -> bool:
    """
    Determine if a data type is a struct type in a dialect-agnostic way.
    """
    return any(
        t in dtype
        for t in [
            "STRUCT",  # BigQuery, Hive, Spark, DuckDB, Databricks
            "COMPOSITE TYPE",  # PostgreSQL, CockroachDB
            "OBJECT TYPE",  # Oracle
            "User-Defined Table Types",  # SQL Server
            "OBJECT",  # Snowflake
            "SUPER",  # Redshift
            "ROW",  # Presto, Trino, Vertica
            "Tuple",  # ClickHouse
            "Nested",  # ClickHouse
            "MAP",  # Apache Drill
            "STRUCTURED UDT",  # Teradata
            "STRUCTURED TYPE",  # DB2
        ]
    )


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
    return bool(any(enum_type in dtype_lower for enum_type in ["enum", "lowcardinality"]))


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
