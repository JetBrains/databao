from abc import ABC
from pathlib import Path
from typing import Self

from pydantic import BaseModel, ConfigDict, Field

from portus.utils import read_config_file


class DataSourceConfig(BaseModel, ABC):
    """
    Base class for the data engine configs. The data engine configs contain login and connection
    information as well as the type of the config class to be loaded. The reason to have multiple types of
    configs instead of one large config is that thus it is more legible to organize the relations between
    the attributes - e.g., one dbms may require an api_key and another a username-password pair, and to assert
    that the correct type has the correct connection attributes we would have to write a large validation functionality,
    which is easily avoidable by just subclassing.
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(
        description="""
    Human-readable name of the data source, which bears some meaning about that datasource.
    Should be consistent with the naming used within the datasource configs as it is used as
    a key (in conjunction with the per-example list of database names, if provided) to retrieve 
    the correct DataEngine, which can connect to that database as credential information is 
    provided during experiment setup.
    Must be unique within the experiment.
    NB. here the assumption is that one account can access all databases referenced in the example.
    """,
        examples=["jetstat", "jetstat_metabase", "bigquery-spider", "ga4"],
    )

    source_type: str | None = Field(
        default=None,
        description="String literal from which we determine which "
        "class to instantiate to create the DataSource object: if "
        "`sqlalchemy`-> SqlAlchemyDataSource, if `metabase` -> "
        "MetabaseSourceProvider. If provided alongside source_class_import_path or "
        "source_class_config_import_path it is ignored when building the respective "
        "object.",
        examples=["sqlalchemy", "metabase"],
    )

    source_class_import_path: str | None = Field(
        default=None,
        description="Import path to the DataSource sub-class which "
        "to dynamically import as plugin. If provided, source_type is ignored for "
        "building the DataSource object (but may be still used in building the "
        "DataSourceConfig object).",
    )

    source_class_config_import_path: str | None = Field(
        default=None,
        description="Import path to the DataSourceConfig sub-class which "
        "to dynamically import as plugin. If provided, source_type is ignored for "
        "building the DataSourceConfig object (but may be still used in building the "
        "DataSource object).",
    )

    database_or_schema: str | list[str] | None = None
    """
    The database or the schema to use for queries. 
    Can be a single database / schema / table (database object) or a list of related database objects.
    Thus we allow maximum flexibility to DataSources - a datasource can inspect multiple tables and combine
    the extracted profiling information; or a datasource can query these objects via complex
    queries involving joins, set operations, etc.  

    For some dialects (ClickHouse), this sets the default schema. 
    For others (PostgreSQL), it sets the default database, in which case you need to specify the default schema using `db_options`. 

    Can be None at experiment startup, but some solvers may raise RuntimeErrors if not set at latest at a certain point.
    
    For compatibility reasons, if the value is of type str, then we add it to the database url. Else, however, as there 
    are multiple databases / schemas which the datasource can connect to, we expect that 1. the names of the databases 
    are used within the queries of the dataset (e.g. select * from my_database.my_schema.my_table) and 2. that the agent 
    also correctly adds these names in its queries. 
    
    N.B. metricflow SQL queries will work regardless of which default schema is used because the 
    generated SQL contains fully qualified tables
    """

    @classmethod
    def from_file(cls, path: Path) -> Self:
        d = read_config_file(path, parse_env_vars=True)
        return cls.model_validate(d)
