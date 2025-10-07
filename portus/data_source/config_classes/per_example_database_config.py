from dataclasses import dataclass


@dataclass(kw_only=True)
class DatabaseGroupConfig:
    """
    A DatabaseGroup is a logical grouping of databases such that queries can be executed
    which contain references to combinations of any members of the group - e.g., joins, unions, etc.
    """

    name: str
    """
    Human-readable name of the data source, which bears some meaning about that datasource.
    Should be consistent with the naming used within the datasource configs as it is used as
    a key to retrieve the correct DataEngine, which can connect to that database as credential
    information is provided during experiment setup.
    NB. here the assumption is that one account can access all databases referenced in the example.
    """
    database_object_names: list[str]
    """
    An example can be related to multiple database objects within the same dbms - which translates
    to creating multiple data sources within the same data engine.
    """


@dataclass(kw_only=True)
class PerExampleDatabaseConfig:
    database_groups: list[DatabaseGroupConfig]
    """
    A list of dataset groups. Thus we allow maximum flexibility on how to create per-example data sources - 
    
    - if one only wants to restrict the DataSource to use a particular subset of tables / schemas among a larger
        collection (e.g. a BigQuery project) - then one can create a group, setting the database_object_names to 
        the names of these tables / schemas. 
        
    - If, on the other hand, one wants (for some reason, e.g. in like in the metabase project) to only independently
        query the selected tables / schemas, then one can define them as separate group objects, thus forcing the 
        creating of separate DataSources per group.
    """
