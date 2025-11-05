from typing import Literal

from pydantic import Field, SecretStr
from sqlalchemy import URL, make_url

from databao.data.configs.data_source_config import DataSourceConfig


class SqlAlchemyDataSourceConfig(DataSourceConfig):
    source_type: Literal["sqlalchemy"] = "sqlalchemy"
    """The type of the data source."""

    db_type: str = Field(
        description="Type of the database management system which is going to be queried by "
        "the agent or at profiling time. If a type is used, which is not among the explicitly "
        "checked types in the create_db_engine function, a warning will be raised.",
        examples=["clickhouse", "ms_sqlserver", "bigquery"],
    )

    ###############################
    # Sqlalchemy query properties #
    ###############################
    limit_max_rows: int | None = 10000
    """Limit how many rows can be returned by the database for all queries. If None, all rows are returned."""
    query_timeout: int | None = 120
    """Database query result timeout in seconds. None means the DB default timeout (usually no timeout).

    A database can start sending results immediately, but the data transfer can take more time than the timeout.
    In that case, no timeout exception will be raised. E.g. `select * from customers` takes >10s with no 
    exception even with a 1s timeout. You can check that the timeout actually works with `select sleep(3)`
    """

    max_concurrent_requests: int = 8
    """Maximum number of concurrent requests to the database. Only applicable to async based execution."""

    ###########################
    # url building properties #
    ###########################
    url: SecretStr | None = None
    """URL to connect to the database. 
    If None, the driver, user, password, host, port, schema, and db_options will be used to construct the URL."""

    driver: SecretStr | None = None
    user: SecretStr | None = None
    password: SecretStr | None = None
    host: SecretStr | None = None
    port: int | None = None

    db_options: SecretStr | None = None
    """Options to add to the url, without the initial question mark (?)."""

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
            database=self.database_or_schema
            if self.database_or_schema is not None and not isinstance(self.database_or_schema, list)
            else None,
        )
        if self.db_options is not None:
            url = url.update_query_string(self.db_options.get_secret_value())
        return url
