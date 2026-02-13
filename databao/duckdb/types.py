from collections.abc import Callable

import duckdb

DbConnFactory = Callable[[], duckdb.DuckDBPyConnection]


def make_duckdb_factory(db_path: str) -> DbConnFactory:
    """Return a factory that creates short-lived connections to the given DB."""

    def factory() -> duckdb.DuckDBPyConnection:
        return duckdb.connect(db_path)

    return factory
