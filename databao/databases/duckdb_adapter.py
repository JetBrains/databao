from _duckdb import DuckDBPyConnection
from databao_context_engine import DatasourceType

from databao.databases.database_adapter import DatabaseAdapter
from databao.databases.database_connection import DBConnection, DBConnectionConfig, DBConnectionRuntime

DUCKDB_TYPE = DatasourceType(full_type="duckdb")

DATABASE_KEY = "database_path"

MAIN_KEYS = {DATABASE_KEY}


class DuckDBAdapter(DatabaseAdapter):
    @classmethod
    def type(cls) -> DatasourceType:
        return DUCKDB_TYPE

    @classmethod
    def main_property_keys(cls) -> set[str]:
        return MAIN_KEYS

    @classmethod
    def accept(cls, conn: DBConnection) -> bool:
        if isinstance(conn, DuckDBPyConnection):
            return True
        if isinstance(conn, DBConnectionConfig):
            return conn.type == DUCKDB_TYPE  # type: ignore[no-any-return]
        return False

    @classmethod
    def convert_to_config(cls, run_conn: DBConnectionRuntime) -> DBConnectionConfig | None:
        if not isinstance(run_conn, DuckDBPyConnection):
            return None

        database = cls._get_database(run_conn)
        if database is None:
            raise RuntimeError("Memory-based DuckDB is not supported.")

        run_conn.close()
        return DBConnectionConfig(DUCKDB_TYPE, {DATABASE_KEY: database})

    @classmethod
    def register_in_duckdb(cls, shared_conn: DuckDBPyConnection, config: DBConnectionConfig, name: str) -> None:
        database = config.content.get(DATABASE_KEY)
        shared_conn.execute(f"ATTACH '{database}' AS \"{name}\" (READ_ONLY);")

    @staticmethod
    def _get_database(conn: DuckDBPyConnection) -> str | None:
        """Get the database file path for DuckDB connection, or None if in-memory."""
        database = conn.execute("PRAGMA database_list").fetchone()
        if database is None:
            return None
        database = database[2]
        return None if database == "memory" else database
