from _duckdb import DuckDBPyConnection
from databao_context_engine import DatasourceType
from sqlalchemy import URL, Connection, Engine, make_url

from databao.databases.database_adapter import DatabaseAdapter
from databao.databases.database_connection import DBConnection, DBConnectionConfig, DBConnectionRuntime

SQLITE_TYPE = DatasourceType(full_type="sqlite")

DATABASE_KEY = "database_path"

EXCLUDED_QUERY_KEYS = {DATABASE_KEY}

MAIN_KEYS = {DATABASE_KEY}


class SQLiteAdapter(DatabaseAdapter):
    @classmethod
    def type(cls) -> DatasourceType:
        return SQLITE_TYPE

    @classmethod
    def main_property_keys(cls) -> set[str]:
        return MAIN_KEYS

    @classmethod
    def accept(self, conn: DBConnection) -> bool:
        if isinstance(conn, (Engine, Connection)):
            dialect = conn.dialect
            return dialect.name.startswith("sqlite")
        if isinstance(conn, DBConnectionConfig):
            return conn.type == SQLITE_TYPE  # type: ignore[no-any-return]
        return False

    @classmethod
    def convert_to_config(cls, run_conn: DBConnectionRuntime) -> DBConnectionConfig | None:
        if not isinstance(run_conn, (Engine, Connection)):
            return None

        engine = run_conn if isinstance(run_conn, Engine) else run_conn.engine
        dialect = engine.dialect
        if not dialect.name.startswith("sqlite"):
            return None

        sa_url_str = engine.url.render_as_string(hide_password=False)
        sa_url = make_url(sa_url_str)
        database = sa_url.database
        if database == ":memory:":
            raise RuntimeError("Memory-based SQLite is not supported.")
        content = dict(dialect.create_connect_args(sa_url)[1])
        content[DATABASE_KEY] = database
        content.pop("check_same_thread", None)

        return DBConnectionConfig(SQLITE_TYPE, content)

    @classmethod
    def register_in_duckdb(cls, shared_conn: DuckDBPyConnection, config: DBConnectionConfig, name: str) -> None:
        database = config.content.get(DATABASE_KEY)
        shared_conn.execute(f"ATTACH '{database}' AS \"{name}\" (TYPE SQLITE);")

    @staticmethod
    def _create_url(conn: DBConnectionConfig) -> str:
        content = conn.content

        url = URL.create(
            drivername="sqlite",
            database=content.get(DATABASE_KEY),
            query={k: v for k, v in content.items() if k not in EXCLUDED_QUERY_KEYS},
        )

        return url.render_as_string(hide_password=False)
