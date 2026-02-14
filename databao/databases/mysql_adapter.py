from _duckdb import DuckDBPyConnection
from databao_context_engine import DatasourceType
from sqlalchemy import URL, Connection, Engine, make_url

from databao.databases.database_adapter import DatabaseAdapter
from databao.databases.database_connection import DBConnection, DBConnectionConfig, DBConnectionRuntime

MYSQL_TYPE = DatasourceType(full_type="mysql")

USER_KEY = "user"
PASSWORD_KEY = "password"
HOST_KEY = "host"
PORT_KEY = "port"
DATABASE_KEY = "database"

MAIN_KEYS = {USER_KEY, PASSWORD_KEY, HOST_KEY, PORT_KEY, DATABASE_KEY}


class MySQLAdapter(DatabaseAdapter):
    @classmethod
    def type(cls) -> DatasourceType:
        return MYSQL_TYPE

    @classmethod
    def main_property_keys(cls) -> set[str]:
        return MAIN_KEYS

    @classmethod
    def accept(cls, conn: DBConnection) -> bool:
        if isinstance(conn, (Engine, Connection)):
            dialect = conn.dialect
            return dialect.name.startswith(("mysql", "mariadb"))
        if isinstance(conn, DBConnectionConfig):
            return conn.type == MYSQL_TYPE  # type: ignore[no-any-return]
        return False

    @classmethod
    def convert_to_config(cls, run_conn: DBConnectionRuntime) -> DBConnectionConfig | None:
        if not isinstance(run_conn, (Engine, Connection)):
            return None

        engine = run_conn if isinstance(run_conn, Engine) else run_conn.engine
        dialect = engine.dialect
        if not dialect.name.startswith(("mysql", "mariadb")):
            return None

        sa_url_str = engine.url.render_as_string(hide_password=False)
        sa_url = make_url(sa_url_str)
        content = dict(dialect.create_connect_args(sa_url)[1])
        if "dbname" in content and "database" not in content:
            content["database"] = content.pop("dbname")
        content.pop("client_flag", None)

        return DBConnectionConfig(type=MYSQL_TYPE, content=content)

    @classmethod
    def register_in_duckdb(cls, shared_conn: DuckDBPyConnection, config: DBConnectionConfig, name: str) -> None:
        url = cls._create_url(config)
        shared_conn.execute("INSTALL mysql;")
        shared_conn.execute("LOAD mysql;")
        shared_conn.execute(f"ATTACH '{url}' AS \"{name}\" (TYPE MYSQL);")

    @staticmethod
    def _create_url(conn: DBConnectionConfig) -> str:
        content = conn.content

        url = URL.create(
            drivername="mysql",
            username=content.get(USER_KEY),
            password=content.get(PASSWORD_KEY),
            host=content.get(HOST_KEY),
            port=content.get(PORT_KEY),
            database=content.get(DATABASE_KEY),
            query={k: v for k, v in content.items() if k not in MAIN_KEYS},
        )

        return url.render_as_string(hide_password=False)
