"""Connection factory using DCE's datasource discovery and plugin system.

This module reuses DCE (databao-context-engine) for:
- Discovering datasources via `discover_datasources()`
- Preparing configs via `prepare_source()`
- Loading plugins via `load_plugins()`
- Creating DuckDB connections via the introspector's `_connect()`

For non-DuckDB databases, we extract connection params from the DCE-parsed
config and create SQLAlchemy engines (since DCE uses different drivers
like asyncpg/pymysql that aren't compatible with Databao).
"""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from duckdb import DuckDBPyConnection
from sqlalchemy import Engine, create_engine

logger = logging.getLogger(__name__)


@dataclass
class ConnectionInfo:
    """Information about a database connection."""

    name: str
    db_type: str
    connection: DuckDBPyConnection | Engine
    config_path: Path


def _create_sqlalchemy_engine_from_config(db_type: str, parsed_config: Any) -> Engine | None:
    """Create a SQLAlchemy engine from a DCE-parsed config.

    This is needed because DCE uses different drivers (asyncpg, pymysql)
    that aren't compatible with Databao's SQLAlchemy-based architecture.
    """
    try:
        conn_config = parsed_config.connection

        if db_type in ("postgres", "postgresql"):
            # PostgresConnectionProperties
            host = getattr(conn_config, "host", "localhost")
            port = getattr(conn_config, "port", 5432) or 5432
            database = getattr(conn_config, "database", "postgres") or "postgres"
            user = getattr(conn_config, "user", None)
            password = getattr(conn_config, "password", None)

            if user and password:
                url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
            elif user:
                url = f"postgresql://{user}@{host}:{port}/{database}"
            else:
                url = f"postgresql://{host}:{port}/{database}"

            return create_engine(url)

        elif db_type == "mysql":
            # MySQL config - may be dict or object
            if isinstance(conn_config, dict):
                host = conn_config.get("host", "localhost")
                port = conn_config.get("port", 3306)
                database = conn_config.get("database", "")
                user = conn_config.get("user", "")
                password = conn_config.get("password", "")
            else:
                host = getattr(conn_config, "host", "localhost")
                port = getattr(conn_config, "port", 3306) or 3306
                database = getattr(conn_config, "database", "")
                user = getattr(conn_config, "user", "")
                password = getattr(conn_config, "password", "")

            if user and password:
                url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
            elif user:
                url = f"mysql+pymysql://{user}@{host}:{port}/{database}"
            else:
                url = f"mysql+pymysql://{host}:{port}/{database}"

            return create_engine(url)

        elif db_type == "clickhouse":
            if isinstance(conn_config, dict):
                host = conn_config.get("host", "localhost")
                port = conn_config.get("port", 8123)
                database = conn_config.get("database", "default")
                user = conn_config.get("user", "default")
                password = conn_config.get("password", "")
            else:
                host = getattr(conn_config, "host", "localhost")
                port = getattr(conn_config, "port", 8123) or 8123
                database = getattr(conn_config, "database", "default")
                user = getattr(conn_config, "user", "default")
                password = getattr(conn_config, "password", "")

            if password:
                url = f"clickhouse+native://{user}:{password}@{host}:{port}/{database}"
            else:
                url = f"clickhouse+native://{user}@{host}:{port}/{database}"

            return create_engine(url)

        elif db_type == "mssql":
            if isinstance(conn_config, dict):
                host = conn_config.get("host", "localhost")
                port = conn_config.get("port", 1433)
                database = conn_config.get("database", "")
                user = conn_config.get("user", "")
                password = conn_config.get("password", "")
            else:
                host = getattr(conn_config, "host", "localhost")
                port = getattr(conn_config, "port", 1433) or 1433
                database = getattr(conn_config, "database", "")
                user = getattr(conn_config, "user", "")
                password = getattr(conn_config, "password", "")

            if user and password:
                url = f"mssql+pymssql://{user}:{password}@{host}:{port}/{database}"
            elif user:
                url = f"mssql+pymssql://{user}@{host}:{port}/{database}"
            else:
                url = f"mssql+pymssql://{host}:{port}/{database}"

            return create_engine(url)

        else:
            logger.warning(f"Unsupported database type for SQLAlchemy: {db_type}")
            return None

    except Exception as e:
        logger.warning(f"Failed to create SQLAlchemy engine for {db_type}: {e}")
        return None


def create_all_connections(project_path: Path) -> list[ConnectionInfo]:
    """Create connections for all datasources in a DCE project.

    Uses DCE's discovery and plugin system to find and parse datasources,
    then creates Databao-compatible connections.
    """
    connections: list[ConnectionInfo] = []

    try:
        # Import DCE modules (paths changed in DCE 0.1.4)
        from databao_context_engine.datasources.datasource_discovery import discover_datasources, prepare_source
        from databao_context_engine.datasources.types import PreparedConfig
        from databao_context_engine.pluginlib.plugin_utils import _validate_datasource_config_file
        from databao_context_engine.plugins.plugin_loader import load_plugins

        # Discover all datasources using DCE
        datasources = discover_datasources(project_path)
        plugins = load_plugins(exclude_file_plugins=True)

        for datasource in datasources:
            try:
                # Prepare the datasource using DCE
                prepared = prepare_source(datasource)

                # Only handle configs (not files)
                if not isinstance(prepared, PreparedConfig):
                    continue

                # Get the plugin for this datasource type
                plugin = plugins.get(prepared.datasource_type)
                if plugin is None:
                    logger.warning(f"No plugin for {prepared.datasource_type.full_type}")
                    continue

                # Parse config using DCE's plugin validation
                parsed_config = _validate_datasource_config_file(prepared.config, plugin)

                # Extract database type
                db_type = prepared.datasource_type.subtype
                name = parsed_config.name or prepared.datasource_name

                # Create connection based on type
                connection: DuckDBPyConnection | Engine | None = None

                if db_type == "duckdb":
                    # DuckDB: create connection directly with read_only=True to avoid file lock conflicts
                    # (DCE's introspector uses read_write mode which can conflict with other processes)
                    import duckdb

                    db_path = str(parsed_config.connection.database)
                    # Resolve relative paths from the project root (not the config file location)
                    if not Path(db_path).is_absolute():
                        db_path = str((project_path / db_path).resolve())
                    connection = duckdb.connect(db_path, read_only=True)
                else:
                    # Other databases: create SQLAlchemy engine from parsed config
                    connection = _create_sqlalchemy_engine_from_config(db_type, parsed_config)

                if connection is not None:
                    connections.append(
                        ConnectionInfo(
                            name=name,
                            db_type=db_type,
                            connection=connection,
                            config_path=prepared.path,
                        )
                    )
                    logger.info(f"Created {db_type} connection '{name}' from {prepared.path}")

            except Exception as e:
                logger.warning(f"Failed to create connection for {datasource.path}: {e}")
                continue

    except ImportError:
        logger.warning("databao-context-engine (DCE) not available - no connections created")
    except ValueError as e:
        # No src directory or invalid project
        logger.warning(f"DCE project issue: {e}")
    except Exception as e:
        logger.exception(f"Unexpected error discovering datasources: {e}")

    return connections
