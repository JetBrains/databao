from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb
from langchain_core.language_models.chat_models import BaseChatModel
from pandas import DataFrame
from sqlalchemy import Engine

from databao.configs.llm import LLMConfig
from databao.core.pipe import Pipe

if TYPE_CHECKING:
    from databao.core.cache import Cache
    from databao.core.executor import Executor
    from databao.core.visualizer import Visualizer


class Session:
    """A session manages all databases and Dataframes as well as the context for them.
    Session determines what LLM to use, what executor to use and how to visualize data for all threads.
    Several threads can be spawned out of the session.
    """

    def __init__(
        self,
        name: str,
        llm: LLMConfig,
        data_executor: "Executor",
        visualizer: "Visualizer",
        cache: "Cache",
        default_rows_limit: int,
    ):
        self.__name = name
        self.__llm = llm.chat_model
        self.__llm_config = llm

        self.__dbs: dict[str, Any] = {}
        self.__dfs: dict[str, DataFrame] = {}

        self.__db_context: dict[str, str] = {}
        self.__df_context: dict[str, str] = {}
        self.__additional_context: list[str] = []

        # Create a DuckDB connection for the session
        self.__duckdb_connection = duckdb.connect(":memory:")

        self.__executor = data_executor
        self.__visualizer = visualizer
        self.__cache = cache
        self.__default_rows_limit = default_rows_limit

    def _parse_context_arg(self, context: str | Path | None) -> str | None:
        if context is None:
            return None
        if isinstance(context, Path):
            return context.read_text()
        return context

    def add_db(self, connection: Any, *, name: str | None = None, context: str | Path | None = None) -> None:
        """
        Add a database connection to the internal collection and optionally associate it
        with a specific context for query execution. Supports integration with SQLAlchemy
        engines and direct DuckDB connections.

        Args:
            connection (Any): The database connection to be added. Can be a SQLAlchemy
                engine or a native DuckDB connection.
            name (str | None): Optional name to assign to the database connection. If
                not provided, a default name such as 'db1', 'db2', etc., will be
                generated dynamically based on the collection size.
            context (str | Path | None): Optional context for the database connection. It can
                be either the path to a file whose content will be used as the context or
                the direct context as a string.
        """
        from databao.duckdb import register_sqlalchemy

        conn_name = name or f"db{len(self.__dbs) + 1}"

        # If it's a SQLAlchemy engine, register it with our DuckDB connection
        if isinstance(connection, Engine):
            register_sqlalchemy(self.__duckdb_connection, connection, conn_name)
            # Store the DuckDB connection in dbs if not already there
            if "duckdb" not in self.__dbs:
                self.__dbs["duckdb"] = self.__duckdb_connection
        else:
            # For other connection types (like native DuckDB), store directly
            self.__dbs[conn_name] = connection

        if (context_text := self._parse_context_arg(context)) is not None:
            self.__db_context[conn_name] = context_text

    def add_df(self, df: DataFrame, *, name: str | None = None, context: str | Path | None = None) -> None:
        """Register a DataFrame in this session and in the session's DuckDB.

        Args:
            df: DataFrame to expose to agents/executors/SQL.
            name: Optional name; defaults to df1/df2/...
            context: Optional text or path to a file describing this dataset for the LLM.
        """
        df_name = name or f"df{len(self.__dfs) + 1}"
        self.__dfs[df_name] = df

        # Register the DataFrame with DuckDB
        self.__duckdb_connection.register(df_name, df)

        # Store the DuckDB connection in dbs if not already there
        if "duckdb" not in self.__dbs:
            self.__dbs["duckdb"] = self.__duckdb_connection

        if (context_text := self._parse_context_arg(context)) is not None:
            self.__df_context[df_name] = context_text

    def add_context(self, context: str | Path) -> None:
        """Add additional context to help models understand your data.

        Use this method to add general information that might not be associated with a specific data source.
        If the information is specific to a data source, use the `context` argument of `add_db` and `add_df`.

        Args:
            context: The string or the path to a file containing the additional context.
        """
        text = self._parse_context_arg(context)
        if text is None:
            raise ValueError("Invalid context provided.")
        self.__additional_context.append(text)

    def thread(self) -> Pipe:
        """Start a new thread in this session."""
        return Pipe(self, default_rows_limit=self.__default_rows_limit)

    @property
    def dbs(self) -> dict[str, Any]:
        return dict(self.__dbs)

    @property
    def dfs(self) -> dict[str, DataFrame]:
        return dict(self.__dfs)

    @property
    def name(self) -> str:
        return self.__name

    @property
    def llm(self) -> BaseChatModel:
        return self.__llm

    @property
    def llm_config(self) -> LLMConfig:
        return self.__llm_config

    @property
    def executor(self) -> "Executor":
        return self.__executor

    @property
    def visualizer(self) -> "Visualizer":
        return self.__visualizer

    @property
    def cache(self) -> "Cache":
        return self.__cache

    @property
    def db_context(self) -> dict[str, str]:
        """Per-source natural-language context for DBs."""
        return self.__db_context

    @property
    def df_context(self) -> dict[str, str]:
        """Per-source natural-language context for DFs."""
        return self.__df_context

    @property
    def additional_context(self) -> list[str]:
        """General additional context not specific to any one data source."""
        return self.__additional_context
