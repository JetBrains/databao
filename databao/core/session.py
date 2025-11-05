import pathlib
from typing import TYPE_CHECKING, Any

from langchain_core.language_models.chat_models import BaseChatModel
from pandas import DataFrame

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

        self.__db_contexts: dict[str, str] = {}
        self.__df_contexts: dict[str, str] = {}

        self.__executor = data_executor
        self.__visualizer = visualizer
        self.__cache = cache
        self.__default_rows_limit = default_rows_limit

    def add_db(self, connection: Any, *, name: str | None = None, context: str | None = None) -> None:
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
            context (str | None): Optional context for the database connection. It can
                be either the path to a file whose content will be used as the context or
                the direct context as a string.
        """
        conn_name = name or f"db{len(self.__dbs) + 1}"
        self.__dbs[conn_name] = connection

        if context:
            if pathlib.Path(context).is_file():
                context = pathlib.Path(context).read_text()
            self.__db_contexts[conn_name] = context

    def add_df(self, df: DataFrame, *, name: str | None = None, context: str | None = None) -> None:
        """Register a DataFrame in this session and in the session's DuckDB.

        Args:
            df: DataFrame to expose to agents/executors/SQL.
            name: Optional name; defaults to df1/df2/...
            context: Optional text or path to a file describing this dataset for the LLM.
        """
        df_name = name or f"df{len(self.__dfs) + 1}"
        self.__dfs[df_name] = df

        if context:
            if pathlib.Path(context).is_file():
                context = pathlib.Path(context).read_text()
            self.__df_contexts[df_name] = context

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
    def db_contexts(self) -> dict[str, str]:
        """Per-source natural-language context for DBs."""
        return self.__db_contexts

    @property
    def df_contexts(self) -> dict[str, str]:
        """Per-source natural-language context for DFs."""
        return self.__df_contexts
