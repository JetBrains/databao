from pathlib import Path
from typing import TYPE_CHECKING

from duckdb import DuckDBPyConnection
from langchain_core.language_models.chat_models import BaseChatModel
from pandas import DataFrame
from sqlalchemy import Connection, Engine

from databao.core.data_source import DBDataSource, DFDataSource, Sources
from databao.core.thread import Thread

if TYPE_CHECKING:
    from databao.configs.agent import AgentConfig
    from databao.configs.llm import LLMConfig
    from databao.core.cache import Cache
    from databao.core.executor import Executor
    from databao.core.visualizer import Visualizer


class Agent:
    """An agent manages all databases and Dataframes as well as the context for them.
    Agent determines what LLM to use, what executor to use and how to visualize data for all threads.
    Several threads can be spawned out of the agent.
    """

    def __init__(
        self,
        llm: "LLMConfig",
        agent_config: "AgentConfig",
        data_executor: "Executor",
        visualizer: "Visualizer",
        cache: "Cache",
        *,
        name: str = "default_agent",
        rows_limit: int,
        stream_ask: bool = True,
        stream_plot: bool = False,
        lazy_threads: bool = False,
        auto_output_modality: bool = True,
    ):
        self.__name = name
        self.__llm = llm.new_chat_model()
        self.__llm_config = llm
        self.__agent_config = agent_config

        self.__sources: Sources = Sources(dfs={}, dbs={}, additional_context=[])

        self.__executor = data_executor
        self.__visualizer = visualizer
        self.__cache = cache

        # Thread defaults
        self.__rows_limit = rows_limit
        self.__lazy_threads = lazy_threads
        self.__auto_output_modality = auto_output_modality
        self.__stream_ask = stream_ask
        self.__stream_plot = stream_plot

    def _parse_context_arg(self, context: str | Path | None) -> str | None:
        if context is None:
            return None
        if isinstance(context, Path):
            return context.read_text()
        return context

    def add_db(
        self,
        connection: DuckDBPyConnection | Engine | Connection,
        *,
        name: str | None = None,
        context: str | Path | None = None,
    ) -> None:
        """
        Add a database connection to the internal collection and optionally associate it
        with a specific context for query execution. Supports integration with SQLAlchemy
        engines and direct DuckDB connections.

        Args:
            connection (DuckDBPyConnection | Engine | Connection): The database connection to be added.
                Can be an SQLAlchemy engine or connection or a native DuckDB connection.
            name (str | None): Optional name to assign to the database connection. If
                not provided, a default name such as 'db1', 'db2', etc., will be
                generated dynamically based on the collection size.
            context (str | Path | None): Optional context for the database connection. It can
                be either the path to a file whose content will be used as the context or
                the direct context as a string.
        """
        if not isinstance(connection, (DuckDBPyConnection, Engine, Connection)):
            raise ValueError("Connection must be a DuckDB connection or SQLAlchemy engine.")

        conn_name = name or f"db{len(self.__sources.dbs) + 1}"

        context_text = self._parse_context_arg(context) or ""

        source = DBDataSource(name=conn_name, context=context_text, db_connection=connection)
        self.__sources.dbs[conn_name] = source
        self.executor.register_db(source)

    def add_df(self, df: DataFrame, *, name: str | None = None, context: str | Path | None = None) -> None:
        """Register a DataFrame in this agent and in the agent's DuckDB.

        Args:
            df: DataFrame to expose to executors/executors/SQL.
            name: Optional name; defaults to df1/df2/...
            context: Optional text or path to a file describing this dataset for the LLM.
        """
        df_name = name or f"df{len(self.__sources.dfs) + 1}"

        context_text = self._parse_context_arg(context) or ""

        source = DFDataSource(name=df_name, context=context_text, df=df)
        self.__sources.dfs[df_name] = source

        self.executor.register_df(source)

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
        self.__sources.additional_context.append(text)

    def thread(
        self,
        *,
        stream_ask: bool | None = None,
        stream_plot: bool | None = None,
        lazy: bool | None = None,
        auto_output_modality: bool | None = None,
    ) -> Thread:
        """Start a new thread in this agent."""
        if not self.__sources.dbs and not self.__sources.dfs:
            raise ValueError("No databases or dataframes registered in this agent.")
        return Thread(
            self,
            rows_limit=self.__rows_limit,
            stream_ask=stream_ask if stream_ask is not None else self.__stream_ask,
            stream_plot=stream_plot if stream_plot is not None else self.__stream_plot,
            lazy=lazy if lazy is not None else self.__lazy_threads,
            auto_output_modality=auto_output_modality
            if auto_output_modality is not None
            else self.__auto_output_modality,
        )

    @property
    def sources(self) -> Sources:
        return self.__sources

    @property
    def dbs(self) -> dict[str, DBDataSource]:
        return dict(self.__sources.dbs)

    @property
    def dfs(self) -> dict[str, DFDataSource]:
        return dict(self.__sources.dfs)

    @property
    def name(self) -> str:
        return self.__name

    @property
    def llm(self) -> BaseChatModel:
        return self.__llm

    @property
    def llm_config(self) -> "LLMConfig":
        return self.__llm_config

    @property
    def agent_config(self) -> "AgentConfig":
        return self.__agent_config

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
    def additional_context(self) -> list[str]:
        """General additional context not specific to any one data source."""
        return self.__sources.additional_context
