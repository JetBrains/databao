from typing import Any

import duckdb
from duckdb import DuckDBPyConnection
from langchain_core.language_models.chat_models import BaseChatModel
from pandas import DataFrame
from sqlalchemy import Engine

from portus.configs.llm import LLMConfig
from portus.core import Pipe, Session, Visualizer
from portus.core.executor import AgentExecutor

from ..pipes.lazy import LazyPipe
from ..sources import DatabaseSource, DataFrameSource, DataSource
from ..visualizers.dumb import DumbVisualizer


class InMemSession(Session):
    def __init__(
        self,
        name: str,
        llm_config: LLMConfig,
        *,
        data_executor: AgentExecutor | type[AgentExecutor],
        visualizer: Visualizer | None = None,
        default_rows_limit: int = 1000,
    ):
        self._name = name
        self._llm = llm_config.chat_model
        self._llm_config = llm_config

        self._dbs: dict[str, Any] = {}
        self._dfs: dict[str, DataFrame] = {}
        self._sources: list[DataSource] = []

        # Normalize to an executor type; always create per-pipe instance in ask()
        if isinstance(data_executor, type):
            self._executor_type = data_executor
        else:
            self._executor_type = type(data_executor)
        self._visualizer = visualizer or DumbVisualizer()
        self._default_rows_limit = default_rows_limit

    def add_db(self, connection: Engine, *, name: str | None = None) -> None:
        name = name or f"db_{len(self._sources) + 1}"
        source = DatabaseSource(connection, name)
        self._sources.append(source)
        self._dbs[name] = connection

    def add_df(self, df: DataFrame, *, name: str | None = None) -> None:
        df_name = name or f"df_{len(self._sources) + 1}"
        source = DataFrameSource(df, df_name)
        self._sources.append(source)
        self._dfs[df_name] = df

    def ask(self, query: str) -> Pipe:
        # Create a fresh executor per pipe
        executor = self._executor_type(self._get_connection(), self._llm_config)
        return LazyPipe(self, executor, default_rows_limit=self._default_rows_limit).ask(query)

    def _get_connection(self) -> DuckDBPyConnection:
        con = duckdb.connect(database=":memory:", read_only=False)
        for source in self._sources:
            source.register(con)
        return con

    @property
    def dbs(self) -> dict[str, Any]:
        return dict(self._dbs)

    @property
    def dfs(self) -> dict[str, DataFrame]:
        return dict(self._dfs)

    @property
    def sources(self) -> list[DataSource]:
        return self._sources

    @property
    def name(self) -> str:
        return self._name

    @property
    def llm(self) -> BaseChatModel:
        return self._llm

    # Session no longer exposes executor; visualizer is still provided

    @property
    def visualizer(self) -> "Visualizer":
        return self._visualizer
