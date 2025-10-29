from pathlib import Path
from typing import TYPE_CHECKING, Any

from langchain_core.language_models.chat_models import BaseChatModel
from pandas import DataFrame

from portus.configs.llm import LLMConfig
from portus.core import DataEngine
from portus.core.pipe import Pipe
from portus.data.duckdb.duckdb_collection import DuckDBCollection

if TYPE_CHECKING:
    from portus.core.cache import Cache
    from portus.core.executor import Executor
    from portus.core.visualizer import Visualizer


class Session:
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

        self.__duckdb_collection = DuckDBCollection()
        self.__data_engine = DataEngine([self.__duckdb_collection])

        self.__executor = data_executor
        self.__visualizer = visualizer
        self.__cache = cache
        self.__default_rows_limit = default_rows_limit

    def add_db(self, connection: Any, *, name: str | None = None, additional_context: str | None = None) -> None:
        if additional_context is not None and Path(additional_context).is_file():
            additional_context = Path(additional_context).read_text()
        self.__duckdb_collection.add_db(connection, name=name, additional_context=additional_context)

    def add_df(self, df: DataFrame, *, name: str | None = None, additional_context: str | None = None) -> None:
        if additional_context is not None and Path(additional_context).is_file():
            additional_context = Path(additional_context).read_text()
        self.__duckdb_collection.add_df(df, name=name, additional_context=additional_context)

    def thread(self) -> Pipe:
        """Start a new thread in this session."""
        self.__duckdb_collection.register_data_sources()
        return Pipe(self, default_rows_limit=self.__default_rows_limit)

    @property
    def dbs(self) -> dict[str, Any]:
        dbs_: dict[str, Any] = {s.name: s.engine for s in self.__duckdb_collection.db_sources}
        # Temporary workaround for AgentExecutor
        dbs_["native_duckdb"] = self.__duckdb_collection.make_duckdb_connection()
        return dbs_

    @property
    def dfs(self) -> dict[str, DataFrame]:
        return {s.name: s.df for s in self.__duckdb_collection.df_sources}

    @property
    def data_engine(self) -> DataEngine:
        return self.__data_engine

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
    def context(self) -> tuple[dict[str, str], dict[str, str]]:
        db_contexts = {
            s.name: s.additional_context
            for s in self.__duckdb_collection.db_sources
            if s.additional_context is not None
        }
        df_contexts = {
            s.name: s.additional_context
            for s in self.__duckdb_collection.df_sources
            if s.additional_context is not None
        }
        return db_contexts, df_contexts
