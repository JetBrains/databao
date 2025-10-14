from typing import TYPE_CHECKING, Any

from langchain_core.language_models.chat_models import BaseChatModel
from pandas import DataFrame

from ..pipes.lazy import LazyPipe
from .pipe import Pipe

if TYPE_CHECKING:
    from .cache import Cache
    from .executor import Executor
    from .visualizer import Visualizer


class Session:
    def __init__(
        self,
        name: str,
        llm: BaseChatModel,
        data_executor: "Executor",
        visualizer: "Visualizer",
        cache: "Cache",
        default_rows_limit: int,
    ):
        self.__name = name
        self.__llm = llm

        self.__dbs: dict[str, Any] = {}
        self.__dfs: dict[str, DataFrame] = {}

        self.__executor = data_executor
        self.__visualizer = visualizer
        self.__cache = cache
        self.__default_rows_limit = default_rows_limit

    def add_db(self, connection: Any, *, name: str | None = None) -> None:
        conn_name = name or f"db{len(self.__dbs) + 1}"
        self.__dbs[conn_name] = connection

    def add_df(self, df: DataFrame, *, name: str | None = None) -> None:
        df_name = name or f"df{len(self.__dfs) + 1}"
        self.__dfs[df_name] = df

    def ask(self, query: str) -> Pipe:
        return LazyPipe(self, default_rows_limit=self.__default_rows_limit).ask(query)

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
    def executor(self) -> "Executor":
        return self.__executor

    @property
    def visualizer(self) -> "Visualizer":
        return self.__visualizer

    @property
    def cache(self) -> "Cache":
        return self.__cache
