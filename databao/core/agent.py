from typing import TYPE_CHECKING

from langchain_core.language_models.chat_models import BaseChatModel

from databao.core.context import Context
from databao.core.data_source import DBDataSource, DFDataSource, Sources
from databao.core.thread import Thread

if TYPE_CHECKING:
    from databao.configs.agent import AgentConfig
    from databao.configs.llm import LLMConfig
    from databao.core.cache import Cache
    from databao.core.executor import Executor
    from databao.core.visualizer import Visualizer


# TODO (dce): use Context.search_context
class Agent:
    """An agent manages all databases and Dataframes as well as the context for them.
    Agent determines what LLM to use, what executor to use and how to visualize data for all threads.
    Several threads can be spawned out of the agent.
    """

    def __init__(
        self,
        context: Context,
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

        self.__sources: Sources = context.sources

        self.__executor = data_executor
        self.__visualizer = visualizer
        self.__cache = cache

        # Thread defaults
        self.__rows_limit = rows_limit
        self.__lazy_threads = lazy_threads
        self.__auto_output_modality = auto_output_modality
        self.__stream_ask = stream_ask
        self.__stream_plot = stream_plot

        self._init_executor()

    def _init_executor(self) -> None:
        for db_source in self.__sources.dbs.values():
            self.executor.register_db(db_source)
        for df_source in self.__sources.dfs.values():
            self.executor.register_df(df_source)

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
