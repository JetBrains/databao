from databao.agents.lighthouse.agent import LighthouseAgent
from databao.caches.in_mem_cache import InMemCache
from databao.configs.llm import LLMConfig, LLMConfigDirectory
from databao.core import Cache, Executor, Session, Visualizer
from databao.visualizers.vega_chat import VegaChatVisualizer


def open_session(
    name: str,
    *,
    llm_config: LLMConfig | None = None,
    data_executor: Executor | None = None,
    visualizer: Visualizer | None = None,
    cache: Cache | None = None,
    default_rows_limit: int = 1000,
    default_stream_ask: bool = True,
    default_stream_plot: bool = False,
    default_lazy_threads: bool = False,
) -> Session:
    """This is an entry point for users to open a session.
    Session can't be modified after it's created. Only new data sources can be added.
    """
    llm_config = llm_config if llm_config else LLMConfigDirectory.DEFAULT
    return Session(
        name,
        llm_config,
        data_executor=data_executor or LighthouseAgent(),
        visualizer=visualizer or VegaChatVisualizer(llm_config),
        cache=cache or InMemCache(),
        default_rows_limit=default_rows_limit,
        default_stream_ask=default_stream_ask,
        default_stream_plot=default_stream_plot,
        default_lazy_threads=default_lazy_threads,
    )
