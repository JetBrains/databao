from portus.agents.lighthouse.agent import LighthouseAgent
from portus.caches.in_mem_cache import InMemCache
from portus.configs.llm import LLMConfig, LLMConfigDirectory
from portus.core import Cache, Executor, Session, Visualizer
from portus.visualizers.dumb import DumbVisualizer


def open_session(
    name: str,
    *,
    llm_config: LLMConfig | None = None,
    data_executor: Executor | None = None,
    visualizer: Visualizer | None = None,
    cache: Cache | None = None,
    default_rows_limit: int = 1000,
) -> Session:
    return Session(
        name,
        llm_config if llm_config else LLMConfigDirectory.DEFAULT,
        data_executor=data_executor or LighthouseAgent(),
        visualizer=visualizer or DumbVisualizer(),
        cache=cache or InMemCache(),
        default_rows_limit=default_rows_limit,
    )
