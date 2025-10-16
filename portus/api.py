from portus.agents.lighthouse.agent import LighthouseAgent
from portus.configs.llm import LLMConfig

from .caches.in_mem_cache import InMemCache
from .core import Cache, Executor, Session, Visualizer
from .visualizers.dumb import DumbVisualizer

_DEFAULT_LLM_CONFIG = LLMConfig(name="gpt-4o-mini")


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
        llm_config if llm_config else _DEFAULT_LLM_CONFIG,
        data_executor=data_executor or LighthouseAgent(),
        visualizer=visualizer or DumbVisualizer(),
        cache=cache or InMemCache(),
        default_rows_limit=default_rows_limit,
    )
