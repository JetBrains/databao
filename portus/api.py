from portus.configs.llm import DefaultLLMConfig, LLMConfig

from .caches.in_mem_cache import InMemCache
from .core import Cache, Executor, Session, Visualizer
from .duckdb.agents import SimpleDuckDBAgenticExecutor
from .visualizers.dumb import DumbVisualizer


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
        llm_config if llm_config else DefaultLLMConfig(),
        data_executor=data_executor or SimpleDuckDBAgenticExecutor(),
        visualizer=visualizer or DumbVisualizer(),
        cache=cache or InMemCache(),
        default_rows_limit=default_rows_limit,
    )
