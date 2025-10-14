from langchain.chat_models import init_chat_model
from langchain_core.language_models.chat_models import BaseChatModel

from .caches.in_mem_cache import InMemCache
from .core import Cache, Executor, Session, Visualizer
from .duckdb.agents import SimpleDuckDBAgenticExecutor
from .visualizers.dumb import DumbVisualizer


def open_session(
    name: str,
    *,
    llm: str | BaseChatModel = "gpt-4o-mini",
    data_executor: Executor | None = None,
    visualizer: Visualizer | None = None,
    cache: Cache | None = None,
    default_rows_limit: int = 1000,
) -> Session:
    return Session(
        name,
        llm if isinstance(llm, BaseChatModel) else init_chat_model(llm),
        data_executor=data_executor or SimpleDuckDBAgenticExecutor(),
        visualizer=visualizer or DumbVisualizer(),
        cache=cache or InMemCache(),
        default_rows_limit=default_rows_limit,
    )
