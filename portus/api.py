from langchain.chat_models import init_chat_model
from langchain_core.language_models.chat_models import BaseChatModel

from .agents.duckdb import SimpleDuckDBAgenticExecutor
from .core import Executor, Session, Visualizer
from .sessions.in_memory import InMemSession
from .visualizers.dumb import DumbVisualizer


def open_session(
    name: str,
    *,
    llm: str | BaseChatModel = "gpt-4o-mini",
    data_executor: Executor | None = None,
    visualizer: Visualizer | None = None,
    default_rows_limit: int = 1000,
) -> Session:
    return InMemSession(
        name,
        llm if isinstance(llm, BaseChatModel) else init_chat_model(llm),
        data_executor=data_executor or SimpleDuckDBAgenticExecutor(),
        visualizer=visualizer or DumbVisualizer(),
        default_rows_limit=default_rows_limit,
    )
