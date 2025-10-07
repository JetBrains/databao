import logging

from langchain.chat_models import init_chat_model
from langchain_core.language_models.chat_models import BaseChatModel

from portus.core.in_mem_session import InMemSession
from portus.duckdb.agent import SimpleDuckDBAgenticExecutor
from portus.executor import Executor
from portus.session import Session
from portus.vizualizer import DumbVisualizer, Visualizer

logger = logging.getLogger(__name__)
# Attach a NullHandler so importing apps without logging config don’t get warnings.
if not logger.handlers:
    logger.addHandler(logging.NullHandler())


def open_session(
    name: str,
    *,
    llm: str | BaseChatModel = "gpt-4o-mini",
    data_executor: Executor | None = None,
    visualizer: Visualizer | None = None,
    default_rows_limit: int = 1000,
) -> Session:
    if data_executor is None:
        data_executor = SimpleDuckDBAgenticExecutor()
    if visualizer is None:
        visualizer = DumbVisualizer()
    return InMemSession(
        name,
        llm if isinstance(llm, BaseChatModel) else init_chat_model(llm),
        data_executor=data_executor,
        visualizer=visualizer,
        default_rows_limit=default_rows_limit,
    )
