import logging
from langchain_core.language_models.chat_models import BaseChatModel
from typing import Union
from langchain.chat_models import init_chat_model

from portus.session import Session, SessionImpl
from portus.executor import Executor
from portus.duckdb.agent import SimpleDuckDBAgenticExecutor
from portus.vizualizer import Visualizer, DumbVisualizer

logger = logging.getLogger(__name__)
# Attach a NullHandler so importing apps without logging config don’t get warnings.
if not logger.handlers:
    logger.addHandler(logging.NullHandler())


def create_session(
        llm: Union[str, BaseChatModel],
        *,
        data_executor: Executor = SimpleDuckDBAgenticExecutor(),
        visualizer: Visualizer = DumbVisualizer(),
        default_rows_limit: int = 1000
) -> Session:
    return SessionImpl(
        llm if isinstance(llm, BaseChatModel) else init_chat_model(llm),
        data_executor=data_executor,
        visualizer=visualizer,
        default_rows_limit=default_rows_limit
    )
