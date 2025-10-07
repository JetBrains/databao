import logging

from portus.agent.base_agent import BaseAgent as BaseAgent
from portus.agent.lighthouse_agent import LighthouseAgent as LighthouseAgent
from portus.agent.react_duckdb_agent import ReactDuckDBAgent as ReactDuckDBAgent
from portus.llms import LLMConfig
from portus.session.base_session import BaseSession
from portus.session.in_mem_session import InMemSession
from portus.vizualizer import Visualizer

logger = logging.getLogger(__name__)
# Attach a NullHandler so importing apps without logging config don’t get warnings.
if not logger.handlers:
    logger.addHandler(logging.NullHandler())


def open_session(
    name: str,
    *,
    llm: str | LLMConfig = "claude-sonnet-4-5-20250929",
    visualizer: Visualizer | None = None,
    default_rows_limit: int = 1000,
) -> BaseSession:
    return InMemSession(
        name,
        llm if isinstance(llm, LLMConfig) else LLMConfig(name=llm),
        visualizer=visualizer,
        default_rows_limit=default_rows_limit,
    )
