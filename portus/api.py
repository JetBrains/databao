from portus.llms import LLMConfig
from portus.session.base_session import BaseSession
from portus.session.in_mem_session import InMemSession
from portus.visualizer import Visualizer


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
