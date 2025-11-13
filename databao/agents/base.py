import pickle
from io import BytesIO
from typing import Any

from langchain_core.messages import HumanMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph.state import CompiledStateGraph

from databao.agents.frontend.text_frontend import TextStreamFrontend
from databao.configs.llm import LLMConfig
from databao.core import Executor, Opa, Session

try:
    from duckdb import DuckDBPyConnection
except ImportError:
    DuckDBPyConnection = Any  # type: ignore


class AgentExecutor(Executor):
    """
    Base class for LangGraph agents that execute with a DuckDB connection and LLM configuration.
    Provides common functionality for graph caching, message handling, and OPA processing.
    """

    def __init__(self) -> None:
        """Initialize agent with graph caching infrastructure."""
        self._graph_recursion_limit = 50

    def _get_llm_config(self, session: Session) -> LLMConfig:
        """Get LLM config from session."""
        return session.llm_config

    def _get_messages(self, session: Session, cache_scope: str) -> list[Any]:
        """Retrieve messages from the session cache."""
        try:
            buffer = BytesIO()
            session.cache.scoped(cache_scope).get("messages", buffer)
            buffer.seek(0)
            result: list[Any] = pickle.load(buffer)
            return result
        except (KeyError, EOFError):
            return []

    def _set_messages(self, session: Session, cache_scope: str, messages: list[Any]) -> None:
        """Store messages in the session cache."""
        buffer = BytesIO()
        pickle.dump(messages, buffer)
        buffer.seek(0)
        session.cache.scoped(cache_scope).put("messages", buffer)

    def _process_opa(self, session: Session, opa: Opa, cache_scope: str) -> list[Any]:
        """
        Process a single opa and convert it to a message, appending to message history.

        Returns:
            All messages including the new one
        """
        messages = self._get_messages(session, cache_scope)
        messages.append(HumanMessage(content=opa.query))
        return messages

    def _update_message_history(self, session: Session, cache_scope: str, final_messages: list[Any]) -> None:
        """Update message history in cache with final messages from graph execution."""
        if final_messages:
            self._set_messages(session, cache_scope, final_messages)

    @staticmethod
    def _invoke_graph_sync(
        compiled_graph: CompiledStateGraph[Any],
        start_state: Any,
        *,
        config: RunnableConfig | None = None,
        stream: bool = True,
        **kwargs: Any,
    ) -> Any:
        """Invoke the graph with the given start state and return the output state."""
        if stream:
            return AgentExecutor._execute_stream_sync(compiled_graph, start_state, config=config, **kwargs)
        else:
            return compiled_graph.invoke(start_state, config=config)

    @staticmethod
    async def _execute_stream(
        compiled_graph: CompiledStateGraph[Any],
        start_state: Any,
        *,
        config: RunnableConfig | None = None,
        **kwargs: Any,
    ) -> Any:
        writer = TextStreamFrontend(start_state)
        last_state = None
        async for mode, chunk in compiled_graph.astream(
            start_state,
            stream_mode=["values", "messages"],
            config=config,
            **kwargs,
        ):
            writer.write_stream_chunk(mode, chunk)
            if mode == "values":
                last_state = chunk
        writer.end()
        assert last_state is not None
        return last_state

    @staticmethod
    def _execute_stream_sync(
        compiled_graph: CompiledStateGraph[Any],
        start_state: Any,
        *,
        config: RunnableConfig | None = None,
        **kwargs: Any,
    ) -> Any:
        writer = TextStreamFrontend(start_state)
        last_state = None
        for mode, chunk in compiled_graph.stream(
            start_state,
            stream_mode=["values", "messages"],
            config=config,
            **kwargs,
        ):
            writer.write_stream_chunk(mode, chunk)
            if mode == "values":
                last_state = chunk
        writer.end()
        assert last_state is not None
        return last_state
