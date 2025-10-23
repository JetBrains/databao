import pickle
from abc import abstractmethod
from io import BytesIO
from typing import Any

from langchain_core.messages import BaseMessage, HumanMessage
from langgraph.graph.state import CompiledStateGraph

from portus.configs.llm import LLMConfig
from portus.core import Executor, Opa, Session


class AgentExecutor(Executor):
    """
    Base class for agents that execute with a DuckDB connection and LLM configuration.
    Provides common functionality for graph caching, message handling, and OPA processing.
    """

    def __init__(self) -> None:
        """Initialize agent with graph caching infrastructure."""
        # TODO Caching should be scoped to the Session/Pipe/Thread, not the Executor instance
        self._cached_compiled_graph: CompiledStateGraph[Any] | None = None
        self._cached_llm_config: LLMConfig | None = None
        self._cached_data_source_names: list[str] | None = None

    def _get_messages(self, session: Session, cache_scope: str) -> list[BaseMessage]:
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

    @abstractmethod
    def _create_graph(self, session: Session) -> CompiledStateGraph[Any]:
        """
        Create and compile the agent graph.

        Subclasses must implement this method to return their specific graph implementation.

        Returns:
            Compiled graph ready for execution
        """
        pass

    def _should_recompile_graph(self, session: Session) -> bool:
        """Check if the graph needs recompilation (e.g., due to connection or config changes)."""
        data_engine = session.data_engine
        data_source_names = sorted(s.name for s in data_engine.sources.values())
        return (
            self._cached_compiled_graph is None
            or self._cached_data_source_names != data_source_names
            or self._cached_llm_config != session.llm_config
        )

    def _cache_graph(self, session: Session, compiled_graph: CompiledStateGraph[Any]) -> None:
        """Cache the compiled graph and associated IDs."""
        data_engine = session.data_engine
        data_source_names = sorted(s.name for s in data_engine.sources.values())
        self._cached_compiled_graph = compiled_graph
        self._cached_llm_config = session.llm_config
        self._cached_data_source_names = data_source_names

    def _get_or_create_cached_graph(self, session: Session) -> CompiledStateGraph[Any]:
        """Get cached graph or create new one if connection/config changed."""
        if self._cached_compiled_graph is not None and not self._should_recompile_graph(session):
            return self._cached_compiled_graph

        compiled_graph = self._create_graph(session)
        self._cache_graph(session, compiled_graph)
        return compiled_graph

    def _process_opa(self, session: Session, opa: Opa, cache_scope: str) -> list[BaseMessage]:
        """
        Process a single opa and convert it to a message, appending to message history.

        Returns:
            All messages including the new one
        """
        messages = self._get_messages(session, cache_scope)
        messages.append(HumanMessage(content=opa.query))
        return messages

    def _update_message_history(self, session: Session, cache_scope: str, final_messages: list[BaseMessage]) -> None:
        """Update message history in cache with final messages from graph execution."""
        if final_messages:
            self._set_messages(session, cache_scope, final_messages)
