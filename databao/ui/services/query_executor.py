"""Background query execution service for chat queries.

This module provides background execution of queries so that they continue
running when users switch between chats. The pattern follows suggestions.py.
"""

import logging
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx

if TYPE_CHECKING:
    from databao.core.thread import Thread
    from databao.ui.models.chat_session import ChatSession
    from databao.ui.streaming import StreamingWriter

logger = logging.getLogger(__name__)


@dataclass
class QueryResult:
    """Result of a background query execution."""

    text: str  # Response text
    thinking: str  # Captured thinking/reasoning text
    result: Any  # ExecutionResult from thread._data_result
    has_visualization: bool
    error: str | None = None  # Error message if execution failed


class QueryThread(threading.Thread):
    """Custom thread for query execution that stores its result."""

    def __init__(
        self,
        databao_thread: "Thread",
        query: str,
        writer: "StreamingWriter | None",
    ):
        super().__init__(name="query_worker", daemon=True)
        self.databao_thread = databao_thread
        self.query = query
        self.writer = writer
        self.result: QueryResult | None = None

    def run(self) -> None:
        """Execute the query and store the result."""
        try:
            self.databao_thread.ask(self.query, stream=True)
            result = self.databao_thread._data_result

            has_visualization = False
            if result and result.meta:
                hints = result.meta.get("output_modality_hints")
                if hints:
                    has_visualization = getattr(hints, "should_visualize", False)
            if self.databao_thread._visualization_result is not None:
                has_visualization = True

            thinking_text = self.writer.getvalue() if self.writer else ""

            self.result = QueryResult(
                text=result.text if result else "",
                thinking=thinking_text,
                result=result,
                has_visualization=has_visualization,
                error=None,
            )
        except Exception as e:
            logger.exception("Query execution failed")
            thinking_text = self.writer.getvalue() if self.writer else ""
            self.result = QueryResult(
                text="",
                thinking=thinking_text,
                result=None,
                has_visualization=False,
                error=str(e),
            )


def start_query_execution(chat: "ChatSession", thread: "Thread", query: str) -> bool:
    """Start background query execution for a chat.

    Following Streamlit's recommended pattern for multithreading:
    1. Create the Thread object
    2. Add script context BEFORE starting
    3. Start the thread

    Args:
        chat: The ChatSession to execute the query for.
        thread: The Thread object to use.
        query: The user's question.

    Returns:
        True if execution was started, False if already running.
    """
    if chat.query_status == "running":
        logger.warning("Query already running for chat %s", chat.id)
        return False

    # Clear the writer buffer before starting a new query
    if chat.writer:
        chat.writer.clear()

    # Capture the current Streamlit script context
    script_ctx = get_script_run_ctx()

    # Create thread object
    query_thread = QueryThread(thread, query, chat.writer)

    # Add script context BEFORE starting thread (per Streamlit docs)
    if script_ctx is not None:
        add_script_run_ctx(query_thread, script_ctx)

    # Start the thread
    query_thread.start()

    # Update chat state - store thread reference instead of future
    chat.query_thread = query_thread
    chat.query_status = "running"

    logger.info("Started background query execution for chat %s: %s", chat.id, query[:50])
    return True


def check_query_completion(chat: "ChatSession") -> QueryResult | None:
    """Check if a chat's background query has completed.

    If completed, updates chat state and returns the result.

    Args:
        chat: The ChatSession to check.

    Returns:
        QueryResult if query completed, None if still running or not started.
    """
    if chat.query_status != "running":
        return None

    query_thread: QueryThread | None = getattr(chat, "query_thread", None)
    if query_thread is None:
        # No thread but status is running - inconsistent state, reset
        chat.query_status = "idle"
        return None

    if query_thread.is_alive():
        return None

    # Thread has finished - get the result
    result = query_thread.result
    if result is None:
        result = QueryResult(
            text="",
            thinking="",
            result=None,
            has_visualization=False,
            error="Thread finished without result",
        )

    # Clean up chat state
    chat.query_thread = None
    chat.query_status = "completed" if result.error is None else "error"

    logger.info(
        "Query completed for chat %s: success=%s",
        chat.id,
        result.error is None,
    )
    return result


def is_query_running(chat: "ChatSession") -> bool:
    """Check if a chat has a query currently running.

    Args:
        chat: The ChatSession to check.

    Returns:
        True if a query is in progress.
    """
    return chat.query_status == "running"


def cancel_query(chat: "ChatSession") -> bool:
    """Attempt to cancel a running query.

    Note: This only prevents the result from being processed.
    The underlying thread.ask() call cannot be truly cancelled.

    Args:
        chat: The ChatSession with the query to cancel.

    Returns:
        True if a query was cancelled, False if no query was running.
    """
    if chat.query_status != "running":
        return False

    # We can't actually stop the thread execution, but we can
    # mark it as cancelled so the result is ignored
    chat.query_status = "idle"
    chat.query_thread = None

    logger.info("Cancelled query for chat %s", chat.id)
    return True
