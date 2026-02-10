"""Chat title generation service for async LLM-based title creation."""

import logging
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from datetime import datetime
from typing import TYPE_CHECKING

import streamlit as st
from langchain_core.messages import HumanMessage, SystemMessage
from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from databao.core.agent import Agent
    from databao.ui.models.chat_session import ChatSession

logger = logging.getLogger(__name__)


@st.cache_resource
def _get_title_executor() -> ThreadPoolExecutor:
    """Get a shared thread pool executor for title generation."""
    return ThreadPoolExecutor(max_workers=2, thread_name_prefix="chat_title")


class ChatTitle(BaseModel):
    """Structured output model for chat title."""

    title: str = Field(
        description="A short 3-5 word title summarizing the conversation topic",
        max_length=50,
    )


TITLE_PROMPT = """Generate a very short title (3-5 words, max 50 characters) that summarizes what this question is about.
The title should be descriptive and help identify the conversation later.

Do NOT include:
- Question marks
- Words like "Question about" or "Asking about"
- Quotes around the title

Just output a short, descriptive phrase like:
- "Sales Revenue Analysis"
- "Customer Order Trends"  
- "Product Inventory Summary"
"""


def _fallback_title(created_at: datetime) -> str:
    """Create a fallback title using timestamp."""
    return f"Chat {created_at.strftime('%b %d, %H:%M')}"


def _generate_chat_title(agent: "Agent", first_message: str, created_at: datetime) -> str:
    """Generate a short title for a chat based on the first user message.

    Args:
        agent: The Databao agent (used for LLM access).
        first_message: The first user message in the chat.
        created_at: Chat creation timestamp for fallback title.

    Returns:
        A short title string (3-5 words).
    """
    try:
        # Use structured output for reliable extraction
        llm_with_structure = agent.llm.with_structured_output(ChatTitle)

        # Generate title
        result = llm_with_structure.invoke([
            SystemMessage(content=TITLE_PROMPT),
            HumanMessage(content=f"Question: {first_message}"),
        ])

        if result and result.title:
            title = result.title.strip()
            # Clean up any trailing punctuation
            title = title.rstrip("?!.")
            logger.info("Generated chat title: %s", title)
            return title

        logger.warning("LLM returned invalid title result")
        return _fallback_title(created_at)

    except Exception as e:
        logger.warning("Failed to generate chat title: %s", e)
        return _fallback_title(created_at)


def _generate_title_task(
    agent: "Agent",
    chat_id: str,
    first_message: str,
    created_at: datetime,
) -> tuple[str, str]:
    """Background task that generates a title.

    Args:
        agent: The Databao agent.
        chat_id: The chat ID to associate the title with.
        first_message: The first user message.
        created_at: Chat creation timestamp for fallback.

    Returns:
        Tuple of (chat_id, title).
    """
    try:
        title = _generate_chat_title(agent, first_message, created_at)
        return chat_id, title
    except Exception as e:
        logger.warning("Background title generation failed for chat %s: %s", chat_id, e)
        return chat_id, _fallback_title(created_at)


def trigger_title_generation(agent: "Agent", chat: "ChatSession") -> bool:
    """Start background title generation for a chat if needed.

    Should be called after the first assistant response.

    Returns True if generation was started.
    """
    # Only generate if pending and we have a first message
    if chat.title_status != "pending":
        return False

    first_msg = chat.first_user_message
    if not first_msg:
        return False

    # Submit task to executor
    executor = _get_title_executor()
    future: Future[tuple[str, str]] = executor.submit(
        _generate_title_task, agent, chat.id, first_msg, chat.created_at
    )

    # Store in session state keyed by chat ID
    title_futures = st.session_state.get("title_futures", {})
    title_futures[chat.id] = future
    st.session_state.title_futures = title_futures

    # Update chat status
    chat.title_status = "generating"

    logger.info("Started background title generation for chat %s", chat.id)
    return True


def check_title_completion(chat: "ChatSession") -> bool:
    """Check if title generation for a chat has completed.

    If completed, updates the chat's title.

    Returns True if title became ready (caller may want to rerun).
    """
    if chat.title_status != "generating":
        return False

    title_futures: dict[str, Future[tuple[str, str]]] = st.session_state.get("title_futures", {})

    if chat.id not in title_futures:
        # No future found - use fallback
        chat.title = _fallback_title(chat.created_at)
        chat.title_status = "ready"
        return True

    future = title_futures[chat.id]

    if not future.done():
        return False

    # Get result
    try:
        result = future.result(timeout=1.0)
    except FuturesTimeoutError:
        logger.warning("Timeout getting title future result for chat %s", chat.id)
        result = None
    except Exception as e:
        logger.warning("Failed to get title result for chat %s: %s", chat.id, e)
        result = None

    # Clean up
    del title_futures[chat.id]
    st.session_state.title_futures = title_futures

    # Handle result
    if result is None:
        chat.title = _fallback_title(chat.created_at)
    else:
        _, title = result
        chat.title = title

    chat.title_status = "ready"

    logger.info("Title generation completed for chat %s: %s", chat.id, chat.title)
    return True
