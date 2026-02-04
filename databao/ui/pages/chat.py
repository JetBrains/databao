"""Chat page - individual chat session interface."""

import logging
from typing import cast

import streamlit as st

from databao.core.agent import Agent
from databao.core.thread import Thread
from databao.ui.databao_project import DatabaoProject, DCEProjectStatus
from databao.ui.app import _clear_all_chat_threads
from databao.ui.components.chat import render_chat_interface
from databao.ui.components.sidebar import render_sidebar_chat_content
from databao.ui.components.status import AppStatus, set_status
from databao.ui.models.chat_session import ChatSession
from databao.ui.services.chat_persistence import save_chat
from databao.ui.services.chat_title import check_title_completion, trigger_title_generation

logger = logging.getLogger(__name__)


def _render_chat_sidebar(project: DatabaoProject | None) -> None:
    """Render chat-specific sidebar content.

    The header (logo + status) is rendered globally by app.py.
    This adds the chat-specific content (project, sources, executor).
    """
    with st.sidebar:
        st.markdown("---")
        render_sidebar_chat_content(project)


def render_chat_page() -> None:
    """Render the chat page for a specific chat session."""
    # Get or create the current chat session
    chat = _get_or_create_current_chat()

    if chat is None:
        st.error("No chat session found. Please start a new chat.")
        welcome_page = st.session_state.get("_page_welcome")
        if welcome_page and st.button("🏠 Go to Home"):
            st.switch_page(welcome_page)
        return

    # Get project and initialize agent if needed
    # Note: Project loading and status (no project / needs build) is handled in app.py
    project = _get_current_project()

    if project is None:
        # Status already set by app.py
        _render_chat_sidebar(None)
        _render_no_project_state()
        return

    if project.dce_status == DCEProjectStatus.NO_BUILD:
        # Status already set by app.py
        _render_chat_sidebar(project)
        _render_no_build_state(project)
        return

    # Get agent (initialized at app level in app.py)
    agent: Agent | None = st.session_state.get("agent")

    if agent is None:
        _render_chat_sidebar(project)
        _render_error_state()
        return

    # Get or create thread for this chat
    if not _get_or_create_thread_for_chat(chat, agent):
        _render_chat_sidebar(project)
        st.error("Failed to create conversation thread")
        return

    # Render chat-specific sidebar content
    _render_chat_sidebar(project)

    # Check for title generation completion
    if chat.title_status == "generating" and check_title_completion(chat):
        # Title is ready - update the chats dict to persist the change
        chats = st.session_state.get("chats", {})
        chats[chat.id] = chat
        st.session_state.chats = chats
        # Save to disk
        save_chat(chat)

    # Render chat title
    title = chat.display_title
    st.title(f"💬 {title}")

    # Render chat interface (chat.thread is guaranteed to be set at this point)
    render_chat_interface(chat)

    # After first response, trigger title generation if needed
    if chat.has_first_response and chat.title_status == "pending":
        trigger_title_generation(agent, chat)
        # Update chats dict
        chats = st.session_state.get("chats", {})
        chats[chat.id] = chat
        st.session_state.chats = chats
        # Save to disk
        save_chat(chat)


def _get_or_create_current_chat() -> ChatSession | None:
    """Get the current chat session from URL or session state."""
    from uuid6 import uuid6

    chats: dict[str, ChatSession] = st.session_state.get("chats", {})
    current_id: str | None = st.session_state.get("current_chat_id")

    # If we have a current chat ID, use it
    if current_id and current_id in chats:
        return chats[current_id]

    # If no current chat but we have chats, use the most recent
    if chats and not current_id:
        # Sort by created_at, newest first
        sorted_chats = sorted(chats.values(), key=lambda c: c.created_at, reverse=True)
        current_id = sorted_chats[0].id
        st.session_state.current_chat_id = current_id
        return sorted_chats[0]

    # No chats exist - create a new one
    chat_id = str(uuid6())
    chat = ChatSession(id=chat_id)

    chats[chat_id] = chat
    st.session_state.chats = chats
    st.session_state.current_chat_id = chat_id

    # Save new chat to disk
    save_chat(chat)

    return chat


def _get_current_project() -> DatabaoProject | None:
    """Get the current DCE project from session state.

    Project loading and auto-detection is handled by app.py.
    This just retrieves the project for chat page use.
    """
    project = st.session_state.get("databao_project")
    return cast(DatabaoProject, project) if project is not None else None


def _get_or_create_thread_for_chat(chat: ChatSession, agent: Agent) -> bool:
    """Get or create a thread for the specific chat session.

    Returns True if thread is available, False on error.
    """
    from databao.ui.streaming import StreamingWriter

    # Ensure writer exists (it's not persisted, so may be None on reload)
    if chat.writer is None:
        chat.writer = StreamingWriter()

    # Each chat has its own thread
    if chat.thread is not None:
        return True

    try:
        # If chat has a saved cache_scope, use it to restore the conversation history
        # This allows the agent to remember previous messages from the persisted chat
        thread = agent.thread(
            stream_ask=True,
            stream_plot=False,
            cache_scope=chat.cache_scope,  # May be None for new chats
            writer=chat.writer,  # Per-chat writer for streaming
        )
        chat.thread = thread
        # Store the cache scope for persistence (in case it was newly generated)
        chat.cache_scope = thread._cache_scope

        # Restore thread's internal state from persisted messages
        # This is needed for "Generate Plot" to work on restored chats
        _restore_thread_state_from_messages(thread, chat)

        # Update chats dict
        chats = st.session_state.get("chats", {})
        chats[chat.id] = chat
        st.session_state.chats = chats

        # Save to disk with cache_scope
        save_chat(chat)

        return True
    except Exception:
        logger.exception("Failed to create thread")
        return False


def _restore_thread_state_from_messages(thread: Thread, chat: ChatSession) -> None:
    """Restore thread's internal state from persisted chat messages.

    When a chat is loaded from disk, the Thread is new and has no internal state.
    This restores _data_result from the last assistant message so that
    "Generate Plot" works correctly.

    Note: We don't restore _visualization_result because VegaChatResult is a
    Pydantic model requiring fields (text, plot, visualizer) that we can't
    reconstruct. Instead, render_visualization_section uses visualization_data
    from ChatMessage as a fallback.
    """
    # Find the last assistant message with a result
    for msg in reversed(chat.messages):
        if msg.role == "assistant" and msg.result is not None:
            # Restore the data result so plot() can use it
            thread._data_result = msg.result
            logger.debug(f"Restored thread._data_result from persisted chat {chat.id}")
            break


def _render_no_project_state() -> None:
    """Render state when no DCE project is found."""
    st.title("💬 Chat")
    st.markdown("---")

    st.warning("No DCE project detected.")

    st.markdown(
        """
        ### Getting Started

        To use Databao, you need a DCE (Databao Context Engine) project with configured datasources.

        **Set up a new project:**
        ```bash
        nemory init
        nemory datasource add
        nemory build
        ```

        Or configure the project path in Settings.
        """
    )

    context_settings_page = st.session_state.get("_page_context_settings")
    if context_settings_page and st.button("⚙️ Go to Settings"):
        st.switch_page(context_settings_page)


def _render_no_build_state(project: DatabaoProject) -> None:
    """Render state when DCE project has no build output."""
    st.title("💬 Chat")
    st.markdown("---")

    st.warning(f"DCE project found at `{project.path}` but no build output exists.")

    st.markdown(
        """
        ### Build Required

        The DCE project needs to be built before Databao can use it.

        Run the following command:
        ```bash
        nemory build
        ```

        Then reload this page.
        """
    )

    if st.button("🔄 Check Again"):
        st.session_state.databao_project = None
        st.rerun()


def _render_error_state() -> None:
    """Render error state."""
    st.title("💬 Chat")
    st.markdown("---")

    default_message = "An error occurred"
    error_message = st.session_state.get("status_message", default_message) or default_message
    st.error(error_message)

    if st.button("🔄 Retry"):
        st.session_state.agent = None
        _clear_all_chat_threads()
        set_status(AppStatus.INITIALIZING, "Retrying...")
        st.rerun()
