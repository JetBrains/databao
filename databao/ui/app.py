"""Databao Streamlit Web Interface - Main Application with Multipage Navigation."""

import argparse
import logging
from pathlib import Path
from typing import cast

import streamlit as st

import databao
from databao.caches.disk_cache import DiskCache, DiskCacheConfig
from databao.core.agent import Agent
from databao.core.v2.context import Context
from databao.dbt import DbtConfig
from databao.ui.components.status import AppStatus, set_status, status_context
from databao.ui.databao_project import DatabaoProject, DCEProjectStatus
from databao.ui.models.chat_session import ChatSession
from databao.ui.services.storage import get_cache_dir

logger = logging.getLogger(__name__)

# Page config - use Databao logo as favicon
_ASSETS_DIR = Path(__file__).parent / "assets"
_FAVICON = _ASSETS_DIR / "bao.png"

st.set_page_config(
    page_title="Databao",
    page_icon=str(_FAVICON) if _FAVICON.exists() else "🎋",
    layout="wide",
    initial_sidebar_state="expanded",
)

def _load_persisted_state() -> None:
    """Load settings and chats from disk on startup."""
    from databao.ui.services.chat_persistence import load_all_chats
    from databao.ui.services.settings_persistence import get_or_create_settings

    # Load or create settings
    if "app_settings" not in st.session_state:
        settings = get_or_create_settings()
        st.session_state.app_settings = settings

        # Apply loaded settings to session state
        st.session_state.executor_type = settings.agent.executor_type

    # Load chats from disk (only once on startup)
    if "_chats_loaded" not in st.session_state:
        chats = load_all_chats()
        if chats:
            st.session_state.chats = chats
            logger.info(f"Loaded {len(chats)} chats from disk")
        st.session_state._chats_loaded = True


def _save_settings_if_changed() -> None:
    """Save settings to disk if they've changed."""
    from databao.ui.models.settings import Settings
    from databao.ui.services.settings_persistence import save_settings

    settings: Settings | None = st.session_state.get("app_settings")
    if settings is None:
        return

    # Check for changes and update settings object
    changed = False

    current_executor = st.session_state.get("executor_type", "lighthouse")
    if settings.agent.executor_type != current_executor:
        settings.agent.executor_type = current_executor
        changed = True

    if changed:
        save_settings(settings)
        logger.debug("Settings saved")


def _get_or_create_disk_cache() -> DiskCache:
    """Get or create the DiskCache instance for the agent."""
    if "disk_cache" not in st.session_state:
        cache_dir = get_cache_dir()
        config = DiskCacheConfig(db_dir=cache_dir / "diskcache")
        st.session_state.disk_cache = DiskCache(config=config)
    return st.session_state.disk_cache


def _initialize_agent(project: DatabaoProject) -> Agent | None:
    """Initialize or return existing Databao agent.

    This is called at app level to ensure one agent is shared across all chats.
    """
    if st.session_state.get("agent") is not None:
        return cast(Agent, st.session_state.agent)

    if project.dce_status == DCEProjectStatus.NO_BUILD:
        set_status(
            AppStatus.INITIALIZING,
            "DCE project found but no build output. Run 'databao build' first.",
        )
        return None

    try:
        executor_type = st.session_state.get("executor_type", "lighthouse")

        # Use DiskCache for persistence
        cache = _get_or_create_disk_cache()

        # Load context using the new DCE integration
        with status_context(AppStatus.INITIALIZING, "Loading context..."):
            context = project.context

        # Check if we have any datasources
        if not context.sources.dbs and not context.sources.dfs:
            set_status(AppStatus.ERROR, "No datasource connections found in DCE project.")
            return None

        # Create AgentV2 with the context
        from databao.api import new_agent_v2

        agent = new_agent_v2(
            context=context,
            executor_type=executor_type,
            cache=cache,
            # add executor props new field for dbt
            dbt_config=DbtConfig(project_dir=Path("/Users/andrei.gasparian/Documents/databao-agent/examples/shopify002"))
        )

        st.session_state.agent = agent

        return agent

    except Exception as e:
        logger.exception("Failed to initialize agent")
        set_status(AppStatus.ERROR, f"Failed to initialize agent: {e}")
        return None


def _clear_all_chat_threads() -> None:
    """Clear thread references from all chats.

    Called when agent is reset. Threads will be recreated from
    persistence (cache_scope) when chats are next accessed.
    """
    chats: dict[str, ChatSession] = st.session_state.get("chats", {})
    for chat in chats.values():
        chat.thread = None


def _initialize_app(project_dir: str):
    """Initialize app-level resources: project and agent.

    This is called on every rerun but returns early if already initialized.
    """

    project = _get_current_project(project_dir)

    if project.dce_status == DCEProjectStatus.NO_BUILD:
        set_status(AppStatus.INITIALIZING, "Project needs build")
        return

    # Load persisted state (settings + chats)
    with status_context(AppStatus.INITIALIZING, "Loading app data from disk..."):
        _load_persisted_state()

    with status_context(AppStatus.INITIALIZING, "Initializing agent..."):
        agent = _initialize_agent(project)

    if agent:
        set_status(AppStatus.READY)


def init_session_state() -> None:
    """Initialize session state variables."""
    # Chat sessions
    if "chats" not in st.session_state:
        st.session_state.chats = {}
    if "current_chat_id" not in st.session_state:
        st.session_state.current_chat_id = None

    # DCE/Agent state
    if "databao_project" not in st.session_state:
        st.session_state.databao_project = None
    if "databao_project_path" not in st.session_state:
        st.session_state.databao_project_path = None
    if "agent" not in st.session_state:
        st.session_state.agent = None
    if "app_status" not in st.session_state:
        st.session_state.app_status = "initializing"
    if "status_message" not in st.session_state:
        st.session_state.status_message = None
    if "executor_type" not in st.session_state:
        st.session_state.executor_type = "lighthouse"

    # Suggestions state
    if "suggested_questions" not in st.session_state:
        st.session_state.suggested_questions = []
    if "suggestions_are_llm_generated" not in st.session_state:
        st.session_state.suggestions_are_llm_generated = False
    if "suggestions_status" not in st.session_state:
        st.session_state.suggestions_status = "not_started"
    if "suggestions_future" not in st.session_state:
        st.session_state.suggestions_future = None
    if "suggestions_cancel_event" not in st.session_state:
        st.session_state.suggestions_cancel_event = None

    # Title generation state
    if "title_futures" not in st.session_state:
        st.session_state.title_futures = {}


def _create_new_chat() -> None:
    """Create a new chat and navigate to it."""
    from uuid6 import uuid6

    from databao.ui.services.chat_persistence import save_chat

    # Save current chat before creating new one
    prev_chat_id = st.session_state.get("current_chat_id")
    chats: dict[str, ChatSession] = st.session_state.get("chats", {})
    if prev_chat_id and prev_chat_id in chats:
        prev_chat = chats[prev_chat_id]
        # Persist previous chat to disk (messages and thread are already in chat object)
        save_chat(prev_chat)

    # Create new chat
    chat_id = str(uuid6())
    chat = ChatSession(id=chat_id)

    chats[chat_id] = chat
    st.session_state.chats = chats
    st.session_state.current_chat_id = chat_id

    # Flag to navigate to this chat on next rerun
    st.session_state._navigate_to_chat = chat_id

    # Save new chat to disk
    save_chat(chat)


def build_navigation() -> None:
    """Build the multipage navigation structure."""
    from databao.ui.pages.agent_settings import render_agent_settings_page
    from databao.ui.pages.chat import render_chat_page
    from databao.ui.pages.context_settings import render_context_settings_page
    from databao.ui.pages.general_settings import render_general_settings_page
    from databao.ui.pages.welcome import render_welcome_page

    # Check if we need to navigate to a newly created chat
    navigate_to_chat: str | None = st.session_state.get("_navigate_to_chat")
    if navigate_to_chat:
        # Clear the navigation flag
        st.session_state._navigate_to_chat = None

    # Settings pages - store in session state for navigation access
    general_settings_page = st.Page(
        render_general_settings_page,
        title="General",
        icon="🛠️",
        url_path="general-settings",
    )
    context_settings_page = st.Page(
        render_context_settings_page,
        title="Context Settings",
        icon="📊",
        url_path="context-settings",
    )
    agent_settings_page = st.Page(
        render_agent_settings_page,
        title="Agent Settings",
        icon="⚙️",
        url_path="agent-settings",
    )
    settings_pages = [general_settings_page, context_settings_page, agent_settings_page]

    # Store page objects in session state for cross-page navigation
    st.session_state._page_general_settings = general_settings_page
    st.session_state._page_context_settings = context_settings_page
    st.session_state._page_agent_settings = agent_settings_page

    # Chat pages - build dynamically from session state
    chat_pages: list[st.Page] = []

    # "New Chat" action (using a function that creates and navigates)
    def new_chat_action():
        _create_new_chat()
        # The chat is created and _navigate_to_chat is set.
        # We need to rerun so navigation picks up the new chat.
        st.rerun()

    chat_pages.append(
        st.Page(
            new_chat_action,
            title="New Chat",
            icon=":material/add:",
            url_path="new-chat",
        )
    )

    # Existing chats
    chats: dict[str, ChatSession] = st.session_state.get("chats", {})
    target_chat_page: st.Page | None = None

    if chats:
        # Sort by creation time, newest first
        sorted_chats = sorted(chats.values(), key=lambda c: c.created_at, reverse=True)

        for chat in sorted_chats:
            # Create a page for each chat
            # Use a closure to capture the chat_id
            def make_chat_page(chat_id: str):
                def page_fn():
                    st.session_state.current_chat_id = chat_id
                    render_chat_page()

                return page_fn

            title = chat.display_title

            # Check if this is the chat we should navigate to
            is_target = navigate_to_chat == chat.id

            page = st.Page(
                make_chat_page(chat.id),
                title=title,
                icon="💬",
                url_path=f"chat-{chat.id}",  # Flat path (no nested paths allowed)
                default=is_target,  # Make this the default if we're navigating to it
            )
            chat_pages.append(page)

            if is_target:
                target_chat_page = page

    # Welcome page (default only if we're not navigating to a chat)
    welcome_page = st.Page(
        render_welcome_page,
        title="Home",
        icon="🏠",
        url_path="welcome",
        default=(navigate_to_chat is None),
    )

    # Store welcome page in session state for navigation
    st.session_state._page_welcome = welcome_page

    # Build navigation with sections
    pages = {
        "": [welcome_page],  # Empty string = no header
        "Settings": settings_pages,
        "Chats": chat_pages,
    }

    pg = st.navigation(pages)

    # If we have a target chat page and navigation returned it, switch to it
    if target_chat_page is not None:
        st.switch_page(target_chat_page)

    pg.run()


def _get_current_project(project_dir: str) -> DatabaoProject:
    """Get the current DCE project, auto-detecting if needed.

    This is called at app level to determine project status for all pages.
    """
    # 1. Return existing project if available
    if st.session_state.get("databao_project") is not None:
        return st.session_state.databao_project

    project = DatabaoProject(path=Path(project_dir))
    st.session_state.databao_project = project

    return project


def _render_global_sidebar() -> None:
    """Render sidebar elements that appear on all pages.

    This is purely for UI rendering - initialization is handled by _initialize_app().
    """
    from databao.ui.components.sidebar import render_sidebar_header

    with st.sidebar:
        render_sidebar_header()

def main() -> None:
    """Main application entry point."""

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--project-dir", type=str, required=True, help="Location of your Databao project")
    try:
        args = parser.parse_args()
    except SystemExit:
        st.warning("Please provide a valid project directory using -d/--project-dir CLI argument")
        st.stop()

    project_dir = args.project_dir

    init_session_state()
    _initialize_app(project_dir)  # Project + agent initialization
    _render_global_sidebar()  # UI only
    build_navigation()

    # Save settings if changed (at end of main loop)
    _save_settings_if_changed()


if __name__ == "__main__":
    main()
