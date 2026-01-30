"""Sidebar component showing project info and sources."""

import streamlit as st

from databao.dce import DCEProject, DCEProjectStatus
from databao.ui.app import _clear_all_chat_threads
from databao.ui.components.status import AppStatus, render_status_fragment, set_status
from databao.ui.suggestions import reset_suggestions_state


@st.dialog("Delete Chat")
def _confirm_delete_chat(chat_id: str, chat_title: str) -> None:
    """Dialog to confirm deleting the current chat."""
    from databao.ui.services.chat_persistence import delete_chat

    st.warning(f"⚠️ Delete chat: **{chat_title}**?")
    st.markdown("This will permanently remove the chat and its history.")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Cancel", use_container_width=True):
            st.rerun()
    with col2:
        if st.button("🗑️ Delete", type="primary", use_container_width=True):
            # Delete chat from disk and memory
            delete_chat(chat_id)

            # Remove from session state
            chats = st.session_state.get("chats", {})
            if chat_id in chats:
                del chats[chat_id]
                st.session_state.chats = chats

            # Clear current chat state
            st.session_state.current_chat_id = None

            # Navigate to home
            st.session_state._navigate_to_chat = None
            st.rerun()

# Icons for different database types
DB_ICONS = {
    "duckdb": "🦆",
    "postgres": "🐘",
    "postgresql": "🐘",
    "mysql": "🐬",
    "sqlite": "📦",
    "default": "🗄️",
}

# Available executor types
EXECUTOR_TYPES = {
    "lighthouse": "LighthouseExecutor (recommended)",
    "react_duckdb": "ReactDuckDBExecutor (experimental)",
}


def get_db_icon(db_type: str) -> str:
    """Get icon for database type."""
    return DB_ICONS.get(db_type.lower(), DB_ICONS["default"])


def render_project_info(project: DCEProject | None) -> None:
    """Render project information section with Reload button."""
    st.markdown("### 📊 Project")

    if project is None:
        st.caption("No project selected")
        # Show Reload button even with no project
        if st.button("🔄 Reload", width="stretch", help="Reload DCE project"):
            st.session_state.dce_project = None
            st.session_state.agent = None
            _clear_all_chat_threads()
            set_status(AppStatus.INITIALIZING, "Reloading...")
            reset_suggestions_state()
            st.rerun()
        return

    st.markdown(f"**{project.name}**")
    st.caption(str(project.path))

    # Status indicator
    if project.status == DCEProjectStatus.VALID:
        st.success("✓ Ready", icon="✅")
        if project.latest_run:
            st.caption(f"Build: {project.latest_run}")
    elif project.status == DCEProjectStatus.NO_BUILD:
        st.warning("Build required", icon="⚠️")
    else:
        st.error("Not found", icon="❌")

    # Reload button at bottom of Project section
    if st.button("🔄 Reload", width="stretch", help="Reload DCE project"):
        # Clear project object but keep dce_project_path so it reloads from same location
        st.session_state.dce_project = None
        st.session_state.agent = None
        _clear_all_chat_threads()
        set_status(AppStatus.INITIALIZING, "Reloading project...")
        # Reset suggestions so they get regenerated with new agent
        reset_suggestions_state()
        st.rerun()


def render_sources_info() -> None:
    """Render connected sources section."""
    st.markdown("### 🔗 Sources")

    agent = st.session_state.get("agent")
    if agent is None:
        st.caption("No sources connected")
        return

    # Get databases
    dbs = agent.dbs
    dfs = agent.dfs

    if not dbs and not dfs:
        st.caption("No sources configured")
        return

    # List databases
    for name, source in dbs.items():
        # Try to determine DB type from connection
        conn = source.db_connection
        db_type = type(conn).__name__
        if "duckdb" in db_type.lower():
            icon = get_db_icon("duckdb")
            db_type = "DuckDB"
        elif "engine" in db_type.lower():
            # SQLAlchemy engine - try to get dialect
            try:
                dialect = conn.dialect.name
                icon = get_db_icon(dialect)
                db_type = dialect.capitalize()
            except Exception:
                icon = get_db_icon("default")
                db_type = "Database"
        else:
            icon = get_db_icon("default")

        st.markdown(f"{icon} **{name}** ({db_type})")

    # List dataframes
    for name in dfs:
        st.markdown(f"📊 **{name}** (DataFrame)")


def render_executor_selector() -> None:
    """Render executor type selector."""
    st.markdown("### ⚙️ Executor")

    current = st.session_state.get("executor_type", "lighthouse")

    selected = st.selectbox(
        "Executor type",
        options=list(EXECUTOR_TYPES.keys()),
        index=list(EXECUTOR_TYPES.keys()).index(current),
        format_func=lambda x: EXECUTOR_TYPES[x],
        label_visibility="collapsed",
        help="Choose the execution engine for queries",
    )

    if selected != current:
        st.session_state.executor_type = selected
        # Reset agent when executor changes
        st.session_state.agent = None
        _clear_all_chat_threads()
        set_status(AppStatus.INITIALIZING, "Applying executor change...")
        st.rerun()


def render_sidebar_header() -> None:
    """Render shared sidebar header (logo + status).

    This is called on ALL pages from app.py to show consistent branding and status.
    Must be called within st.sidebar context.
    """
    import base64
    from pathlib import Path

    # Header with logo - use HTML for proper vertical alignment
    logo_path = Path(__file__).parent.parent / "assets" / "bao.png"
    if logo_path.exists():
        with open(logo_path, "rb") as f:
            logo_b64 = base64.b64encode(f.read()).decode()
        st.markdown(
            f"""
            <div style="display: flex; align-items: center; gap: 6px;">
                <img src="data:image/png;base64,{logo_b64}" width="32" height="32" style="vertical-align: middle;">
                <span style="font-size: 1.4rem; font-weight: 600; line-height: 32px;">Databao</span>
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.markdown("## Databao")

    st.markdown("")  # Vertical spacing

    # Status (right after header) - uses st.empty() for real-time updates
    render_status_fragment()


def render_sidebar_chat_content(project: DCEProject | None) -> None:
    """Render chat-specific sidebar content.

    This is called only on chat pages to show project info, sources, and executor.
    Must be called within st.sidebar context.
    """
    from databao.ui.models.chat_session import ChatSession

    # Project info (includes Reload button)
    render_project_info(project)

    st.markdown("---")

    # Sources
    render_sources_info()

    st.markdown("---")

    # Executor selector
    render_executor_selector()

    st.markdown("---")

    # Remove chat button (only show if there's a current chat)
    current_chat_id = st.session_state.get("current_chat_id")
    chats: dict[str, ChatSession] = st.session_state.get("chats", {})

    if current_chat_id and current_chat_id in chats:
        chat = chats[current_chat_id]
        # Use custom CSS to make button red
        st.markdown(
            """
            <style>
            div[data-testid="stButton"] button[kind="secondary"]:has(p:contains("Remove")) {
                background-color: #ff4b4b;
                color: white;
                border-color: #ff4b4b;
            }
            div[data-testid="stButton"] button[kind="secondary"]:has(p:contains("Remove")):hover {
                background-color: #ff3333;
                border-color: #ff3333;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )
        if st.button("🗑️ Remove Chat", use_container_width=True, type="secondary"):
            _confirm_delete_chat(current_chat_id, chat.display_title)

    # Footer
    st.markdown("---")
    st.caption("Databao v0.1")
