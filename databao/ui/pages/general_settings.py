"""General Settings page - Storage and app-wide configuration."""

import streamlit as st

from databao.ui.app import _clear_all_chat_threads
from databao.ui.services.chat_persistence import delete_all_chats
from databao.ui.services.settings_persistence import delete_settings
from databao.ui.services.storage import get_cache_dir, get_chats_dir, get_storage_base_path


@st.dialog("Clear All Chats")
def _confirm_clear_chats() -> None:
    """Dialog to confirm clearing all chats."""
    st.warning("⚠️ This will permanently delete all chat history and cached data.")
    st.markdown("This action cannot be undone.")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Cancel", use_container_width=True):
            st.rerun()
    with col2:
        if st.button("🗑️ Delete All", type="primary", use_container_width=True):
            # Delete all chats (also deletes diskcache directory)
            deleted = delete_all_chats()
            # Clear session state
            st.session_state.chats = {}
            st.session_state.current_chat_id = None
            # Reset agent AND disk_cache since cache directory was deleted
            st.session_state.agent = None
            st.session_state.disk_cache = None  # Critical: also reset the cache!
            _clear_all_chat_threads()
            st.success(f"Deleted {deleted} chats")
            st.rerun()


@st.dialog("Reset to Defaults")
def _confirm_reset_settings() -> None:
    """Dialog to confirm resetting settings to defaults."""
    st.warning("⚠️ This will reset all settings to their default values.")
    st.markdown("Your chat history will be preserved.")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Cancel", use_container_width=True):
            st.rerun()
    with col2:
        if st.button("🔄 Reset", type="primary", use_container_width=True):
            # Delete settings file
            delete_settings()
            # Clear settings-related session state
            st.session_state.executor_type = "lighthouse"
            st.session_state.databao_project = None
            st.session_state.agent = None
            _clear_all_chat_threads()
            st.session_state.app_settings = None
            st.success("Settings reset to defaults")
            st.rerun()


def render_general_settings_page() -> None:
    """Render the General Settings page."""
    st.title("General Settings")
    st.markdown("Configure application storage and manage data.")

    st.markdown("---")

    # Storage Information
    st.subheader("📁 Storage Location")

    base_path = get_storage_base_path()
    chats_dir = get_chats_dir()
    cache_dir = get_cache_dir()

    st.markdown("**Base Path** (read-only)")
    st.code(str(base_path), language=None)

    with st.expander("📂 Storage Details", expanded=False):
        st.markdown("**Chats Directory**")
        st.code(str(chats_dir), language=None)

        st.markdown("**Cache Directory**")
        st.code(str(cache_dir), language=None)

        # Show storage statistics
        try:
            chats = st.session_state.get("chats", {})
            num_chats = len(chats)
            st.metric("Saved Chats", num_chats)
        except Exception:
            pass

    st.markdown("---")

    # Danger Zone
    st.subheader("⚠️ Data Management")

    st.markdown(
        """
        These actions affect your stored data. Use with caution.
        """
    )

    col1, col2 = st.columns(2)

    with col1:
        if st.button(
            "🗑️ Clear All Chats",
            use_container_width=True,
            type="secondary",
            help="Removes all chat history and cached data.",
        ):
            _confirm_clear_chats()

    with col2:
        if st.button(
            "🔄 Reset to Defaults",
            use_container_width=True,
            type="secondary",
            help="Resets settings but keeps chats.",
        ):
            _confirm_reset_settings()

