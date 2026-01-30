"""Welcome page - shown when no specific chat is selected."""

import base64
from pathlib import Path

import streamlit as st

from databao.ui.app import _create_new_chat


def render_welcome_page() -> None:
    """Render the welcome page with overview and quick actions."""
    # Center content with columns
    col1, col2, col3 = st.columns([1, 2, 1])

    with col2:
        # Logo and title
        logo_path = Path(__file__).parent.parent / "assets" / "bao.png"
        if logo_path.exists():
            with open(logo_path, "rb") as f:
                logo_b64 = base64.b64encode(f.read()).decode()
            st.markdown(
                f"""
                <div style="text-align: center; margin-bottom: 2rem;">
                    <img src="data:image/png;base64,{logo_b64}" width="80" height="80">
                    <h1 style="margin-top: 1rem;">Welcome to Databao</h1>
                </div>
                """,
                unsafe_allow_html=True,
            )
        else:
            st.title("Welcome to Databao")

        st.markdown(
            """
            <div style="text-align: center; color: #666; margin-bottom: 2rem;">
                <p style="font-size: 1.1rem;">
                    Your AI-powered data analysis assistant.
                    Ask questions about your data in natural language.
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # Quick stats
        chats = st.session_state.get("chats", {})
        agent = st.session_state.get("agent")

        # Stats row
        stat_col1, stat_col2, stat_col3 = st.columns(3)

        with stat_col1:
            num_chats = len(chats)
            st.metric("Active Chats", num_chats)

        with stat_col2:
            if agent:
                num_sources = len(agent.dbs) + len(agent.dfs)
            else:
                num_sources = 0
            st.metric("Data Sources", num_sources)

        with stat_col3:
            project = st.session_state.get("dce_project")
            status = "Ready" if project and agent else "Not ready"
            st.metric("Status", status)

        st.markdown("---")

        # Quick actions
        st.subheader("Quick Actions")

        action_col1, action_col2 = st.columns(2)

        with action_col1:
            if st.button("💬 Start New Chat", width="stretch", type="primary"):
                _create_new_chat()
                st.rerun()

        with action_col2:
            if st.button("⚙️ Configure Settings", width="stretch"):
                context_settings_page = st.session_state.get("_page_context_settings")
                if context_settings_page:
                    st.switch_page(context_settings_page)

        # Getting started section
        st.markdown("---")
        st.subheader("Getting Started")

        with st.expander("📖 How to use Databao", expanded=False):
            st.markdown(
                """
                1. **Configure your data sources** in Context Settings
                2. **Start a new chat** to begin asking questions
                3. **Ask questions in natural language** - Databao will analyze your data
                4. **View results** as tables, charts, and explanations
                
                **Example questions:**
                - "What tables are in my database?"
                - "Show me the top 10 customers by revenue"
                - "What's the trend in sales over the last month?"
                """
            )

        # Recent chats (if any)
        if chats:
            st.markdown("---")
            st.subheader("Recent Chats")

            # Sort by creation time, newest first
            sorted_chats = sorted(
                chats.values(),
                key=lambda c: c.created_at,
                reverse=True,
            )[:5]  # Show max 5

            for chat in sorted_chats:
                col_title, col_action = st.columns([4, 1])
                with col_title:
                    st.markdown(f"**{chat.display_title}**")
                    st.caption(chat.created_at.strftime("%b %d, %H:%M"))
                with col_action:
                    if st.button("Open", key=f"open_{chat.id}"):
                        st.session_state.current_chat_id = chat.id
                        # Flag for navigation system to switch to this chat
                        st.session_state._navigate_to_chat = chat.id
                        st.rerun()
