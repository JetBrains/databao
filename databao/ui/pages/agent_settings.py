"""Agent Settings page - Executor and LLM configuration."""

import streamlit as st

from databao.ui.app import _clear_all_chat_threads
from databao.ui.components.status import AppStatus, set_status

# Available executor types
EXECUTOR_TYPES = {
    "lighthouse": "LighthouseExecutor (recommended)",
    "react_duckdb": "ReactDuckDBExecutor (experimental)",
}


def render_agent_settings_page() -> None:
    """Render the Agent Settings page."""
    st.title("Agent Settings")
    st.markdown("Configure the AI agent behavior and execution engine.")

    st.markdown("---")

    # Executor selection
    st.subheader("⚙️ Execution Engine")

    st.markdown(
        """
        The executor determines how queries are processed and executed against your data sources.
        """
    )

    current = st.session_state.get("executor_type", "lighthouse")

    selected = st.selectbox(
        "Executor type",
        options=list(EXECUTOR_TYPES.keys()),
        index=list(EXECUTOR_TYPES.keys()).index(current),
        format_func=lambda x: EXECUTOR_TYPES[x],
        help="Choose the execution engine for queries",
    )

    # Show executor description
    if selected == "lighthouse":
        st.info(
            """
            **LighthouseExecutor** is the default and recommended executor.
            It uses a sophisticated graph-based approach with planning and validation steps.
            Best for complex queries requiring multiple steps.
            """,
            icon="💡",
        )
    elif selected == "react_duckdb":
        st.warning(
            """
            **ReactDuckDBExecutor** is experimental.
            It uses a ReAct-style loop optimized for DuckDB queries.
            May be faster for simple queries but less reliable for complex ones.
            """,
            icon="⚠️",
        )

    # Apply button (only shown when selection differs from current)
    if selected != current and st.button("✓ Apply Changes", type="primary"):
        st.session_state.executor_type = selected
        # Reset agent to reinitialize with new executor
        st.session_state.agent = None
        _clear_all_chat_threads()
        set_status(AppStatus.INITIALIZING, "Applying executor change...")
        st.success(f"Executor changed to {EXECUTOR_TYPES[selected]}")
        st.rerun()

    st.markdown("---")

    # LLM Configuration (placeholder for future)
    st.subheader("🤖 Language Model")

    st.info(
        """
        LLM configuration will be available in a future update.
        Currently using the default model configuration.
        """,
        icon="🔮",
    )

    # Show current LLM info if agent exists
    agent = st.session_state.get("agent")
    if agent:
        st.markdown("**Current Configuration:**")
        try:
            llm = agent.llm
            model_name = getattr(llm, "model_name", None) or getattr(llm, "model", "Unknown")
            st.code(f"Model: {model_name}")
        except Exception:
            st.caption("Could not retrieve model information.")

