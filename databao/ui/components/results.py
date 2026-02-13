"""Result display component with foldable sections and action buttons."""

from contextlib import suppress
from typing import TYPE_CHECKING, Any

import streamlit as st

from databao.ui.services.chat_persistence import save_current_chat

if TYPE_CHECKING:
    from databao.core.executor import ExecutionResult
    from databao.core.thread import Thread
    from databao.ui.models.chat_session import ChatSession


def _extract_visualization_data(thread: "Thread") -> dict[str, Any] | None:
    """Extract serializable visualization data from thread._visualization_result.

    For VegaChatResult, this extracts the Vega-Lite spec (JSON) and spec_df (DataFrame).
    """
    vis_result = thread._visualization_result
    if vis_result is None:
        return None

    data: dict[str, Any] = {
        "text": vis_result.text,
        "code": vis_result.code,
    }

    # Check if it's a VegaChatResult with spec and spec_df
    if hasattr(vis_result, "spec") and hasattr(vis_result, "spec_df"):
        data["spec"] = vis_result.spec  # dict, JSON-serializable
        data["spec_df"] = vis_result.spec_df  # DataFrame, will be pickled separately

    return data


def render_response_section(text: str, has_visualization: bool) -> None:
    """Render the response text section."""
    # Response is expanded by default if no visualization
    expanded = not has_visualization

    with st.expander("📝 Response", expanded=expanded):
        st.markdown(text)


def render_code_section(code: str) -> None:
    """Render the code section (collapsed by default)."""
    with st.expander("💻 Code", expanded=False):
        # Try to detect SQL
        sql_keywords = ["SELECT", "FROM", "WHERE", "JOIN", "INSERT", "UPDATE", "DELETE"]
        if any(keyword in code.upper() for keyword in sql_keywords):
            st.code(code, language="sql")
        else:
            st.code(code, language="python")


def render_dataframe_section(result: "ExecutionResult", has_visualization: bool) -> None:
    """Render the dataframe section."""
    df = result.df
    if df is None:
        return

    # Expanded if no visualization
    expanded = not has_visualization

    with st.expander(f"📊 Data ({len(df)} rows)", expanded=expanded):
        st.dataframe(df, width="stretch")


def render_visualization_section(thread: "Thread", visualization_data: dict[str, Any] | None = None) -> None:
    """Render the visualization section.

    Follows the same rendering logic as Jupyter notebooks:
    1. Vega-Lite/Altair charts: use st.vega_lite_chart for proper rendering
    2. HTML-capable objects: embed HTML
    3. PIL Images: render as images

    Args:
        thread: The Thread object (may have _visualization_result)
        visualization_data: Optional persisted visualization data (used if thread result is None)
    """
    vis_result = thread._visualization_result

    # If we have persisted visualization_data with spec/spec_df, render directly
    if vis_result is None and visualization_data is not None:
        spec = visualization_data.get("spec")
        spec_df = visualization_data.get("spec_df")
        if spec is not None and spec_df is not None:
            with st.expander("📈 Visualization", expanded=True):
                try:
                    st.vega_lite_chart(spec_df, spec, width="stretch")
                    return
                except Exception as e:
                    st.warning(f"Failed to render persisted visualization: {e}")
                    return
        return

    if vis_result is None:
        return

    with st.expander("📈 Visualization", expanded=True):
        plot = vis_result.plot

        if plot is None:
            st.warning("No visualization generated")
            return

        plot_type = type(plot).__name__

        # First: Try Vega-Lite chart (preferred for VegaChatResult)
        # Use st.vega_lite_chart directly with the spec and DataFrame
        if hasattr(vis_result, "spec") and hasattr(vis_result, "spec_df"):
            spec = vis_result.spec
            spec_df = vis_result.spec_df
            if spec is not None and spec_df is not None:
                try:
                    st.vega_lite_chart(spec_df, spec, width="stretch")
                    return
                except Exception:
                    pass  # Fall through to other methods

        # Second: Try Altair chart directly
        if "altair" in plot_type.lower() or "Chart" in plot_type:
            try:
                st.altair_chart(plot, width="stretch")
                return
            except Exception:
                pass  # Fall through to other methods

        # Third: Try HTML embedding for other interactive objects
        html_content = None

        # Try _repr_mimebundle_ (Vega-Embed compatible objects)
        if hasattr(plot, "_repr_mimebundle_"):
            try:
                bundle = plot._repr_mimebundle_()
                if isinstance(bundle, tuple):
                    format_dict, _metadata = bundle
                else:
                    format_dict = bundle
                if format_dict and "text/html" in format_dict:
                    html_content = format_dict["text/html"]
            except Exception:
                pass

        # Try _repr_html_
        if html_content is None and hasattr(plot, "_repr_html_"):
            with suppress(Exception):
                html_content = plot._repr_html_()

        # Try vis_result._get_plot_html()
        if html_content is None and hasattr(vis_result, "_get_plot_html"):
            with suppress(Exception):
                html_content = vis_result._get_plot_html()

        # Render HTML if we got it
        if html_content:
            st.components.v1.html(html_content, height=500, scrolling=False)
            return

        # Fourth: Check if it's a PIL Image (fallback for static images)
        if "Image" in plot_type or hasattr(plot, "_repr_png_"):
            try:
                st.image(plot)
                return
            except Exception:
                pass

        st.warning(f"Could not render visualization: {plot_type}")


def _get_current_chat() -> "ChatSession | None":
    """Get the current chat session from session state."""
    current_chat_id = st.session_state.get("current_chat_id")
    chats = st.session_state.get("chats", {})
    if current_chat_id and current_chat_id in chats:
        return chats[current_chat_id]
    return None


@st.fragment
def render_visualization_and_actions(
    result: "ExecutionResult",
    chat: "ChatSession",
    message_index: int,
    *,
    is_latest: bool = False,
) -> None:
    """Fragment that renders visualization and action buttons together.

    This is a fragment so that when action buttons trigger updates (e.g., Generate Plot),
    only this fragment reruns, showing the new visualization without a full app rerun.

    The fragment reads has_visualization and visualization_data from chat.messages
    so it can see updates made by button click handlers on fragment rerun.
    """
    # Get fresh chat reference in case it was updated
    current_chat = _get_current_chat()
    if current_chat is None:
        return

    thread = current_chat.thread
    if thread is None:
        return

    # Read current state from chat messages (may be updated by button clicks within this fragment)
    messages = current_chat.messages
    if message_index < len(messages):
        msg = messages[message_index]
        has_visualization = msg.has_visualization
        visualization_data = msg.visualization_data
    else:
        has_visualization = False
        visualization_data = None

    # Visualization section (if visualization exists or we have persisted data)
    if has_visualization or thread._visualization_result is not None or visualization_data is not None:
        render_visualization_section(thread, visualization_data)

    # Action buttons (only for latest message)
    if is_latest:
        _render_and_handle_action_buttons(result, current_chat, message_index, has_visualization)


def _render_and_handle_action_buttons(
    result: "ExecutionResult",
    chat: "ChatSession",
    message_index: int,
    has_visualization: bool,
) -> None:
    """Render action buttons and handle clicks inline.

    Called from within the fragment, so button clicks can trigger fragment-scoped reruns.
    """
    from databao.ui.services import is_query_running

    thread = chat.thread
    if thread is None:
        return

    # Check if we're processing a query - buttons will be disabled
    is_processing = is_query_running(chat)

    has_data = result.df is not None

    # Plot button: only show if we have data to plot (plot requires dataframe)
    # Also check thread._data_result exists - it may be None if we failed to restore
    # the thread state from a persisted chat
    if not (has_data and not has_visualization and thread._data_result is not None):
        return

    # Render button in columns (disabled while processing)
    cols = st.columns(3)  # Extra columns for spacing

    with cols[0]:
        button_key = f"action_generate_plot_{message_index}"
        clicked = st.button("📈 Generate Plot", key=button_key, width="stretch", disabled=is_processing)
        if clicked and not is_processing:
            _handle_generate_plot(chat, message_index)


def _handle_generate_plot(chat: "ChatSession", message_index: int) -> None:
    """Handle Generate Plot button click.

    Called from within a fragment, so st.rerun() will only rerun the fragment.
    """
    thread = chat.thread
    if thread is None:
        return

    with st.spinner("Generating visualization..."):
        try:
            thread.plot()

            messages = chat.messages
            if message_index < len(messages):
                messages[message_index].has_visualization = True
                messages[message_index].visualization_data = _extract_visualization_data(thread)
                save_current_chat()

            # Fragment-scoped rerun to show new visualization
            st.rerun()
        except Exception as e:
            st.error(f"Failed to generate visualization: {e}")


def render_execution_result(
    result: "ExecutionResult",
    chat: "ChatSession",
    message_index: int,
    has_visualization: bool,
    *,
    is_latest: bool = False,
    visualization_data: dict[str, Any] | None = None,
) -> None:
    """Render the complete execution result with all sections."""
    # Response section (always present)
    if result.text:
        render_response_section(result.text, has_visualization)

    # Code section (if code exists)
    if result.code:
        render_code_section(result.code)

    # DataFrame section (if df exists)
    if result.df is not None:
        render_dataframe_section(result, has_visualization)

    # Visualization and action buttons are rendered together in a fragment
    # This allows "Generate Plot" to update the visualization with a fragment-scoped rerun
    render_visualization_and_actions(result, chat, message_index, is_latest=is_latest)
