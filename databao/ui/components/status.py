"""Status management component with real-time updates.

Uses st.fragment with polling for independent status updates without full app reruns.

Provides:
- AppStatus: Enum for status values
- set_status(): Permanently set status (survives until next call)
- status_context(): Context manager for temporary status (auto-restores on exit)
- render_status_fragment(): Fragment that displays status (polls for changes)
"""

from collections.abc import Generator
from contextlib import contextmanager
from datetime import timedelta
from enum import Enum

import streamlit as st


class AppStatus(str, Enum):
    """Application status values."""

    READY = "ready"
    INITIALIZING = "initializing"
    ERROR = "error"


def _ensure_status_state() -> None:
    """Ensure status-related session state is initialized."""
    if "status_stack" not in st.session_state:
        st.session_state.status_stack = []
    if "app_status" not in st.session_state:
        st.session_state.app_status = AppStatus.INITIALIZING
    if "status_message" not in st.session_state:
        st.session_state.status_message = None


@st.fragment(run_every=timedelta(milliseconds=500))
def render_status_fragment() -> None:
    """Render status as an independent fragment.

    This fragment polls session state every 500ms and updates the display
    without triggering a full app rerun. Call this once in the sidebar.
    """
    _ensure_status_state()

    status_value = st.session_state.get("app_status", AppStatus.INITIALIZING)
    # Handle both enum and string values for backwards compatibility
    if isinstance(status_value, str):
        try:
            status = AppStatus(status_value)
        except ValueError:
            status = AppStatus.INITIALIZING
    else:
        status = status_value

    message = st.session_state.get("status_message")

    if status == AppStatus.READY:
        st.success(message or "Ready", icon="✅")
    elif status == AppStatus.INITIALIZING:
        st.info(message or "Initializing...", icon="⏳")
    elif status == AppStatus.ERROR:
        st.error(message or "Unknown error", icon="❌")


def set_status(status: AppStatus, message: str | None = None) -> None:
    """Set status permanently (survives until next set_status call).

    The status fragment polls session state and will pick up changes
    within ~500ms without requiring a full app rerun.

    Args:
        status: AppStatus enum value (READY, INITIALIZING, ERROR)
        message: Optional message to display. If None, uses default message.
    """
    _ensure_status_state()
    st.session_state.app_status = status
    st.session_state.status_message = message


@contextmanager
def status_context(
    status: AppStatus, message: str | None = None
) -> Generator[None, None, None]:
    """Temporarily set status, restore previous on exit.

    Use this when you want to show a status during an operation
    and automatically restore the previous status when done.

    Args:
        status: AppStatus enum value (READY, INITIALIZING, ERROR)
        message: Optional message to display. If None, uses default message.

    Example:
        with status_context(AppStatus.INITIALIZING, "Loading data..."):
            load_data()
        # Previous status is automatically restored here
    """
    _ensure_status_state()

    # Push current status to stack
    prev_status = st.session_state.get("app_status", AppStatus.INITIALIZING)
    prev_message = st.session_state.get("status_message")
    st.session_state.status_stack.append((prev_status, prev_message))

    # Set new status
    set_status(status, message)
    try:
        yield
    finally:
        # Pop and restore previous status
        if st.session_state.status_stack:
            prev_status, prev_message = st.session_state.status_stack.pop()
            set_status(prev_status, prev_message)
