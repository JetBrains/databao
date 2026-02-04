"""Streaming adapter for capturing TextStreamFrontend output in Streamlit.

This module provides a simple writer that captures streaming output from
Databao executors for display in Streamlit's chat interface.
"""

import io
from collections.abc import Callable


class StreamingWriter(io.StringIO):
    """A StringIO-based writer that notifies on writes.

    This writer captures all output written to it and can optionally
    call a callback on each write for real-time updates.

    Usage:
        writer = StreamingWriter()
        agent = databao.new_agent(writer=writer)
        thread = agent.thread()
        result = thread.ask("...")
        thinking_text = writer.getvalue()
    """

    def __init__(self, on_write: Callable[[str], None] | None = None) -> None:
        """Initialize the streaming writer.

        Args:
            on_write: Optional callback called with the full buffer content
                     whenever new content is written.
        """
        super().__init__()
        self._on_write = on_write

    def write(self, text: str) -> int:
        """Write text to the buffer and notify callback."""
        result = super().write(text)
        if self._on_write and text:
            self._on_write(self.getvalue())
        return result

    def clear(self) -> None:
        """Clear the buffer."""
        self.seek(0)
        self.truncate(0)
