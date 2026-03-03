"""Databao viewer module for displaying multimodal tabs in the browser."""

from databao.multimodal.html_viewer import open_html_content
from databao.multimodal.mcp.viewer import create_mcp_app

try:
    from databao.multimodal.jupyter_widget import MultimodalWidget, create_jupyter_widget

    __all__ = [
        "MultimodalWidget",
        "create_jupyter_widget",
        "create_mcp_app",
        "open_html_content",
    ]
except ImportError:
    __all__ = [
        "create_mcp_app",
        "open_html_content",
    ]
