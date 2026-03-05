"""Databao viewer module for displaying multimodal tabs in the browser."""

from databao.agent.multimodal.html_viewer import open_html_content

try:
    from databao.agent.multimodal.jupyter_widget import MultimodalWidget, create_jupyter_widget

    __all__ = [
        "MultimodalWidget",
        "create_jupyter_widget",
        "open_html_content",
    ]
except ImportError:
    __all__ = [
        "open_html_content",
    ]
