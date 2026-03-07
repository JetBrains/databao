"""Widget module for displaying multimodal content in Jupyter notebooks."""

import json
import logging
from collections.abc import Callable
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

try:
    import anywidget
    import traitlets
except ImportError as e:
    raise ImportError(
        "anywidget and traitlets are required for Jupyter notebook support. "
        "Install them with: pip install databao[jupyter]"
    ) from e
from edaplot.data_utils import spec_add_data

from databao.agent.multimodal.utils import dataframe_to_html
from databao.agent.visualizers.vega_chat import VegaChatResult

if TYPE_CHECKING:
    from databao.agent.core.thread import Thread

logger = logging.getLogger(__name__)


_PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
_WIDGET_OUT_DIR = _PROJECT_ROOT / "client" / "out" / "multimodal-jupyter"

WIDGET_ESM_PATH = _WIDGET_OUT_DIR / "index.js"
WIDGET_CSS_PATH = _WIDGET_OUT_DIR / "style.css"

LOADING_STATUS_VALUES = ("initial", "loading", "loaded", "failed")


class ClientAction(Enum):
    SELECT_MODALITY = "SELECT_MODALITY"


class MultimodalWidget(anywidget.AnyWidget):
    """An anywidget for displaying multimodal content in Jupyter notebooks."""

    _esm = WIDGET_ESM_PATH
    _css = WIDGET_CSS_PATH if WIDGET_CSS_PATH.exists() else None

    available_modalities = traitlets.List(["DATAFRAME", "DESCRIPTION", "CHART"]).tag(sync=True)

    spec = traitlets.Dict(default_value=None, allow_none=True).tag(sync=True)
    spec_status = traitlets.Enum(values=LOADING_STATUS_VALUES, default_value="initial").tag(sync=True)

    text = traitlets.Unicode("").tag(sync=True)
    text_status = traitlets.Enum(values=LOADING_STATUS_VALUES, default_value="initial").tag(sync=True)

    dataframe_html_content = traitlets.Unicode("").tag(sync=True)
    dataframe_html_content_status = traitlets.Enum(values=LOADING_STATUS_VALUES, default_value="initial").tag(sync=True)

    def __init__(
        self,
        thread: "Thread",
        **kwargs: Any,
    ) -> None:
        """Initialize the multimodal widget.

        Args:
            thread: The databao thread to interact with.
            **kwargs: Additional arguments passed to the parent class.
        """
        super().__init__(**kwargs)

        self.thread = thread

        thread_text = thread.text()
        self.text = thread_text
        self.text_status = "loaded"

        df = thread.df()
        if df is not None:
            self.dataframe_html_content = dataframe_to_html(df)
            self.dataframe_html_content_status = "loaded"

        self.on_msg(self._on_client_message)

        self._action_handlers: dict[ClientAction, Callable[[Any], None]] = {
            ClientAction.SELECT_MODALITY: self._handle_change_tab,
        }

    def _handle_change_tab(self, payload: str) -> None:
        if payload == "CHART":
            if self.spec_status != "initial":
                return

            self.spec_status = "loading"
            plot = self.thread.plot()

            if not isinstance(plot, VegaChatResult):
                self.spec_status = "failed"
                raise ValueError("Failed to generate visualization")

            if plot.spec is None or plot.spec_df is None:
                self.spec_status = "failed"
                raise ValueError("Failed to generate visualization")

            spec_with_data = spec_add_data(plot.spec.copy(), plot.spec_df)
            self.spec_status = "loaded"
            self.spec = spec_with_data

        elif payload == "DATAFRAME":
            if self.dataframe_html_content_status != "initial":
                return

            self.dataframe_html_content_status = "loading"
            df = self.thread.df()

            if df is None:
                self.dataframe_html_content_status = "failed"
                raise ValueError("Failed to generate dataframe")

            self.dataframe_html_content_status = "loaded"
            self.dataframe_html_content = dataframe_to_html(df)

        elif payload == "DESCRIPTION":
            if self.text_status != "initial":
                return

            self.text_status = "loading"
            prepared_text = self.thread.text()
            self.text = prepared_text
            self.text_status = "loaded"

    def _on_client_message(
        self,
        _widget: "MultimodalWidget",
        content: dict[str, Any],
        _buffers: list[memoryview],
    ) -> None:
        self._handle_client_message(content, _buffers)

    def _handle_client_message(
        self,
        content: dict[str, Any],
        _buffers: list[memoryview],
    ) -> None:
        action = content.get("action", {})
        action_type_str = action.get("type")

        if not action_type_str:
            return

        try:
            action_type = ClientAction(action_type_str)
            handler = self._action_handlers.get(action_type)

            if handler:
                raw_payload = action.get("payload")
                action_payload = json.loads(raw_payload) if isinstance(raw_payload, str) and raw_payload else {}
                handler(action_payload)
            else:
                raise ValueError(f"No handler for action: {action_type.value}")
        except (ValueError, json.JSONDecodeError) as e:
            logger.debug(f"Failed to handle client message: {e}")


def create_jupyter_widget(
    thread: "Thread",
) -> "MultimodalWidget":
    """Create an anywidget for displaying multimodal content in Jupyter notebooks.

    Args:
        thread: The databao thread to interact with.

    Returns:
        A MultimodalWidget instance.

    Raises:
        FileNotFoundError: If the widget ESM file is not found.
    """
    if not WIDGET_ESM_PATH.exists():
        raise FileNotFoundError(
            f"Widget ESM file not found at {WIDGET_ESM_PATH}. "
            "This usually means the frontend wasn't built during installation. "
            "If you installed from pip, please report this as a bug."
        )

    return MultimodalWidget(thread=thread)
