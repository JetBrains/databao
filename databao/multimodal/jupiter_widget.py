"""Widget module for displaying multimodal content in Jupyter notebooks."""

import json
from collections.abc import Callable
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

import anywidget
import traitlets
from edaplot.data_utils import spec_add_data

from databao.visualizers.vega_chat import VegaChatResult

if TYPE_CHECKING:
    from databao.core.thread import Thread


WIDGET_ESM_PATH = Path(__file__).parent.parent.parent / "client" / "out" / "multimodal-jupiter" / "index.js"
WIDGET_CSS_PATH = Path(__file__).parent.parent.parent / "client" / "out" / "multimodal-jupiter" / "style.css"

DATABAO_REQUEST = "databao_request"
DATABAO_RESPONSE = "databao_response"


class FrontendAction(Enum):
    SELECT_MODALITY = "SELECT_MODALITY"
    INIT_WIDGET = "INIT_WIDGET"


class MultimodalWidget(anywidget.AnyWidget):
    """An anywidget for displaying multimodal content in Jupyter notebooks."""

    _esm = WIDGET_ESM_PATH
    _css = WIDGET_CSS_PATH if WIDGET_CSS_PATH.exists() else None

    status = traitlets.Enum(
        values=["initializing", "initialized", "computating", "computated", "failed"], default_value="initializing"
    ).tag(sync=True)

    thread: "Thread"
    spec = traitlets.Dict(default_value=None, allow_none=True).tag(sync=True)
    text = traitlets.Unicode("").tag(sync=True)
    dataframe_html_content = traitlets.Unicode("").tag(sync=True)

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
        self.on_msg(self._on_frontend_message)

        self._action_handlers: dict[FrontendAction, Callable[[Any], None]] = {
            FrontendAction.SELECT_MODALITY: self._handle_change_tab,
            FrontendAction.INIT_WIDGET: self._handle_init_widget,
        }

    def _handle_change_tab(self, payload: str) -> None:
        if payload == "CHART":
            if self.spec is not None:
                return

            plot = self.thread.plot()

            if not isinstance(plot, VegaChatResult):
                raise ValueError(f"html() requires VegaChatVisualizer, got {type(plot).__name__}")

            if plot.spec is None or plot.spec_df is None:
                raise ValueError("Failed to generate visualization")

            spec_with_data = spec_add_data(plot.spec.copy(), plot.spec_df)
            self.spec = spec_with_data

        elif payload == "DATAFRAME":
            if self.dataframe_html_content != "":
                return

            df = self.thread.df()
            self.dataframe_html_content = df.to_html() if df is not None else "<i>No data</i>"

        elif payload == "DESCRIPTION":
            if self.text != "":
                return

            self.text = self.thread.text()

    def _handle_init_widget(self, payload: Any) -> None:
        del payload
        self.status = "initialized"

    def _respond_with_message(self, message_id: str, success: bool, error: str, action_type_str: str) -> None:
        response = {
            "type": DATABAO_RESPONSE,
            "messageId": message_id,
            "success": success,
            "error": error,
            "action": {"type": action_type_str},
        }
        self.send(response)

    def _on_frontend_message(
        self,
        widget: "MultimodalWidget",
        content: dict[str, Any],
        buffers: list[memoryview],
    ) -> None:
        del widget
        self._handle_frontend_message(content, buffers)

    def _handle_frontend_message(
        self,
        content: dict[str, Any],
        buffers: list[memoryview],
    ) -> None:
        del buffers

        message_id = content.get("messageId")
        if not message_id:
            return

        if content.get("type") != DATABAO_REQUEST:
            self._respond_with_message(message_id, False, "Unknown message event", "")
            return

        action = content.get("action", {})
        action_type_str = action.get("type")

        if not action_type_str:
            self._respond_with_message(message_id, False, "Missing action type", "")
            return

        error = ""
        success = False

        try:
            self.status = "computating"
            action_type = FrontendAction(action_type_str)
            raw_payload = action.get("payload")
            action_payload = json.loads(raw_payload) if isinstance(raw_payload, str) and raw_payload else {}

            handler = self._action_handlers.get(action_type)
            if handler:
                handler(action_payload)
                success = True
            else:
                raise SystemError(f"No handler for action: {action_type.value}")
        except (ValueError, json.JSONDecodeError) as e:
            self.status = "failed"
            error = str(e)
        finally:
            self._respond_with_message(message_id, success, error, action_type_str)
            self.status = "computated"


def create_jupiter_widget(
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
