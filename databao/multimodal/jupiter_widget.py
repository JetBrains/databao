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

DATABAO_REQUEST_MESSAGE_TYPE = "databao_request"
DATABAO_RESPONSE_MESSAGE_TYPE = "databao_response"


class ClientAction(Enum):
    SELECT_MODALITY = "SELECT_MODALITY"


class MultimodalWidget(anywidget.AnyWidget):
    """An anywidget for displaying multimodal content in Jupyter notebooks."""

    _esm = WIDGET_ESM_PATH
    _css = WIDGET_CSS_PATH if WIDGET_CSS_PATH.exists() else None

    thread: "Thread"

    available_modalities = traitlets.List(["DATAFRAME", "DESCRIPTION", "CHART"]).tag(sync=True)

    spec = traitlets.Dict(default_value=None, allow_none=True).tag(sync=True)
    spec_status = traitlets.Enum(
        values=["initial", "computating", "computated", "failed"], default_value="initial"
    ).tag(sync=True)

    text = traitlets.Unicode("").tag(sync=True)
    text_status = traitlets.Enum(
        values=["initial", "computating", "computated", "failed"], default_value="initial"
    ).tag(sync=True)

    dataframe_html_content = traitlets.Unicode("").tag(sync=True)
    dataframe_html_content_status = traitlets.Enum(
        values=["initial", "computating", "computated", "failed"], default_value="initial"
    ).tag(sync=True)

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
        self.text_status = "computated"

        df = thread.df()
        if df is not None:
            self.dataframe_html_content = self._dataframe_to_html(df)
            self.dataframe_html_content_status = "computated"

        self.on_msg(self._on_client_message)

        self._action_handlers: dict[ClientAction, Callable[[Any], None]] = {
            ClientAction.SELECT_MODALITY: self._handle_change_tab,
        }

    def _dataframe_to_html(self, df: "Any") -> str:
        import pandas as pd

        if len(df) > 20:
            first_10 = df.head(10)
            last_10 = df.tail(10)

            separator_data = {col: "..." for col in df.columns}
            separator_df = pd.DataFrame([separator_data], index=["..."])

            truncated_df = pd.concat([first_10, separator_df, last_10])
            return truncated_df.to_html()
        else:
            return df.to_html()

    def _handle_change_tab(self, payload: str) -> None:
        if payload == "CHART":
            if self.spec_status != "initial":
                return

            self.spec_status = "computating"
            plot = self.thread.plot()

            if not isinstance(plot, VegaChatResult):
                self.spec_status = "failed"
                raise ValueError("Failed to generate visualization")

            if plot.spec is None or plot.spec_df is None:
                self.spec_status = "failed"
                raise ValueError("Failed to generate visualization")

            spec_with_data = spec_add_data(plot.spec.copy(), plot.spec_df)
            self.spec_status = "computated"
            self.spec = spec_with_data

        elif payload == "DATAFRAME":
            if self.dataframe_html_content_status != "initial":
                return

            self.dataframe_html_content_status = "computating"
            df = self.thread.df()

            if df is None:
                self.dataframe_html_content_status = "failed"
                raise ValueError("Failed to generate data")

            self.dataframe_html_content_status = "computated"
            self.dataframe_html_content = self._dataframe_to_html(df)

        elif payload == "DESCRIPTION":
            if self.text_status != "initial":
                return

            self.text_status = "computating"
            prepared_text = self.thread.text()
            self.text = prepared_text
            self.text_status = "computated"

    def _respond_with_message(self, message_id: str, success: bool, error: str, action_type_str: str) -> None:
        response = {
            "type": DATABAO_RESPONSE_MESSAGE_TYPE,
            "messageId": message_id,
            "success": success,
            "error": error,
            "action": {"type": action_type_str},
        }
        self.send(response)

    def _on_client_message(
        self,
        widget: "MultimodalWidget",
        content: dict[str, Any],
        buffers: list[memoryview],
    ) -> None:
        del widget
        self._handle_client_message(content, buffers)

    def _handle_client_message(
        self,
        content: dict[str, Any],
        buffers: list[memoryview],
    ) -> None:
        del buffers

        message_id = content.get("messageId")
        if not message_id:
            self._respond_with_message("", False, "Missing messageId", "")
            return

        if content.get("type") != DATABAO_REQUEST_MESSAGE_TYPE:
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
            action_type = ClientAction(action_type_str)
            raw_payload = action.get("payload")
            action_payload = json.loads(raw_payload) if isinstance(raw_payload, str) and raw_payload else {}

            handler = self._action_handlers.get(action_type)

            if handler:
                handler(action_payload)
                success = True
            else:
                raise SystemError(f"No handler for action: {action_type.value}")
        except (ValueError, json.JSONDecodeError) as e:
            error = str(e)
        finally:
            self._respond_with_message(message_id, success, error, action_type_str)


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
