"""Databao viewer module for displaying multimodal tabs in the browser."""

import contextlib
import json
import queue
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import TYPE_CHECKING, Any

from edaplot.data_utils import spec_add_data

from databao.visualizers.vega_chat import VegaChatResult

if TYPE_CHECKING:
    from databao.core.thread import Thread


TEMPLATE_PATH = Path(__file__).parent.parent.parent / "client" / "out" / "multimodal-html" / "index.html"
DATA_PLACEHOLDER = "window.__DATA__ = null;"


class MultimodalHTTPRequestHandler(BaseHTTPRequestHandler):
    """
    Serves:
      - GET / -> redirect to /<html_path>
      - GET /<html_path> -> returns HTML bytes (self.html_bytes)
      - GET /events -> Server-Sent Events that report spec generation progress
    """

    thread: "Thread"
    html_bytes: bytes
    html_path: str

    def do_GET(self) -> None:
        path = self.path.split("?", 1)[0]
        expected = f"/{self.html_path.lstrip('/')}"
        if path == expected:
            self.handle_html()
        elif path == "/events":
            self.handle_events()
        elif path == "/":
            self.send_response(302)
            self.send_header("Location", expected)
            self.end_headers()
        else:
            self.send_error(404, "Not Found")

    def handle_html(self) -> None:
        try:
            body = getattr(self, "html_bytes", b"")
            if not isinstance(body, (bytes, bytearray)):
                body = str(body).encode("utf-8")

            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()

            try:
                self.wfile.write(body)
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                return
        except Exception:
            with contextlib.suppress(Exception):
                self.finish()

    def handle_events(self) -> None:
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache, no-transform")
            self.send_header("Connection", "keep-alive")
            self.end_headers()
        except Exception:
            return

        result_queue: queue.Queue[Any] = queue.Queue()

        def _send_sse(data: dict[str, Any] | None = None, event: str | None = None) -> bool:
            try:
                payload_lines = []
                if event:
                    payload_lines.append(f"event: {event}")
                if data is not None:
                    # SSE 'data:' lines can be multi-line; ensure JSON fits on one line.
                    payload_lines.append(f"data: {json.dumps(data, separators=(',', ':'))}")
                else:
                    payload_lines.append("data: ")
                payload = "\n".join(payload_lines) + "\n\n"
                self.wfile.write(payload.encode("utf-8"))
                self.wfile.flush()
                return True
            except (BrokenPipeError, ConnectionResetError):
                return False
            except Exception:
                return False

        def generate_spec_worker() -> None:
            """
            Worker that computes the visualization spec and puts the spec or an Exception into result_queue.
            """
            try:
                plot = self.thread.plot()

                if not isinstance(plot, VegaChatResult):
                    raise ValueError(f"Plot requires VegaChatVisualizer, got {type(plot).__name__}")

                if plot.spec is None or plot.spec_df is None:
                    raise ValueError("Failed to generate visualization")

                spec_with_data = spec_add_data(plot.spec.copy(), plot.spec_df)

                result_queue.put(spec_with_data)

            except Exception as exc:
                result_queue.put(exc)

        spec_thread = threading.Thread(target=generate_spec_worker, daemon=True)

        """
        The SSE protocol:
        - send repeated {"type":"GENERATE_SPEC","status":"loading",...} while worker is alive
        - on success send {"type":"GENERATE_SPEC","status":"loaded","data": "<json-string>"}
        - on failure send {"type":"GENERATE_SPEC","status":"failed","error": "<message>"}
        - send a named SSE event "close" (no payload) to let client clean up
        """

        try:
            spec_thread.start()

            while spec_thread.is_alive():
                alive = _send_sse(
                    {
                        "type": "GENERATE_SPEC",
                        "status": "loading",
                        "error": "",
                        "data": "",
                    }
                )
                if not alive:
                    return

            try:
                result = result_queue.get_nowait()
            except queue.Empty as err:
                raise RuntimeError("Spec worker finished without producing a result") from err

            if isinstance(result, Exception):
                raise result

            try:
                data_str = json.dumps(result, separators=(",", ":"))
            except Exception as err:
                raise RuntimeError("Spec is not serializable") from err

            _send_sse(
                {
                    "type": "GENERATE_SPEC",
                    "status": "loaded",
                    "error": "",
                    "data": data_str,
                }
            )
        except Exception as exc:
            with contextlib.suppress(Exception):
                _send_sse(
                    {
                        "type": "GENERATE_SPEC",
                        "status": "failed",
                        "error": str(exc),
                        "data": "",
                    }
                )
        finally:
            _send_sse(event="close", data=None)
            threading.Thread(target=self.server.shutdown).start()

    def log_message(self, format: str, *args: Any) -> None:
        return


def _dataframe_to_html(df: "Any") -> str:
    import pandas as pd

    if len(df) > 20:
        first_10 = df.head(10)
        last_10 = df.tail(10)

        separator_data = {col: "..." for col in df.columns}
        separator_df = pd.DataFrame([separator_data], index=["..."])

        truncated_df = pd.concat([first_10, separator_df, last_10])
        html_result = truncated_df.to_html()
    else:
        html_result = df.to_html()

    return html_result if html_result is not None else ""


def open_html_content(thread: "Thread") -> str:
    """Create an HTML file with the embedded Vega spec and open it in the browser.

    This function starts a temporary HTTP server, opens the HTML content in the browser,
    and closes the server after spec generation.

    Args:
        thread: The databao thread.

    Returns:
        The URL that was opened in the browser.

    Raises:
        FileNotFoundError: If the template file is not found.
    """

    if not TEMPLATE_PATH.exists():
        raise FileNotFoundError(
            f"Template file not found at {TEMPLATE_PATH}. "
            "This usually means the frontend wasn't built during installation. "
            "If you installed from pip, please report this as a bug."
        )

    df = thread.df()
    df_html = _dataframe_to_html(df) if df is not None else "<i>No data available</i>"

    data_object = {"text": thread.text(), "dataframeHtmlContent": df_html}
    data_json = json.dumps(data_object)

    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    html = template.replace(DATA_PLACEHOLDER, f"window.__DATA__ = {data_json};")
    html_bytes = html.encode("utf-8")

    MultimodalHTTPRequestHandler.thread = thread
    MultimodalHTTPRequestHandler.html_bytes = html_bytes
    MultimodalHTTPRequestHandler.html_path = _generate_short_id()

    server = HTTPServer(("127.0.0.1", 0), MultimodalHTTPRequestHandler)
    port = server.server_port
    url = f"http://127.0.0.1:{port}/"

    def run_server_and_cleanup() -> None:
        server.serve_forever()
        server.server_close()

    server_thread = threading.Thread(target=run_server_and_cleanup)
    server_thread.start()

    webbrowser.open(url, new=2, autoraise=True)

    return url


def _generate_short_id() -> str:
    import uuid

    return uuid.uuid4().hex[:8]
