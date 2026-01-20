"""HTML viewer module for displaying multimodal content in the browser."""

import json
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

from edaplot.spec_utils import SpecType

TEMPLATE_PATH = Path(__file__).parent.parent.parent / "client" / "out" / "multimodal-html" / "index.html"
DATA_PLACEHOLDER = "window.__DATA__ = null;"


def _generate_short_id() -> str:
    """Generate a short unique ID for the URL."""
    import uuid

    return uuid.uuid4().hex[:8]


def open_html_content(spec: SpecType, df_html: str, description: str) -> str:
    """Create an HTML file with the embedded Vega spec and open it in the browser.

    This function starts a temporary HTTP server, opens the HTML content in the browser,
    and closes the server after serving the single request.

    Args:
        spec: A Vega-Lite specification.
        df_html: HTML representation of the DataFrame.
        description: Description to display.

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

    data_object = {"spec": spec, "text": description, "dataframeHtmlContent": df_html}
    data_json = json.dumps(data_object)

    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    html = template.replace(DATA_PLACEHOLDER, f"window.__DATA__ = {data_json};")

    html_bytes = html.encode("utf-8")

    class OneShotRequestHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            try:
                self.send_response(200)
                self.send_header("Content-type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(html_bytes)))
                self.end_headers()

                buffer_size = 1024 * 1024
                for i in range(0, len(html_bytes), buffer_size):
                    self.wfile.write(html_bytes[i : i + buffer_size])
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, format: str, *args: Any) -> None:
            pass

    server = HTTPServer(("127.0.0.1", 0), OneShotRequestHandler)
    url = f"http://127.0.0.1:{server.server_port}/{_generate_short_id()}"

    server.timeout = 60
    webbrowser.open(url, new=2, autoraise=True)
    server.handle_request()
    server.server_close()

    return url
