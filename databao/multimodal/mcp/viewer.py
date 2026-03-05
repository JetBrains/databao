"""FastMCP server exposing an interactive data visualization tool with MCP App UI."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.types import TextContent

_FRONTEND_HTML = Path(__file__).parent.parent.parent.parent / "client" / "out" / "multimodal-mcp" / "index.html"

RESOURCE_URI = "ui://databao-viz/view.html"
RESOURCE_MIME_TYPE = "text/html;profile=mcp-app"


def create_mcp_app() -> FastMCP:
    """Create and configure the FastMCP application."""
    mcp = FastMCP(
        "Databao Visualization",
        host="127.0.0.1",
        port=8765,
        stateless_http=True,
    )

    @mcp.tool(
        name="show_visualization",
        description=(
            "Display an interactive data visualization with Chart, Data table, and Description tabs. "
            "Call this after fetching data and generating a Vega-Lite spec. "
            "The spec must be a raw Vega-Lite JSON string — no markdown fences, no extra formatting. "
            "The csv_data must be raw CSV text matching the data the spec references — no markdown fences."
        ),
        meta={"ui": {"resourceUri": RESOURCE_URI}},
    )
    def show_visualization(
        spec: str,
        csv_data: str,
        description: str = "",
    ) -> list[TextContent]:
        """Render a Vega-Lite chart with an accompanying data table and description.

        Args:
            spec: Vega-Lite spec as a JSON string.
            csv_data: CSV text data for the chart and table.
            description: Optional text description shown in the Description tab.
        """
        try:
            spec_obj: dict[str, Any] = json.loads(spec)
        except json.JSONDecodeError as exc:
            raise ValueError(f"spec is not valid JSON: {exc}") from exc

        available_tabs = []
        if csv_data:
            available_tabs.append("DATAFRAME")
        if description:
            available_tabs.append("DESCRIPTION")
        if spec_obj:
            available_tabs.append("CHART")

        payload = json.dumps(
            {
                "spec": spec_obj,
                "csvData": csv_data,
                "description": description,
                "availableTabs": available_tabs,
            }
        )

        return [TextContent(type="text", text=payload)]

    @mcp.resource(
        uri=RESOURCE_URI,
        name="Databao Visualization View",
        mime_type=RESOURCE_MIME_TYPE,
        meta={
            "ui": {
                "csp": {
                    "resourceDomains": ["https://cdn.jsdelivr.net", "blob:", "'wasm-unsafe-eval'"],
                    "connectDomains": ["https://cdn.jsdelivr.net"],
                }
            }
        },
    )
    def get_visualization_view() -> str:
        """Serve the built frontend HTML as the MCP App UI resource."""
        if not _FRONTEND_HTML.exists():
            raise FileNotFoundError(
                f"Frontend HTML not found at {_FRONTEND_HTML}. Run 'pnpm run build' in the client/ directory first."
            )
        return _FRONTEND_HTML.read_text(encoding="utf-8")

    return mcp
