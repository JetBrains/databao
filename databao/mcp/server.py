"""MCP server for Databao with MCP Apps support."""

import os
import sys
import uuid
from pathlib import Path
from typing import Any

import uvicorn
from mcp import types
from mcp.server.fastmcp import FastMCP
from starlette.middleware.cors import CORSMiddleware

VIEW_URI = "ui://databao/visualizer.html"
HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "3001"))

HTML_PATH = Path(__file__).parent.parent.parent / "client" / "out" / "multimodal-mcp-ui" / "index.html"

mcp = FastMCP("Databao Visualizer", stateless_http=True)

# In-memory cache for threads (thread_id -> thread)
# Cache lives for the duration of the MCP session
_thread_cache: dict[str, Any] = {}


def _store_thread(thread: Any) -> str:
    """Store thread in cache and return unique ID."""
    thread_id = str(uuid.uuid4())
    _thread_cache[thread_id] = thread
    return thread_id


def _get_thread(thread_id: str) -> Any | None:
    """Retrieve thread from cache, or None if not found."""
    return _thread_cache.get(thread_id)


@mcp.tool(meta={"ui": {"resourceUri": VIEW_URI}})
def analyze_data(
    query: str,
    data: list[dict] | None = None,
    database_url: str | None = None,
    context: str | None = None,
) -> list[types.TextContent]:
    """Analyze data using natural language queries and generate visualizations.

    This tool uses the Databao AI agent to understand your question, analyze data,
    and automatically create appropriate visualizations. The agent can write SQL,
    perform calculations, and generate charts based on your natural language request.

    CRITICAL REQUIREMENTS:
    1. MUST provide 'query' parameter - the user's natural language question
    2. MUST provide EITHER 'data' OR 'database_url' (but not both)

    HOW IT WORKS:
    - You pass the user's natural language query directly (no need to pre-process)
    - The Databao agent analyzes the data and determines the best approach
    - Agent generates SQL queries (if using database) or pandas operations
    - Agent creates appropriate visualizations automatically
    - Returns text insights, data tables, and interactive charts

    Args:
        query: The user's natural language question about the data.
               Pass the question EXACTLY as the user asked it.

               Examples:
               ✓ "Show me sales by region"
               ✓ "What are the top 10 products by revenue?"
               ✓ "How has the user count changed over time?"
               ✓ "Compare performance across different categories"

               The agent will understand the question and create appropriate
               SQL queries, calculations, and visualizations automatically.

        data: Optional - Array of data objects (dictionaries) to analyze.
              Use this when you have the data directly available.

              Format: [{field1: value1, field2: value2, ...}, ...]

              Examples:
              ✓ [{'category': 'A', 'sales': 100}, {'category': 'B', 'sales': 200}]
              ✓ [{'date': '2024-01', 'revenue': 1000, 'costs': 800}]

        database_url: Optional - Database connection string (SQLAlchemy format).
                      Use this when data is in a database.

                      Examples:
                      ✓ "postgresql://user:pass@host:port/dbname"
                      ✓ "sqlite:///path/to/database.db"
                      ✓ "mysql://user:pass@host:port/dbname"

        context: Optional - Additional context about the data to help the agent
                 understand it better (e.g., "Sales data from Q4 2024").

    Returns:
        Text content with visualization data encoded as JSON, including:
        - text: Natural language insights from the analysis
        - dataframeHtmlContent: HTML table with the data
        - spec: Vega-Lite specification for interactive chart
    """
    import concurrent.futures
    import json
    import sys
    import traceback

    try:
        if not data and not database_url:
            raise ValueError("Must provide either 'data' or 'database_url' parameter")

        if data and database_url:
            raise ValueError("Cannot provide both 'data' and 'database_url' - choose one")

        # Suppress stdout during execution to avoid polluting MCP stdio protocol
        def execute_with_suppressed_output():
            import pandas as pd
            from sqlalchemy import create_engine

            from databao import new_agent

            temp_agent = new_agent()

            # Add data source to agent
            if data:
                df = pd.DataFrame(data)

                # Generate context from first few rows if not provided
                if not context:
                    context_rows = df.head(5).to_dict(orient="records")
                    data_context = f"Sample data: {context_rows}"
                else:
                    data_context = context

                temp_agent.add_df(df, name="data", context=data_context)

            elif database_url:
                engine = create_engine(database_url)
                temp_agent.add_db(engine, context=context)

            thread = temp_agent.thread()

            # Redirect stdout to stderr so MCP protocol isn't polluted
            old_stdout = sys.stdout
            sys.stdout = sys.stderr
            try:
                thread.ask(query)
            finally:
                sys.stdout = old_stdout

            return thread

        # Run in thread pool to avoid blocking/conflicting with existing event loop
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(execute_with_suppressed_output)
            thread = future.result()

        thread_id = _store_thread(thread)
        viz_data = {}
        available_modalities = []

        df = thread.df()
        if df is not None:
            viz_data["dataframeHtmlContent"] = _dataframe_to_html(df)
            available_modalities.append("DATAFRAME")

        text = thread.text()
        if text:
            viz_data["text"] = text
            available_modalities.append("DESCRIPTION")

        # Chart is always available (will be loaded lazily)
        available_modalities.append("CHART")

        viz_data["thread_id"] = thread_id
        viz_data["availableModalities"] = available_modalities

        json_data = json.dumps(viz_data)
        return [types.TextContent(type="text", text=json_data)]

    except Exception as e:
        error_msg = str(e)
        stack_trace = traceback.format_exc()
        error_data = {"error": error_msg, "text": f"Error: {error_msg}", "traceback": stack_trace}
        return [types.TextContent(type="text", text=json.dumps(error_data))]


@mcp.tool()
def generate_spec(thread_id: str) -> list[types.TextContent]:
    """Generate Vega-Lite chart specification from a cached analysis thread.

    This tool is called lazily when the user clicks on the Chart tab to view
    the visualization. It retrieves the analysis thread from cache and generates
    the chart specification.

    Args:
        thread_id: Unique identifier for the cached analysis thread
                   (returned by analyze_data tool)

    Returns:
        Text content with Vega-Lite spec encoded as JSON

    Raises:
        ValueError: If thread_id is not found or has expired
    """
    import json
    import traceback

    try:
        thread = _get_thread(thread_id)
        if thread is None:
            raise ValueError(f"Thread not found or expired: {thread_id}")

        from edaplot.data_utils import spec_add_data

        from databao.visualizers.vega_chat import VegaChatResult

        result = {}

        try:
            plot = thread.plot()
            if isinstance(plot, VegaChatResult) and plot.spec and plot.spec_df is not None:
                spec_with_data = spec_add_data(plot.spec.copy(), plot.spec_df)
                result["spec"] = spec_with_data
            else:
                result["error"] = "No chart available for this analysis"
        except Exception as e:
            result["error"] = f"Failed to generate chart: {e!s}"

        json_data = json.dumps(result)
        return [types.TextContent(type="text", text=json_data)]

    except Exception as e:
        error_msg = str(e)
        stack_trace = traceback.format_exc()
        error_data = {"error": error_msg, "traceback": stack_trace}
        return [types.TextContent(type="text", text=json.dumps(error_data))]


def _dataframe_to_html(df) -> str:
    """Convert DataFrame to HTML (same logic as in jupiter_widget.py)."""
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


@mcp.resource(
    VIEW_URI,
    mime_type="text/html;profile=mcp-app",
)
def view() -> str:
    """View HTML resource for the Databao visualizer."""
    if not HTML_PATH.exists():
        raise FileNotFoundError(f"MCP-UI HTML not found at {HTML_PATH}")
    return HTML_PATH.read_text()


def main() -> None:
    """Main entry point for the MCP server."""
    if "--stdio" in sys.argv or len(sys.argv) == 1:
        mcp.run(transport="stdio")
    else:
        # HTTP mode for testing with basic-host - with CORS
        app = mcp.streamable_http_app()
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["*"],
            allow_headers=["*"],
        )
        print(f"Databao MCP Server listening on http://{HOST}:{PORT}/mcp")
        uvicorn.run(app, host=HOST, port=PORT)


if __name__ == "__main__":
    main()
