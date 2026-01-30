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

from databao.mcp.thread_storage import SQLiteThreadStorage, ThreadState, ThreadStorage

VIEW_URI = "ui://databao/visualizer.html"
HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "3001"))

HTML_PATH = Path(__file__).parent.parent.parent / "client" / "out" / "multimodal-mcp-ui" / "index.html"

STORAGE_DIR = Path(os.environ.get("DATABAO_STORAGE_DIR", Path.home() / ".local" / "share" / "databao"))
DB_PATH = STORAGE_DIR / "mcp_threads.db"

mcp = FastMCP("Databao Visualizer", stateless_http=True)

_thread_storage: ThreadStorage = SQLiteThreadStorage(DB_PATH)


def _store_thread_state(
    thread_id: str,
    thread: Any,
    query: str,
    data: list[dict] | None,
    database_url: str | None,
    context: str | None,
) -> None:
    """Store minimal thread state for later plot generation."""
    df = thread.df()
    df_json = df.to_json(orient="split", date_format="iso") if df is not None else None

    state = ThreadState(
        thread_id=thread_id,
        df_json=df_json,
        text=thread.text(),
        code=thread.code(),
        original_query=query,
        data=data,
        database_url=database_url,
        context=context,
    )

    _thread_storage.store(state)


def _get_thread_state(thread_id: str) -> ThreadState | None:
    """Retrieve thread state using storage abstraction."""
    return _thread_storage.get(thread_id)


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

        # Generate unique thread ID and store serializable state
        thread_id = str(uuid.uuid4())
        _store_thread_state(thread_id, thread, query, data, database_url, context)

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
        error_data = {"error": error_msg, "text": f"Error: {error_msg}"}
        return [types.TextContent(type="text", text=json.dumps(error_data))]


@mcp.tool()
def generate_spec(thread_id: str) -> list[types.TextContent]:
    """Generate Vega-Lite chart specification from a cached analysis thread.

    This tool is called lazily when the user clicks on the Chart tab to view
    the visualization. It retrieves the analysis thread state from cache,
    recreates the agent with the data, and generates the chart specification.

    Args:
        thread_id: Unique identifier for the cached analysis thread
                   (returned by analyze_data tool)

    Returns:
        Text content with Vega-Lite spec encoded as JSON

    Raises:
        ValueError: If thread_id is not found or has expired
    """
    import json

    try:
        saved_thread_state = _get_thread_state(thread_id)

        if saved_thread_state is None:
            raise ValueError(f"Thread state not found or expired: {thread_id}")

        import pandas as pd
        from edaplot.data_utils import spec_add_data
        from sqlalchemy import create_engine

        from databao import new_agent
        from databao.core.executor import ExecutionResult
        from databao.visualizers.vega_chat import VegaChatResult

        result = {}

        try:
            # Check if spec is already cached
            if saved_thread_state.spec_json:
                result["spec"] = json.loads(saved_thread_state.spec_json)
                json_data = json.dumps(result)
                return [types.TextContent(type="text", text=json_data)]

            # Spec not cached - need to generate it
            # Recreate agent with the original data source
            agent = new_agent()

            if saved_thread_state.data:
                source_df = pd.DataFrame(saved_thread_state.data)
                if not saved_thread_state.context:
                    context_rows = source_df.head(5).to_dict(orient="records")
                    data_context = f"Sample data: {context_rows}"
                else:
                    data_context = saved_thread_state.context
                agent.add_df(source_df, name="data", context=data_context)
            elif saved_thread_state.database_url:
                engine = create_engine(saved_thread_state.database_url)
                agent.add_db(engine, context=saved_thread_state.context)
            else:
                raise ValueError("No data source found in thread state")

            if saved_thread_state.df_json is None:
                result["error"] = "No data available for visualization"
                return [types.TextContent(type="text", text=json.dumps(result))]

            result_df = pd.read_json(saved_thread_state.df_json, orient="split")

            execution_result = ExecutionResult(
                text=saved_thread_state.text,
                code=saved_thread_state.code,
                df=result_df,
                meta={},
            )

            viz_result = agent.visualizer.visualize(
                request=None,
                data=execution_result,
                stream=False,
            )

            if isinstance(viz_result, VegaChatResult) and viz_result.spec and viz_result.spec_df is not None:
                spec_with_data = spec_add_data(viz_result.spec.copy(), viz_result.spec_df)
                result["spec"] = spec_with_data

                saved_thread_state.spec_json = json.dumps(spec_with_data)
                saved_thread_state.spec_df_json = viz_result.spec_df.to_json(orient="split", date_format="iso")
                _thread_storage.store(saved_thread_state)
            else:
                result["error"] = "No chart available for this analysis"

        except Exception as e:
            result["error"] = f"Failed to generate chart: {e!s}"

        json_data = json.dumps(result)
        return [types.TextContent(type="text", text=json_data)]

    except Exception as e:
        error_msg = str(e)
        error_data = {"error": error_msg}
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
