"""Entry point for running the Databao MCP visualization server."""

from __future__ import annotations

import argparse
import logging

from databao.multimodal.mcp.viewer import create_mcp_app

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Databao MCP Visualization Server")
    parser.add_argument(
        "--transport",
        choices=["streamable-http", "stdio"],
        default="streamable-http",
        help="Transport to use (default: streamable-http)",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind to (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8765,
        help="Port to listen on (default: 8765)",
    )
    args = parser.parse_args()

    mcp = create_mcp_app()
    mcp.settings.host = args.host
    mcp.settings.port = args.port

    logger.info(
        "Starting Databao MCP server on %s:%d (transport=%s)",
        args.host,
        args.port,
        args.transport,
    )
    mcp.run(transport=args.transport)


if __name__ == "__main__":
    main()
