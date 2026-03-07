"""Converts MCP tools to LangChain BaseTool instances."""

from __future__ import annotations

from typing import Any

from langchain_core.tools import BaseTool, StructuredTool
from mcp.types import TextContent
from mcp.types import Tool as McpTool
from pydantic import BaseModel, Field, create_model

from databao.agent.mcp.connection import McpConnection

_JSON_SCHEMA_TYPE_MAP: dict[str, type[Any]] = {
    "string": str,
    "integer": int,
    "number": float,
    "boolean": bool,
    "array": list,
    "object": dict,
}


def _json_schema_to_pydantic(tool_name: str, schema: dict[str, Any]) -> type[BaseModel]:
    """Build a Pydantic model from a JSON Schema ``properties`` / ``required`` object."""
    properties: dict[str, Any] = schema.get("properties", {})
    required: set[str] = set(schema.get("required", []))

    field_definitions: dict[str, Any] = {}
    for prop_name, prop_schema in properties.items():
        python_type = _resolve_type(prop_schema)
        description = prop_schema.get("description", "")
        if prop_name in required:
            field_definitions[prop_name] = (python_type, Field(description=description))
        else:
            field_definitions[prop_name] = (python_type | None, Field(default=None, description=description))

    model: type[BaseModel] = create_model(f"{tool_name}_args", **field_definitions)
    # Allow extra fields so graph_state injection doesn't break validation
    model.model_config["extra"] = "ignore"
    return model


def _resolve_type(prop_schema: dict[str, Any]) -> type[Any]:
    """Map a JSON Schema type to a Python type.

    Handles ``array`` with nested ``items`` so that the generated Pydantic
    schema always includes a ``type`` key on every node (required by OpenAI).
    """
    json_type = prop_schema.get("type", "string")
    if isinstance(json_type, list):
        non_null = [t for t in json_type if t != "null"]
        json_type = non_null[0] if non_null else "string"

    if json_type == "array":
        items_schema = prop_schema.get("items", {})
        inner = _resolve_type(items_schema) if items_schema else str
        return list[inner]  # type: ignore[valid-type]

    return _JSON_SCHEMA_TYPE_MAP.get(json_type, str)


def _format_tool_result(result: Any) -> str:
    """Extract text from a CallToolResult."""
    if hasattr(result, "content"):
        parts: list[str] = []
        for block in result.content:
            if isinstance(block, TextContent):
                parts.append(block.text)
            else:
                parts.append(str(block))
        text = "\n".join(parts) if parts else "(no output)"
        if hasattr(result, "isError") and result.isError:
            return f"[MCP Error] {text}"
        return text
    return str(result)


def mcp_tools_to_langchain(connection: McpConnection) -> list[BaseTool]:
    """Convert all MCP tools from a connection into LangChain tools."""
    tools: list[BaseTool] = []
    for mcp_tool in connection.tools:
        lc_tool = _make_langchain_tool(mcp_tool, connection)
        tools.append(lc_tool)
    return tools


def _make_langchain_tool(mcp_tool: McpTool, connection: McpConnection) -> BaseTool:
    args_schema = _json_schema_to_pydantic(mcp_tool.name, mcp_tool.inputSchema)

    def call_mcp(**kwargs: Any) -> str:
        schema_properties = mcp_tool.inputSchema.get("properties", {})
        filtered = {k: v for k, v in kwargs.items() if k in schema_properties}
        result = connection.call_tool(mcp_tool.name, filtered)
        return _format_tool_result(result)

    return StructuredTool(
        name=mcp_tool.name,
        description=mcp_tool.description or f"MCP tool: {mcp_tool.name}",
        func=call_mcp,
        args_schema=args_schema,
    )
