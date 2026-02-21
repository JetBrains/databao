from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from mcp.types import TextContent
from mcp.types import Tool as McpTool

import databao
from databao.configs import LLMConfigDirectory
from databao.core.agent import Agent
from databao.core.domain import Domain
from databao.mcp.adapter import _format_tool_result, _json_schema_to_pydantic, mcp_tools_to_langchain
from databao.mcp.config import parse_mcp_config
from databao.mcp.connection import McpConnection

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def domain() -> Domain:
    d = databao.domain()
    d.add_source(pd.DataFrame({"x": [1, 2, 3]}))
    return d


def _new_agent(domain: Domain) -> Agent:
    llm_config = LLMConfigDirectory.DEFAULT.model_copy(update={"model_kwargs": {"api_key": "test"}})
    return databao.agent(domain, llm_config=llm_config)


# ---------------------------------------------------------------------------
# JSON-Schema → Pydantic conversion
# ---------------------------------------------------------------------------


class TestJsonSchemaToPydantic:
    def test_basic_properties(self) -> None:
        schema = {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "SQL query"},
                "limit": {"type": "integer", "description": "Max rows"},
            },
            "required": ["query"],
        }
        model = _json_schema_to_pydantic("test_tool", schema)
        assert "query" in model.model_fields
        assert "limit" in model.model_fields

        instance = model(query="SELECT 1")
        assert instance.query == "SELECT 1"  # type: ignore[attr-defined]
        assert instance.limit is None  # type: ignore[attr-defined]

    def test_all_types(self) -> None:
        schema = {
            "type": "object",
            "properties": {
                "s": {"type": "string"},
                "i": {"type": "integer"},
                "f": {"type": "number"},
                "b": {"type": "boolean"},
                "a": {"type": "array"},
                "o": {"type": "object"},
            },
            "required": ["s", "i", "f", "b", "a", "o"],
        }
        model = _json_schema_to_pydantic("all_types", schema)
        instance = model(s="hello", i=42, f=3.14, b=True, a=[1], o={"k": "v"})
        assert instance.s == "hello"  # type: ignore[attr-defined]
        assert instance.i == 42  # type: ignore[attr-defined]

    def test_empty_schema(self) -> None:
        schema: dict[str, Any] = {"type": "object", "properties": {}}
        model = _json_schema_to_pydantic("empty", schema)
        instance = model()
        assert instance is not None

    def test_extra_fields_ignored(self) -> None:
        """MCP tools should not break when extra args (like graph_state) are passed."""
        schema = {
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "required": ["name"],
        }
        model = _json_schema_to_pydantic("flexible", schema)
        instance = model(name="test", graph_state={"messages": []})
        assert instance.name == "test"  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Tool result formatting
# ---------------------------------------------------------------------------


class TestFormatToolResult:
    def test_text_content(self) -> None:
        result = MagicMock()
        result.content = [TextContent(type="text", text="Hello world")]
        result.isError = False
        assert _format_tool_result(result) == "Hello world"

    def test_error_result(self) -> None:
        result = MagicMock()
        result.content = [TextContent(type="text", text="something broke")]
        result.isError = True
        assert "[MCP Error]" in _format_tool_result(result)

    def test_multiple_content_blocks(self) -> None:
        result = MagicMock()
        result.content = [
            TextContent(type="text", text="line 1"),
            TextContent(type="text", text="line 2"),
        ]
        result.isError = False
        formatted = _format_tool_result(result)
        assert "line 1" in formatted
        assert "line 2" in formatted


# ---------------------------------------------------------------------------
# MCP → LangChain adapter
# ---------------------------------------------------------------------------


class TestMcpToolsToLangchain:
    def test_converts_mcp_tool(self) -> None:
        mcp_tool = McpTool(
            name="get_weather",
            description="Get weather for a city",
            inputSchema={
                "type": "object",
                "properties": {
                    "city": {"type": "string", "description": "City name"},
                },
                "required": ["city"],
            },
        )

        mock_conn = MagicMock(spec=McpConnection)
        mock_conn.tools = [mcp_tool]

        mock_result = MagicMock()
        mock_result.content = [TextContent(type="text", text="Sunny, 72F")]
        mock_result.isError = False
        mock_conn.call_tool.return_value = mock_result

        lc_tools = mcp_tools_to_langchain(mock_conn)
        assert len(lc_tools) == 1

        tool = lc_tools[0]
        assert tool.name == "get_weather"
        assert tool.description == "Get weather for a city"

        result = tool.invoke({"city": "Paris"})
        assert result == "Sunny, 72F"
        mock_conn.call_tool.assert_called_once_with("get_weather", {"city": "Paris"})


# ---------------------------------------------------------------------------
# Agent.add_mcp validation
# ---------------------------------------------------------------------------


class TestAgentAddMcpValidation:
    def test_no_args_raises(self, domain: Domain) -> None:
        agent = _new_agent(domain)
        with pytest.raises(ValueError, match="Specify either"):
            agent.add_mcp()

    def test_both_url_and_command_raises(self, domain: Domain) -> None:
        agent = _new_agent(domain)
        with pytest.raises(ValueError, match="not both"):
            agent.add_mcp(url="http://x", command="y")

    @patch("databao.mcp.connection.McpConnection.connect_stdio")
    @patch("databao.mcp.adapter.mcp_tools_to_langchain")
    def test_stdio_transport(
        self,
        mock_to_lc: MagicMock,
        mock_connect: MagicMock,
        domain: Domain,
    ) -> None:
        mock_conn = MagicMock(spec=McpConnection)
        mock_conn.server_name = "stdio:test"
        mock_connect.return_value = mock_conn
        mock_to_lc.return_value = []

        agent = _new_agent(domain)
        agent.add_mcp(command="python", args=["server.py"], env={"KEY": "val"})

        mock_connect.assert_called_once_with("python", args=["server.py"], env={"KEY": "val"})

    @patch("databao.mcp.connection.McpConnection.connect_sse")
    @patch("databao.mcp.adapter.mcp_tools_to_langchain")
    def test_sse_transport(
        self,
        mock_to_lc: MagicMock,
        mock_connect: MagicMock,
        domain: Domain,
    ) -> None:
        mock_conn = MagicMock(spec=McpConnection)
        mock_conn.server_name = "sse:http://x"
        mock_connect.return_value = mock_conn
        mock_to_lc.return_value = []

        agent = _new_agent(domain)
        agent.add_mcp(url="http://localhost:8080/sse")

        mock_connect.assert_called_once_with("http://localhost:8080/sse", headers=None)

    @patch("databao.mcp.connection.McpConnection.connect_streamable_http")
    @patch("databao.mcp.adapter.mcp_tools_to_langchain")
    def test_streamable_http_transport(
        self,
        mock_to_lc: MagicMock,
        mock_connect: MagicMock,
        domain: Domain,
    ) -> None:
        mock_conn = MagicMock(spec=McpConnection)
        mock_conn.server_name = "http:http://x"
        mock_connect.return_value = mock_conn
        mock_to_lc.return_value = []

        agent = _new_agent(domain)
        agent.add_mcp(url="http://localhost:8080/mcp", transport="streamable_http")

        mock_connect.assert_called_once_with("http://localhost:8080/mcp", headers=None)


# ---------------------------------------------------------------------------
# Extra tools reach the executor
# ---------------------------------------------------------------------------


class TestExtraToolsRegistration:
    @patch("databao.mcp.connection.McpConnection.connect_stdio")
    def test_tools_registered_on_executor(self, mock_connect: MagicMock, domain: Domain) -> None:
        mcp_tool = McpTool(
            name="my_tool",
            description="A test tool",
            inputSchema={"type": "object", "properties": {"x": {"type": "string"}}, "required": ["x"]},
        )
        mock_conn = MagicMock(spec=McpConnection)
        mock_conn.server_name = "stdio:test"
        mock_conn.tools = [mcp_tool]

        mock_result = MagicMock()
        mock_result.content = [TextContent(type="text", text="ok")]
        mock_result.isError = False
        mock_conn.call_tool.return_value = mock_result
        mock_connect.return_value = mock_conn

        agent = _new_agent(domain)
        agent.add_mcp(command="test")

        from databao.executors.base import GraphExecutor

        assert isinstance(agent.executor, GraphExecutor)
        assert len(agent.executor._extra_tools) == 1
        assert agent.executor._extra_tools[0].name == "my_tool"


# ---------------------------------------------------------------------------
# Config parsing (parse_mcp_config)
# ---------------------------------------------------------------------------


class TestParseMcpConfig:
    def test_claude_code_format(self) -> None:
        cfg = {
            "mcpServers": {
                "Bright Data": {
                    "command": "npx",
                    "args": ["@brightdata/mcp"],
                    "env": {"API_TOKEN": "tok"},
                }
            }
        }
        servers = parse_mcp_config(cfg)
        assert len(servers) == 1
        assert servers[0]["command"] == "npx"
        assert servers[0]["args"] == ["@brightdata/mcp"]
        assert servers[0]["env"] == {"API_TOKEN": "tok"}
        assert servers[0]["name"] == "Bright Data"

    def test_multiple_servers(self) -> None:
        cfg = {
            "mcpServers": {
                "server_a": {"command": "a", "args": []},
                "server_b": {"url": "http://b/sse"},
            }
        }
        servers = parse_mcp_config(cfg)
        assert len(servers) == 2
        names = {s["name"] for s in servers}
        assert names == {"server_a", "server_b"}

    def test_bare_servers_dict(self) -> None:
        cfg = {
            "my_server": {"command": "python", "args": ["s.py"]},
        }
        servers = parse_mcp_config(cfg)
        assert len(servers) == 1
        assert servers[0]["command"] == "python"
        assert servers[0]["name"] == "my_server"

    def test_single_server_config(self) -> None:
        cfg = {"command": "npx", "args": ["pkg"]}
        servers = parse_mcp_config(cfg)
        assert len(servers) == 1
        assert servers[0]["command"] == "npx"

    def test_single_server_url(self) -> None:
        cfg = {"url": "http://localhost:8080/sse"}
        servers = parse_mcp_config(cfg)
        assert len(servers) == 1
        assert servers[0]["url"] == "http://localhost:8080/sse"

    def test_json_string(self) -> None:
        raw = json.dumps({"mcpServers": {"s": {"command": "x"}}})
        servers = parse_mcp_config(raw)
        assert len(servers) == 1
        assert servers[0]["command"] == "x"

    def test_json_file(self, tmp_path: Path) -> None:
        p = tmp_path / "mcp.json"
        p.write_text(json.dumps({"mcpServers": {"s": {"command": "y"}}}))
        servers = parse_mcp_config(str(p))
        assert len(servers) == 1
        assert servers[0]["command"] == "y"

    def test_invalid_string_raises(self) -> None:
        with pytest.raises(ValueError, match="neither valid JSON"):
            parse_mcp_config("not-json-and-not-a-file")

    def test_unrecognised_dict_raises(self) -> None:
        with pytest.raises(ValueError, match="Unrecognised"):
            parse_mcp_config({"foo": 42})  # type: ignore[arg-type]

    def test_bad_type_raises(self) -> None:
        with pytest.raises(TypeError, match="Expected"):
            parse_mcp_config(42)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Agent.add_mcp with config dict
# ---------------------------------------------------------------------------


class TestAgentAddMcpConfig:
    def test_config_and_kwargs_raises(self, domain: Domain) -> None:
        agent = _new_agent(domain)
        with pytest.raises(ValueError, match="Cannot combine"):
            agent.add_mcp({"mcpServers": {"s": {"command": "x"}}}, command="y")

    @patch("databao.mcp.connection.McpConnection.connect_stdio")
    @patch("databao.mcp.adapter.mcp_tools_to_langchain")
    def test_claude_code_config_connects_all_servers(
        self,
        mock_to_lc: MagicMock,
        mock_connect: MagicMock,
        domain: Domain,
    ) -> None:
        mock_conn = MagicMock(spec=McpConnection)
        mock_conn.server_name = "stdio:test"
        mock_connect.return_value = mock_conn
        mock_to_lc.return_value = []

        agent = _new_agent(domain)
        agent.add_mcp({
            "mcpServers": {
                "server_a": {"command": "a", "args": ["--flag"]},
                "server_b": {"command": "b"},
            }
        })

        assert mock_connect.call_count == 2
        calls = mock_connect.call_args_list
        assert calls[0] == (("a",), {"args": ["--flag"], "env": None})
        assert calls[1] == (("b",), {"args": None, "env": None})


# ---------------------------------------------------------------------------
# New tests for fixed issues
# ---------------------------------------------------------------------------


class TestFormatToolResultSentinel:
    def test_empty_content_returns_sentinel(self) -> None:
        result = MagicMock()
        result.content = []
        result.isError = False
        assert _format_tool_result(result) == "(no output)"


class TestAgentAddMcpTransportValidation:
    def test_invalid_transport_raises(self, domain: Domain) -> None:
        agent = _new_agent(domain)
        with pytest.raises(ValueError, match="Unknown transport"):
            agent.add_mcp(url="http://localhost:8080/mcp", transport="invalid_transport")

    def test_typo_transport_raises(self, domain: Domain) -> None:
        agent = _new_agent(domain)
        with pytest.raises(ValueError, match="Unknown transport"):
            agent.add_mcp(url="http://localhost:8080/mcp", transport="streamable-http")


class TestParseMcpConfigEdgeCases:
    def test_empty_dict_returns_empty_list(self) -> None:
        result = parse_mcp_config({})
        assert result == []

    def test_format3_returns_copy(self) -> None:
        original = {"command": "npx", "args": ["pkg"]}
        servers = parse_mcp_config(original)
        assert len(servers) == 1
        # Mutating the returned dict must not affect the original
        servers[0]["command"] = "changed"
        assert original["command"] == "npx"


class TestAgentToolNameCollision:
    @patch("databao.mcp.connection.McpConnection.connect_stdio")
    def test_tool_name_collision_warns(
        self, mock_connect: MagicMock, domain: Domain, caplog: pytest.LogCaptureFixture
    ) -> None:
        tool_a = McpTool(
            name="shared_tool",
            description="First registration",
            inputSchema={"type": "object", "properties": {}, "required": []},
        )
        tool_b = McpTool(
            name="shared_tool",
            description="Second registration — same name",
            inputSchema={"type": "object", "properties": {}, "required": []},
        )

        mock_conn_a = MagicMock(spec=McpConnection)
        mock_conn_a.server_name = "stdio:server_a"
        mock_conn_a.tools = [tool_a]

        mock_conn_b = MagicMock(spec=McpConnection)
        mock_conn_b.server_name = "stdio:server_b"
        mock_conn_b.tools = [tool_b]

        mock_connect.side_effect = [mock_conn_a, mock_conn_b]

        agent = _new_agent(domain)
        with caplog.at_level(logging.WARNING, logger="databao.core.agent"):
            agent.add_mcp(command="server_a")
            agent.add_mcp(command="server_b")

        assert any("collision" in record.message for record in caplog.records)


class TestAgentMcpServers:
    @patch("databao.mcp.connection.McpConnection.connect_stdio")
    @patch("databao.mcp.adapter.mcp_tools_to_langchain")
    def test_mcp_servers_returns_names(
        self,
        mock_to_lc: MagicMock,
        mock_connect: MagicMock,
        domain: Domain,
    ) -> None:
        mock_to_lc.return_value = []

        conn_a = MagicMock(spec=McpConnection)
        conn_a.server_name = "stdio:a"
        conn_a.tools = []
        conn_b = MagicMock(spec=McpConnection)
        conn_b.server_name = "stdio:b"
        conn_b.tools = []
        mock_connect.side_effect = [conn_a, conn_b]

        agent = _new_agent(domain)
        assert agent.mcp_servers == []

        agent.add_mcp(command="a")
        assert agent.mcp_servers == ["stdio:a"]

        agent.add_mcp(command="b")
        assert set(agent.mcp_servers) == {"stdio:a", "stdio:b"}

    @patch("databao.mcp.connection.McpConnection.connect_stdio")
    @patch("databao.mcp.adapter.mcp_tools_to_langchain")
    def test_duplicate_server_replaces_and_warns(
        self,
        mock_to_lc: MagicMock,
        mock_connect: MagicMock,
        domain: Domain,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        mock_to_lc.return_value = []

        conn_old = MagicMock(spec=McpConnection)
        conn_old.server_name = "stdio:x"
        conn_old.tools = []
        conn_new = MagicMock(spec=McpConnection)
        conn_new.server_name = "stdio:x"
        conn_new.tools = []
        mock_connect.side_effect = [conn_old, conn_new]

        agent = _new_agent(domain)
        agent.add_mcp(command="x")
        with caplog.at_level(logging.WARNING, logger="databao.core.agent"):
            agent.add_mcp(command="x")

        conn_old.close.assert_called_once()
        assert agent.mcp_servers == ["stdio:x"]
        assert any("more than once" in r.message for r in caplog.records)


class TestAgentClose:
    @patch("databao.mcp.connection.McpConnection.connect_stdio")
    def test_close_calls_connection_close(
        self, mock_connect: MagicMock, domain: Domain
    ) -> None:
        mock_conn = MagicMock(spec=McpConnection)
        mock_conn.server_name = "stdio:test"
        mock_conn.tools = []
        mock_connect.return_value = mock_conn

        agent = _new_agent(domain)
        agent.add_mcp(command="test")
        agent.close()

        mock_conn.close.assert_called_once()
        assert agent.mcp_servers == []

