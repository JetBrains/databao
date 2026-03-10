import asyncio
import json
import logging
import queue
import threading
from collections.abc import Generator
from io import StringIO
from pathlib import Path
from typing import Any, TextIO

import pandas as pd
from _duckdb import DuckDBPyConnection
from claude_agent_sdk import (
    ClaudeAgentOptions,
    ClaudeSDKClient,
    SdkMcpTool,
    UserMessage,
    create_sdk_mcp_server,
    tool,
)
from claude_agent_sdk.types import McpSdkServerConfig, ToolResultBlock
from claude_agent_sdk.types import Message as ClaudeMessage
from claude_agent_sdk.types import SystemMessage as ClaudeSystemMessage
from langchain_core.messages import AIMessage, AIMessageChunk, BaseMessage, ToolMessage

from databao.agent.configs.llm import LLMConfig
from databao.agent.core.executor import ExecutionResult
from databao.agent.executors.claude_code.utils import cast_claude_message_to_langchain_message
from databao.agent.executors.frontend.text_frontend import TextStreamFrontend
from databao.agent.executors.lighthouse.graph import RUN_SQL_QUERY_TOOL_DESCRIPTION
from databao.agent.executors.utils import run_sql_query

_LOGGER = logging.getLogger(__name__)
_DEFAULT_MAX_TURNS = 100

_QUESTION_TEMPLATE_STR = """{% filter replace("\n", " ") %}
{{ question }}
{% if mcp_prompt_name %}
{{ mcp_prompt_name }}{% if mcp_prompt_inputs %} "{{ mcp_prompt_inputs|join(\'" "\') }}"{% endif %}
{% endif %}
{% if mcp_resources_urls %}
ReadMcpResourceTool {{ mcp_resources_urls|join(" ") }}
{% endif %}
{% endfilter %}
"""


class ClaudeModelWrapper:
    DISPLAY_ROW_LIMIT = 12
    """Max number of rows to return in SQL tool calls."""

    DISPLAY_CELL_CHAR_LIMIT = 1024
    """Max number of characters a dataframe cell can have before it is trimmed."""

    SQL_ROW_LIMIT = None
    """Max number of rows to return in SQL tool calls."""

    __runtime_mcp_server: McpSdkServerConfig | None = None

    def __init__(self, *, config: LLMConfig, connection: DuckDBPyConnection):
        self._duckdb_connection = connection
        self.config = config
        self.sdk_mcp_tools = self._build_tools()
        self._tool_server_name = Path(__file__).stem + "_mcp_server"
        self.mcp_tool_names = [f"mcp__{self._tool_server_name}__{t.name}" for t in self.sdk_mcp_tools]

        self.options = ClaudeAgentOptions(
            max_turns=_DEFAULT_MAX_TURNS,
            cwd=".",
            allowed_tools=self.mcp_tool_names,
            model=self.config.name,
            mcp_servers={self._tool_server_name: self._build_tool_server()},
            permission_mode="acceptEdits",
        )
        self.client = ClaudeSDKClient(options=self.options)
        self._query_cache: dict[int, tuple[str, str]] = {}
        self._ready_event: threading.Event
        self._exit_event: asyncio.Event

    def __enter__(self) -> "ClaudeModelWrapper":
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True, name=f"{self._tool_server_name}")
        self._thread.start()

        self._ready_event = threading.Event()

        async def _lifecycle() -> None:
            self._exit_event = asyncio.Event()
            async with self.client:
                self._ready_event.set()
                await self._exit_event.wait()

        self._lifecycle_task = asyncio.run_coroutine_threadsafe(_lifecycle(), self._loop)
        self._ready_event.wait()
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> None:
        self._loop.call_soon_threadsafe(self._exit_event.set)
        self._lifecycle_task.result()
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()

    def _build_tools(self) -> list[SdkMcpTool[Any]]:
        tools = []

        @tool("run_sql_query", RUN_SQL_QUERY_TOOL_DESCRIPTION, {"sql": str})
        async def _run_sql_query(args: dict[str, Any]) -> dict[str, Any]:
            result = run_sql_query(
                args.get("sql", ""),
                con=self._duckdb_connection,
                sql_row_limit=self.SQL_ROW_LIMIT,
                display_row_limit=self.DISPLAY_ROW_LIMIT,
                display_cell_char_limit=self.DISPLAY_CELL_CHAR_LIMIT,
            )
            if "error" in result:
                return {"content": [{"type": "text", "text": json.dumps(result, default=str)}]}

            result_to_return = {"sql": args.get("sql", ""), "csv": result.get("csv", "")}

            if (sql := result.get("sql")) and (csv := result.get("df")):
                query_id = len(self._query_cache) + 1
                self._query_cache[query_id] = sql, csv
                result_to_return |= {"query_id": query_id}

            return {"content": [{"type": "text", "text": json.dumps(result_to_return, default=str)}]}

        tools.append(_run_sql_query)

        @tool(
            "submit_query_id",
            """This tool call must be the last tool to be called by the model.
              It will provide to the user the generated sql and the output thereof resulting from the query with
               the respective query id. You will find the query ids of the error-free queries in the outputs of
               the run_sql_query tool in the`query_id` key. The `query_id` itself need not be the one of the last
               generated query, it rather needs to reference the query which most closely matches the
               user's question.

               Args:
                    query_id: The ID of the query to submit.
               """,
            {"query": int},
        )
        async def submit_query_id(args: dict[str, Any]) -> dict[str, Any]:
            query_id = args.get("query")
            if query_id not in self._query_cache:
                return {"content": [{"type": "text", "text": json.dumps({"error": f"Query id {query_id} not found"})}]}
            sql, csv = self._query_cache[query_id]
            return {"content": [{"type": "text", "text": json.dumps({"sql": sql, "csv": csv})}]}

        tools.append(submit_query_id)

        return tools

    def _build_tool_server(self) -> McpSdkServerConfig:
        tools = self._build_tools()
        if self.__runtime_mcp_server is None:
            self.__runtime_mcp_server = create_sdk_mcp_server(
                name=self._tool_server_name,
                version="1.0.0",
                tools=tools,
            )
        return self.__runtime_mcp_server

    def _check_mcp_tool_availability(self, first_message: ClaudeMessage) -> None:
        """
        Each conversation begins with an initial init system message. This SystemMessage
        carries the information about the tools available to claude. To prevent
        the system from running with the mcp tools being silently not available, we
        explicitly look for them and raise and error if any of them is missing.
        """
        if not isinstance(first_message, ClaudeSystemMessage):
            raise TypeError(
                f"The first message should be a system message, got {type(first_message)}. "
                "Check if you are actually calling this function on the first message of the conversation."
            )

        if missing_tools := set(self.mcp_tool_names).difference(first_message.data["tools"]):
            raise ValueError(
                f"The following mcp tools are not available: {missing_tools}. "
                "Check the connection to the mcp servers by running /mcp in the claude console."
            )

    def solve(self, prompt: str) -> Generator[ClaudeMessage, None, None]:
        _LOGGER.info(f"Querying {prompt}")

        _sentinel = object()
        q: queue.Queue[Any] = queue.Queue()

        async def _produce() -> None:
            await self.client.query(prompt=prompt)
            messages = self.client.receive_response()
            async for message in messages:
                q.put(message)
            q.put(_sentinel)

        asyncio.run_coroutine_threadsafe(_produce(), self._loop)

        first_message = q.get()
        self._check_mcp_tool_availability(first_message)
        yield first_message
        _LOGGER.info(first_message)

        n_messages = 1
        while (message := q.get()) is not _sentinel:
            _LOGGER.info(message)
            n_messages += 1
            yield message

        _LOGGER.info(f"End of conversation. Got {n_messages} messages.\n\n")

    def ask(
        self,
        prompt: str,
        *,
        stream: bool = False,
        writer: TextIO | None = None,
    ) -> ExecutionResult:
        """
        Iterate through the messages from claude, cast them into BaseMessage
        object so that they are compatible with the Experiment class and pack
        them into a SolverResult object.
        """
        frontend = TextStreamFrontend({"prompt": prompt}, writer=writer)

        message_log: list[BaseMessage] = []
        df_history: list[pd.DataFrame] = []
        sql_history: list[str] = []
        for message in self.solve(prompt):
            langchain_message = cast_claude_message_to_langchain_message(message)

            if isinstance(langchain_message, list):
                message_log.extend(langchain_message)
            else:
                message_log.append(langchain_message)

            _log_message(langchain_message, frontend, stream=stream)

            if not isinstance(message, UserMessage):
                continue

            sql, df = _extract_sql_and_dataframe(message)

            if sql:
                sql_history.append(sql)

            if df is not None:
                df_history.append(df)

        return ExecutionResult(
            text=message_log[-1].content if message_log else "",
            meta={},
            code=sql_history[-1] if df_history else "",
            df=df_history[-1] if df_history else None,
        )


def _log_message(message: BaseMessage | list[BaseMessage], frontend: TextStreamFrontend, stream: bool = False) -> None:
    if not stream:
        return
    if isinstance(message, AIMessage):
        frontend.write_stream_chunk("messages", (AIMessageChunk(content=message.content), {}))
    elif isinstance(message, ToolMessage):
        frontend.write_stream_chunk("values", {"messages": [message]})
    elif isinstance(message, list):
        frontend.write_stream_chunk("values", {"messages": message})


def _extract_sql_and_dataframe(message: UserMessage) -> tuple[str | None, pd.DataFrame | None]:
    for result_block in message.content:
        if not isinstance(result_block, ToolResultBlock):
            continue
        if not isinstance(result_block.content, list) or not result_block.content:
            continue

        last_output = result_block.content[-1]
        if not isinstance(last_output, dict):
            continue

        raw_text = last_output.get("text", "")

        try:
            payload = json.loads(raw_text)
        except json.JSONDecodeError:
            continue

        sql = payload.get("sql")

        df = None
        csv_data = payload.get("csv")
        if csv_data:
            df = pd.read_csv(StringIO(csv_data))

        return sql, df

    return None, None
