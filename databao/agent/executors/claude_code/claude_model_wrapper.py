import asyncio
import json
import logging
import queue
import threading
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TextIO

import pandas as pd
from _duckdb import DuckDBPyConnection
from claude_agent_sdk import (
    ClaudeAgentOptions,
    ClaudeSDKClient,
    SdkMcpTool,
    create_sdk_mcp_server,
    tool,
)
from claude_agent_sdk.types import McpSdkServerConfig, ResultMessage, SystemPromptPreset
from claude_agent_sdk.types import Message as ClaudeMessage
from claude_agent_sdk.types import SystemMessage as ClaudeSystemMessage
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from mcp.types import ToolAnnotations

from databao.agent.configs.llm import LLMConfig
from databao.agent.core import Domain
from databao.agent.core.executor import ExecutionResult
from databao.agent.executors.claude_code.utils import cast_claude_message_to_langchain_message, is_dce_search_enabled
from databao.agent.executors.frontend.messages import get_tool_call
from databao.agent.executors.frontend.text_frontend import TextStreamFrontend
from databao.agent.executors.langchain_tools import SEARCH_CONTEXT_TOOL_DESCRIPTION
from databao.agent.executors.lighthouse.graph import RUN_SQL_QUERY_TOOL_DESCRIPTION
from databao.agent.executors.utils import run_sql_query
from databao.agent.executors.utils import (
    search_context as _search_context,
)

_LOGGER = logging.getLogger(__name__)


@dataclass
class QueryResult:
    sql: str
    df: pd.DataFrame


class ClaudeModelWrapper:
    DISPLAY_ROW_LIMIT = 12
    """Max number of rows to return in SQL tool calls."""

    DISPLAY_CELL_CHAR_LIMIT = 1024
    """Max number of characters a dataframe cell can have before it is trimmed."""

    def __init__(
        self,
        *,
        config: LLMConfig,
        connection: DuckDBPyConnection,
        system_prompt: str,
        domain: Domain,
        append_system_prompt: bool = False,
        session_id: str | None = None,
        limit_max_rows: int | None = None,
        max_turns: int | None = 100,
    ):
        self._duckdb_connection = connection
        self._domain = domain
        self._limit_max_rows = limit_max_rows
        self.config = config
        self.sdk_mcp_tools = self._build_tools()
        self._tool_server_name = Path(__file__).stem + "_mcp_server"
        self.mcp_tool_names = [self._get_full_tool_name(t.name) for t in self.sdk_mcp_tools]

        self.options = ClaudeAgentOptions(
            max_turns=max_turns,
            cwd=".",
            allowed_tools=self.mcp_tool_names,
            model=self.config.name,
            mcp_servers={self._tool_server_name: self._build_tool_server()},
            permission_mode="acceptEdits",
            resume=session_id,
            system_prompt=system_prompt
            if not append_system_prompt
            else SystemPromptPreset(
                type="preset",  # Append to Claude's internal system prompt
                preset="claude_code",
                append=system_prompt,
            ),
        )
        self.client = ClaudeSDKClient(options=self.options)
        self._query_cache: dict[int, QueryResult] = {}
        self._ready_event: threading.Event
        self._exit_event: asyncio.Event
        self._visualization_prompt: str | None = None

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

    def _get_full_tool_name(self, tool_name: str) -> str:
        return f"mcp__{self._tool_server_name}__{tool_name}"

    def _build_tools(self) -> list[SdkMcpTool[Any]]:
        # Set read only hints to enable parallel tool execution
        # (see https://platform.claude.com/docs/en/agent-sdk/agent-loop#parallel-tool-execution)

        tools = []

        @tool(
            "run_sql_query",
            RUN_SQL_QUERY_TOOL_DESCRIPTION,
            {"sql": str},
            annotations=ToolAnnotations(readOnlyHint=True),
        )
        async def _run_sql_query(args: dict[str, Any]) -> dict[str, Any]:
            result = await asyncio.to_thread(
                run_sql_query,
                args.get("sql", ""),
                con=self._duckdb_connection,
                sql_row_limit=self._limit_max_rows,
                display_row_limit=self.DISPLAY_ROW_LIMIT,
                display_cell_char_limit=self.DISPLAY_CELL_CHAR_LIMIT,
            )
            if "error" in result:
                return {"content": [{"type": "text", "text": json.dumps(result, default=str)}]}

            result_for_llm: dict[str, Any] = {"csv": result.get("csv", "")}

            if (sql := result.get("sql")) and (df := result.get("df")) is not None:
                query_id = len(self._query_cache) + 1
                self._query_cache[query_id] = QueryResult(sql=sql, df=df)
                result_for_llm["query_id"] = query_id

            return {"content": [{"type": "text", "text": json.dumps(result_for_llm, default=str)}]}

        tools.append(_run_sql_query)

        @tool(
            "submit_query_id",
            """\
This tool call must be the last tool to be called by the model.
It will provide to the user the generated sql and the output thereof resulting from the query with
the respective query id. You will find the query ids of the error-free queries in the outputs of
the run_sql_query tool in the `query_id` key. The `query_id` itself need not be the one of the last
generated query, it rather needs to reference the query which most closely matches the
user's question.

Args:
query_id: The ID of the query to submit.""",
            {"query_id": int, "visualization_prompt": str},
            annotations=ToolAnnotations(readOnlyHint=True),
        )
        async def submit_query_id(args: dict[str, Any]) -> dict[str, Any]:
            query_id: int | None = args.get("query_id")
            self._visualization_prompt = args.get("visualization_prompt")

            if query_id not in self._query_cache:
                return {"content": [{"type": "text", "text": json.dumps({"error": f"Query id {query_id} not found"})}]}
            return {"content": [{"type": "text", "text": json.dumps({"query_id": query_id})}]}

        tools.append(submit_query_id)

        if is_dce_search_enabled(self._domain):

            @tool(
                "search_context",
                SEARCH_CONTEXT_TOOL_DESCRIPTION,
                {"retrieve_text": str},
                annotations=ToolAnnotations(readOnlyHint=True),
            )
            async def search_context(args: dict[str, Any]) -> dict[str, Any]:
                if retrieve_text := args.get("retrieve_text", ""):
                    dce_output = await asyncio.to_thread(_search_context, retrieve_text, domain=self._domain)  # type: ignore[arg-type]
                    return {
                        "content": [
                            {
                                "type": "text",
                                "text": json.dumps(dce_output),
                            }
                        ]
                    }
                return {"content": [{"type": "text", "text": json.dumps({"error": "No retrieve text provided"})}]}

            tools.append(search_context)
        else:
            raise ValueError(f"Search context tool is not supported for domain type: {type(self._domain)}")

        return tools

    def _build_tool_server(self) -> McpSdkServerConfig:
        return create_sdk_mcp_server(
            name=self._tool_server_name,
            version="1.0.0",
            tools=self.sdk_mcp_tools,
        )

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

    def _get_tool_query_id_results(self, message: ToolMessage) -> QueryResult | None:
        try:
            payload = json.loads(message.text)
        except json.JSONDecodeError as e:
            _LOGGER.warning("Failed to parse tool call payload: %s", message.text, exc_info=e)
            payload = {}
        query_id = payload.get("query_id")
        if query_id is not None:
            return self._query_cache.get(query_id)
        return None

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
    ) -> tuple[ExecutionResult, str | None]:
        """
        Iterate through the messages from claude, cast them into BaseMessage
        object so that they are compatible with the Experiment class and pack
        them into a SolverResult object.
        """
        session_id: str | None = None
        max_init_query_id = max(self._query_cache) if self._query_cache else 0
        message_log: list[BaseMessage] = []
        submitted_query_result: QueryResult | None = None
        frontend = TextStreamFrontend({"messages": message_log}, writer=writer)
        for message in self.solve(prompt):
            if isinstance(message, ClaudeSystemMessage) and session_id is None:
                # Child subagents have their own system messages, but we want the parent one only
                session_id = message.data.get("session_id", "default")

            # Skip the final text-only ResultMessage, as the previous AssistantMessage already contains the text
            # of this message.
            if isinstance(message, ResultMessage):
                continue

            lc_message = cast_claude_message_to_langchain_message(message)

            if isinstance(lc_message, ToolMessage):
                tool_call = get_tool_call(message_log, lc_message)
                if tool_call is not None:
                    if tool_call["name"] == self._get_full_tool_name("run_sql_query"):  # noqa: SIM102
                        if query_result := self._get_tool_query_id_results(lc_message):
                            lc_message.artifact = {
                                "sql": query_result.sql,
                                "df": query_result.df,
                            }  # To show when streaming
                    if tool_call["name"] == self._get_full_tool_name("submit_query_id"):  # noqa: SIM102
                        if query_result := self._get_tool_query_id_results(lc_message):
                            submitted_query_result = query_result

            message_log.append(lc_message)

            if stream:
                if isinstance(lc_message, AIMessage):
                    frontend.write_full_ai_message(lc_message)
                frontend.write_stream_chunk("values", {"messages": message_log})

        if stream:
            frontend.end()

        if submitted_query_result is None:
            # Fallback to the last executed query if no query was submitted
            max_query_id = max(self._query_cache) if self._query_cache else 0
            if max_query_id > max_init_query_id:
                submitted_query_result = self._query_cache[max_query_id]

        return ExecutionResult(
            text=message_log[-1].text if message_log else "",
            meta={
                "visualization_prompt": self._visualization_prompt,
                ExecutionResult.META_MESSAGES_KEY: message_log,
            },
            code=submitted_query_result.sql if submitted_query_result else "",
            df=submitted_query_result.df if submitted_query_result else None,
        ), session_id
