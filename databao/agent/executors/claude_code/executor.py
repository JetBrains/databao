import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TextIO, cast

import pandas as pd
from claude_agent_sdk import (
    ClaudeAgentOptions,
    ClaudeSDKClient,
    SdkMcpTool,
    create_sdk_mcp_server,
    tool,
)
from claude_agent_sdk.types import McpSdkServerConfig, ResultMessage
from claude_agent_sdk.types import Message as ClaudeMessage
from claude_agent_sdk.types import SystemMessage as ClaudeSystemMessage
from duckdb import DuckDBPyConnection
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from mcp.types import ToolAnnotations

from databao.agent.configs import LLMConfig
from databao.agent.configs.agent import AgentConfig
from databao.agent.core import Cache, Domain, ExecutionResult, Opa
from databao.agent.core.domain import _Domain
from databao.agent.databases.databases import db_type as get_db_type
from databao.agent.executors.base import DuckDBExecutor
from databao.agent.executors.claude_code.utils import cast_claude_message_to_langchain_message
from databao.agent.executors.claude_sdk_bridge import ClaudeSDKBridge
from databao.agent.executors.frontend.messages import get_tool_call
from databao.agent.executors.frontend.text_frontend import TextStreamFrontend
from databao.agent.executors.lighthouse.executor import LighthouseExecutor
from databao.agent.executors.lighthouse.graph import RUN_SQL_QUERY_TOOL_DESCRIPTION
from databao.agent.executors.prompt import build_context_text, get_today_date_str, load_prompt_template
from databao.agent.executors.utils import run_sql_query

_LOGGER = logging.getLogger(__name__)

_TOOL_SERVER_NAME = Path(__file__).stem + "_mcp_server"


@dataclass
class QueryResult:
    sql: str
    df: pd.DataFrame


class ClaudeCodeExecutor(DuckDBExecutor):
    DISPLAY_ROW_LIMIT = 12
    """Max number of rows to return in SQL tool calls."""

    DISPLAY_CELL_CHAR_LIMIT = 1024
    """Max number of characters a dataframe cell can have before it is trimmed."""

    def __init__(self, writer: Any = None) -> None:
        super().__init__(writer=writer)
        self._prompt_template = load_prompt_template("databao.agent.executors.claude_code", "system_prompt.jinja")

        self._max_columns_per_table: int | None = None
        self._max_schema_summary_length: int | None = 250_000  # 1 token ~= 4 characters

    def register_tools(self, tools: list[SdkMcpTool[Any]]) -> None:
        """Register additional tools to be available during execution."""
        # TODO: add to allowed tool list

    def drop_last_opa_group(self, cache: "Cache", n: int = 1) -> None:
        pass

    def render_system_prompt(
        self,
        data_connection: DuckDBPyConnection,
        domain: Domain,
        recursion_limit: int = 50,
    ) -> str:
        """Render system prompt with database schema."""
        domain = cast(_Domain, domain)

        db_types = {}
        for name, source in domain.sources.dbs.items():
            db_type = get_db_type(source.config).full_type
            db_types[name] = db_type

        db_schema = LighthouseExecutor.inspect_database_schema(
            data_connection,
            db_types,
            max_schema_summary_length=self._max_schema_summary_length,
            max_columns_per_table=self._max_columns_per_table,
        )

        sources = domain.sources
        context_text = build_context_text(
            sources,
            df_label_fn=lambda name: f"DF {name} (fully qualified name 'temp.main.{name}')",
        )

        dce_search_enabled = False

        prompt = self._prompt_template.render(
            date=get_today_date_str(),
            db_schema=db_schema,
            context=context_text,
            tool_limit=recursion_limit // 2,
            db_types=db_types,
            dce_search_enabled=dce_search_enabled,
        )

        return prompt.strip()

    def _build_tools(
        self,
        connection: DuckDBPyConnection,
        query_cache: dict[int, QueryResult],
        limit_max_rows: int | None,
    ) -> list[SdkMcpTool[Any]]:
        # Set read only hints to enable parallel tool execution
        # (see https://platform.claude.com/docs/en/agent-sdk/agent-loop#parallel-tool-execution)

        tools: list[SdkMcpTool[Any]] = []

        @tool(
            "run_sql_query",
            RUN_SQL_QUERY_TOOL_DESCRIPTION,
            {"sql": str},
            annotations=ToolAnnotations(readOnlyHint=True),
        )
        async def _run_sql_query(args: dict[str, Any]) -> dict[str, Any]:
            result = run_sql_query(
                args.get("sql", ""),
                con=connection,
                sql_row_limit=limit_max_rows,
                display_row_limit=self.DISPLAY_ROW_LIMIT,
                display_cell_char_limit=self.DISPLAY_CELL_CHAR_LIMIT,
            )
            if "error" in result:
                return {"content": [{"type": "text", "text": json.dumps(result, default=str)}]}

            result_for_llm: dict[str, Any] = {"csv": result.get("csv", "")}

            if (sql := result.get("sql")) and (df := result.get("df")) is not None:
                query_id = len(query_cache) + 1
                query_cache[query_id] = QueryResult(sql=sql, df=df)
                result_for_llm["query_id"] = query_id

            return {"content": [{"type": "text", "text": json.dumps(result_for_llm, default=str)}]}

        tools.append(_run_sql_query)

        visualization_prompt_holder: dict[str, str | None] = {"value": None}

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
            visualization_prompt_holder["value"] = args.get("visualization_prompt")

            if query_id not in query_cache:
                return {"content": [{"type": "text", "text": json.dumps({"error": f"Query id {query_id} not found"})}]}
            return {"content": [{"type": "text", "text": json.dumps({"query_id": query_id})}]}

        tools.append(submit_query_id)

        # Store the holder on self so _process_messages can access it
        self._visualization_prompt_holder = visualization_prompt_holder

        return tools

    @staticmethod
    def _build_tool_server(tools: list[SdkMcpTool[Any]]) -> McpSdkServerConfig:
        return create_sdk_mcp_server(
            name=_TOOL_SERVER_NAME,
            version="1.0.0",
            tools=tools,
        )

    @staticmethod
    def _get_full_tool_name(tool_name: str) -> str:
        return f"mcp__{_TOOL_SERVER_NAME}__{tool_name}"

    @staticmethod
    def _check_mcp_tool_availability(first_message: ClaudeMessage, expected_tool_names: list[str]) -> None:
        if not isinstance(first_message, ClaudeSystemMessage):
            raise TypeError(
                f"The first message should be a system message, got {type(first_message)}. "
                "Check if you are actually calling this function on the first message of the conversation."
            )

        if missing_tools := set(expected_tool_names).difference(first_message.data["tools"]):
            raise ValueError(
                f"The following mcp tools are not available: {missing_tools}. "
                "Check the connection to the mcp servers by running /mcp in the claude console."
            )

    @staticmethod
    def _get_tool_query_id_results(message: ToolMessage, query_cache: dict[int, QueryResult]) -> QueryResult | None:
        try:
            payload = json.loads(message.text)
        except json.JSONDecodeError as e:
            _LOGGER.warning("Failed to parse tool call payload: %s", message.text, exc_info=e)
            payload = {}
        query_id = payload.get("query_id")
        if query_id is not None:
            return query_cache.get(query_id)
        return None

    def _process_messages(
        self,
        messages: Any,
        query_cache: dict[int, QueryResult],
        mcp_tool_names: list[str],
        *,
        stream: bool = False,
        writer: TextIO | None = None,
    ) -> tuple[ExecutionResult, str | None]:
        session_id: str | None = None
        max_init_query_id = max(query_cache) if query_cache else 0
        message_log: list[BaseMessage] = []
        submitted_query_result: QueryResult | None = None
        frontend = TextStreamFrontend({"messages": message_log}, writer=writer)
        is_first = True

        for message in messages:
            if is_first:
                self._check_mcp_tool_availability(message, mcp_tool_names)
                is_first = False

            if isinstance(message, ClaudeSystemMessage) and session_id is None:
                # Child subagents have their own system messages, but we want the parent one only
                session_id = message.data.get("session_id", "default")

            # Skip the final text-only ResultMessage, as the previous AssistantMessage already contains the text
            if isinstance(message, ResultMessage):
                continue

            lc_message = cast_claude_message_to_langchain_message(message)

            if isinstance(lc_message, ToolMessage):
                tool_call_info = get_tool_call(message_log, lc_message)
                if tool_call_info is not None:
                    if tool_call_info["name"] == self._get_full_tool_name("run_sql_query"):  # noqa: SIM102
                        if query_result := self._get_tool_query_id_results(lc_message, query_cache):
                            lc_message.artifact = {
                                "sql": query_result.sql,
                                "df": query_result.df,
                            }  # To show when streaming
                    if tool_call_info["name"] == self._get_full_tool_name("submit_query_id"):  # noqa: SIM102
                        if query_result := self._get_tool_query_id_results(lc_message, query_cache):
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
            max_query_id = max(query_cache) if query_cache else 0
            if max_query_id > max_init_query_id:
                submitted_query_result = query_cache[max_query_id]

        visualization_prompt = self._visualization_prompt_holder.get("value")

        return ExecutionResult(
            text=message_log[-1].text if message_log else "",
            meta={
                "visualization_prompt": visualization_prompt,
                ExecutionResult.META_MESSAGES_KEY: message_log,
            },
            code=submitted_query_result.sql if submitted_query_result else "",
            df=submitted_query_result.df if submitted_query_result else None,
        ), session_id

    def _process_opas(self, opas: list[Opa]) -> str:
        query = "\n\n".join(opa.query for opa in opas)
        return query

    def execute(
        self,
        opas: list[Opa],
        cache: Cache,
        llm_config: LLMConfig,
        agent_config: AgentConfig,
        domain: Domain,
        *,
        rows_limit: int = 100,
        stream: bool = True,
        writer: TextIO | None = None,
    ) -> ExecutionResult:
        self._init_sources_from_domain(domain)
        system_prompt = self.render_system_prompt(self._duckdb_connection, domain, agent_config.recursion_limit)

        claude_session_id = cache.get("state").get("claude_session_id")

        query_cache: dict[int, QueryResult] = {}
        sdk_tools = self._build_tools(self._duckdb_connection, query_cache, rows_limit)
        mcp_tool_names = [self._get_full_tool_name(t.name) for t in sdk_tools]
        server = self._build_tool_server(sdk_tools)

        options = ClaudeAgentOptions(
            max_turns=agent_config.recursion_limit,
            cwd=".",
            allowed_tools=mcp_tool_names,
            model=llm_config.name,
            mcp_servers={_TOOL_SERVER_NAME: server},
            permission_mode="acceptEdits",
            resume=claude_session_id,
            system_prompt=system_prompt,
        )
        client = ClaudeSDKClient(options=options)

        with ClaudeSDKBridge(client) as bridge:
            user_query = self._process_opas(opas)
            execution_result, claude_session_id = self._process_messages(
                bridge.query_sync(user_query),
                query_cache,
                mcp_tool_names,
                stream=stream,
                writer=writer,
            )

        cache.put("state", {"claude_session_id": claude_session_id})

        return execution_result
