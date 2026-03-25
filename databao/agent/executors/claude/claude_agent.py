import asyncio
import threading
from concurrent.futures import Future
from pathlib import Path
from typing import Any, TextIO, cast

import anyio
import pandas as pd
from claude_agent_sdk import (
    AgentDefinition,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    HookContext,
    HookInput,
    HookJSONOutput,
    HookMatcher,
    ResultMessage,
    create_sdk_mcp_server,
    tool,
)
from claude_agent_sdk.types import McpSdkServerConfig, SyncHookJSONOutput
from claude_agent_sdk.types import SystemMessage as ClaudeSystemMessage
from langchain_core.messages import AIMessage, BaseMessage

from databao.agent import Domain, ExecutionResult, LLMConfig, Opa
from databao.agent.configs.agent import AgentConfig
from databao.agent.core import Cache
from databao.agent.core.domain import _Domain
from databao.agent.duckdb.react_tools import execute_duckdb_sql
from databao.agent.executors.base import DuckDBExecutor
from databao.agent.executors.claude.memory_manager import MEMORY_FOLDERS, MemoryManager
from databao.agent.executors.claude_code.utils import cast_claude_message_to_langchain_message
from databao.agent.executors.frontend.text_frontend import TextStreamFrontend
from databao.agent.executors.prompt import get_today_date_str, load_prompt_template
from databao.agent.executors.utils import exception_to_string, trim_dataframe_values


class ClaudeAgentExecutor(DuckDBExecutor):
    """Executor which uses Claude to answer questions.
    Depends on DBT project and uses DuckDB as a merged database.
    """

    DISPLAY_ROW_LIMIT = 12
    """Max number of rows to return in SQL tool calls."""

    DISPLAY_CELL_CHAR_LIMIT = 1024
    """Max number of characters a dataframe cell can have before it is trimmed."""

    def __init__(self, writer: Any = None):
        super().__init__(writer=writer)
        self._prompt_template = load_prompt_template("databao.agent.executors.claude", "system_prompt.jinja")
        self._retriever_template = load_prompt_template("databao.agent.executors.claude", "retriever_prompt.jinja")
        self._max_memories = 100

    def register_tools(self, tools: list[Any]) -> None:
        """Register additional tools to be available during execution."""
        # TODO: add to allowed tool list

    def drop_last_opa_group(self, cache: "Cache", n: int = 1) -> None:
        """Drop last n groups of operations from the message history."""
        # TODO: implement

    def _render_system_prompt(
        self,
        memory: MemoryManager,
    ) -> str:
        """Render system prompt."""
        memories_index = memory.read_index()
        prompt = self._prompt_template.render(
            date=get_today_date_str(),
            memory_count=memory.count(),
            memory_limit=memory.max_memories,
            memories_index=memories_index,
        )
        return prompt.strip()

    def _build_mcp_server(
        self,
        memory: MemoryManager,
        results_cache: dict[str, tuple[str, pd.DataFrame]],
        submitted: dict[str, str],
        recursion_limit: int,
        rows_limit: int,
    ) -> McpSdkServerConfig:
        sql_count = 0

        @tool(
            "execute_sql",
            f"""Execute a SQL query on the DuckDB database.
                     Returns a result_id you can use to submit the result.
                     Can be used only {recursion_limit} times.""",
            {"sql": str},
        )
        async def execute_sql(args: dict[str, Any]) -> dict[str, Any]:
            nonlocal sql_count
            if sql_count >= recursion_limit:
                return {
                    "content": [
                        {"type": "text", "text": f"Error: Maximum number of {recursion_limit} sql calls reached."}
                    ]
                }
            sql_count += 1
            try:
                sql = args["sql"]
                df = execute_duckdb_sql(sql, self._duckdb_connection, limit=rows_limit)

                # Limit the size of sampled values to show to avoid context size explosions (e.g., json/binary blobs)
                df_display = df.head(self.DISPLAY_ROW_LIMIT)
                df_display = trim_dataframe_values(df_display, max_cell_chars=self.DISPLAY_CELL_CHAR_LIMIT)

                df_csv = df_display.to_csv(index=False)
                if len(df) > self.DISPLAY_ROW_LIMIT:
                    df_csv += f"\nResult is truncated from {len(df)} to {self.DISPLAY_ROW_LIMIT} rows."
                query_id = str(sql_count)
                results_cache[query_id] = (sql, df)
                return {"content": [{"type": "text", "text": f"Result with ID {query_id} is:\n{df_csv}"}]}
            except Exception as e:
                return {"content": [{"type": "text", "text": f"Error: {exception_to_string(e)}"}]}

        @tool(
            "submit_result",
            """Call this tool with the ID of the query you want to submit to the user.
            This will return control to the user and must always be the last tool call.
            The user will see the full query result, not just the first 12 rows. Returns a confirmation message.

            Args:
                result_id: The ID of the query to submit (result_ids are automatically generated when you execute sql).
                result_description: A comment to a final result. This will be included in the final result.
                visualization_prompt: Optional visualization prompt. If not empty, a Vega-Lite visualization agent
                    will be asked to plot the submitted query data according to instructions in the prompt.
                    The instructions should be short and simple.""",
            {"result_id": str, "result_description": str, "visualization_prompt": str},
        )
        async def submit_result(args: dict[str, Any]) -> dict[str, Any]:
            res_id = args["result_id"]
            if res_id not in results_cache:
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": f"Error: result_id '{res_id}' not found. Use a result_id returned by execute_sql.",
                        }
                    ]
                }
            submitted["result_id"] = res_id
            submitted["result_description"] = args["result_description"]
            submitted["visualization_prompt"] = args["visualization_prompt"]
            return {"content": [{"type": "text", "text": f"Result '{res_id}' submitted successfully."}]}

        @tool(
            "add_memory",
            f"""Save a new memory as a file. Folder must be one of: {", ".join(MEMORY_FOLDERS)}.
                     Use 'general' for broad facts, 'metrics' for metrics with SQLs, 'vocabulary' for business terms,
                     'common_sql' for reusable SQL snippets.""",
            {"name": str, "folder": str, "filename": str, "content": str},
        )
        async def add_memory(args: dict[str, Any]) -> dict[str, Any]:
            result = memory.add(args["name"], args["folder"], args["filename"], args["content"])
            return {"content": [{"type": "text", "text": result}]}

        @tool(
            "delete_memory",
            "Delete a memory by name.",
            {"name": str},
        )
        async def delete_memory(args: dict[str, Any]) -> dict[str, Any]:
            result = memory.delete(args["name"])
            return {"content": [{"type": "text", "text": result}]}

        @tool(
            "update_memory",
            "Update the content of an existing memory by name. Path remains unchanged.",
            {"name": str, "content": str},
        )
        async def update_memory(args: dict[str, Any]) -> dict[str, Any]:
            result = memory.update(args["name"], args["content"])
            return {"content": [{"type": "text", "text": result}]}

        server = create_sdk_mcp_server(
            "tools",
            tools=[
                execute_sql,
                submit_result,
                add_memory,
                delete_memory,
                update_memory,
            ],
        )
        return server

    @staticmethod
    def _resolve_dbt_path(domain: Domain) -> Path:
        domain_obj = cast(_Domain, domain)
        dbts = domain_obj.sources.dbts
        if dbts:
            return next(iter(dbts.values())).dir
        raise ValueError(
            "dbt_path is required for ClaudeAgentExecutor. "
            "Either set it in AgentConfig or register a dbt source in the domain."
        )

    def _build_retriever_agent(self, memory: MemoryManager) -> AgentDefinition:
        memories_index = memory.read_index()
        return AgentDefinition(
            description=(
                "Retrieves relevant schema context before writing SQL. "
                "Explores dbt models, column definitions, metric definitions, docs, and project memories "
                "to return a focused summary of tables, columns, and hints relevant to a given question."
            ),
            prompt=self._retriever_template.render(memories_index=memories_index),
            tools=["Read", "Glob", "Grep"],
        )

    @staticmethod
    def _build_result(
        messages: list[Any],
        results_cache: dict[str, tuple[str, pd.DataFrame]],
        submitted: dict[str, str],
    ) -> ExecutionResult:
        if not submitted:
            if not results_cache:
                sql = None
                df = None
            else:
                sql, df = results_cache[max(list(results_cache.keys()))]
            return ExecutionResult(
                text=(messages[-1].result if isinstance(messages[-1], ResultMessage) else "") if messages else "",
                code=sql,
                df=df,
                meta={
                    ExecutionResult.META_MESSAGES_KEY: messages,
                    "submit_called": False,
                },
            )

        result_id = submitted["result_id"]
        visualization_prompt = submitted.get("visualization_prompt", "")
        sql, df = results_cache[result_id]

        return ExecutionResult(
            text=submitted["result_description"],
            code=sql,
            df=df,
            meta={
                "visualization_prompt": visualization_prompt,
                ExecutionResult.META_MESSAGES_KEY: messages,
                "submit_called": True,
            },
        )

    @staticmethod
    def _load_state(opas: list[Opa], cache: Cache) -> tuple[str, str | None, list[BaseMessage]]:
        """Build the query and read prior session_id and messages from cache."""
        query = "\n\n".join(opa.query for opa in opas)
        state = cache.get("state", {})
        session_id: str | None = state.get("session_id")
        prior_messages: list[BaseMessage] = state.get("messages", [])
        return query, session_id, prior_messages

    @staticmethod
    def _save_state(
        cache: Cache, prior_messages: list[BaseMessage], new_messages: list[BaseMessage], session_id: str | None
    ) -> None:
        """Persist accumulated messages and session_id to cache."""
        cache.put("state", {"messages": prior_messages + new_messages, "session_id": session_id})

    def _build_options(
        self,
        *,
        dbt_path: Path,
        memory: MemoryManager,
        server: Any,
        submit_event: anyio.Event,
        session_id: str | None = None,
    ) -> ClaudeAgentOptions:
        async def stop_after_submit(
            input_data: HookInput, tool_use_id: str | None, context: HookContext
        ) -> HookJSONOutput:
            submit_event.set()
            return SyncHookJSONOutput()

        return ClaudeAgentOptions(
            cwd=str(dbt_path),
            resume=session_id,
            permission_mode="bypassPermissions",
            allowed_tools=[
                "Read",
                "Glob",
                "Grep",
                "Agent",
                "TodoWrite",
                "mcp__tools__execute_sql",
                "mcp__tools__submit_result",
                "mcp__tools__add_memory",
                "mcp__tools__delete_memory",
                "mcp__tools__update_memory",
            ],
            disallowed_tools=[
                "Bash",
                "Write",
                "Edit",
                "NotebookEdit",
                "WebFetch",
                "WebSearch",
                "AskUserQuestion",
                "EnterWorktree",
                "EnterPlanMode",
                "ExitPlanMode",
                "TaskCreate",
                "TaskUpdate",
                "TaskGet",
                "TaskList",
                "TaskStop",
                "TaskOutput",
            ],
            agents={"schema-and-context-retriever": self._build_retriever_agent(memory)},
            mcp_servers={"tools": server},
            setting_sources=["project"],
            hooks={
                "PostToolUse": [
                    HookMatcher(matcher="mcp__tools__submit_result", hooks=[stop_after_submit]),
                ]
            },
            system_prompt=self._render_system_prompt(memory),
        )

    async def _ask_async(
        self,
        question: str,
        dbt_path: Path,
        memory: MemoryManager,
        recursion_limit: int = 25,
        rows_limit: int = 100,
        writer: TextIO | None = None,
        stream: bool = True,
        session_id: str | None = None,
    ) -> tuple[ExecutionResult, str | None, list[BaseMessage]]:
        results_cache: dict[str, tuple[str, pd.DataFrame]] = {}
        submitted: dict[str, str] = {}
        submit_event = anyio.Event()

        server = self._build_mcp_server(memory, results_cache, submitted, recursion_limit, rows_limit)
        options = self._build_options(
            dbt_path=dbt_path, memory=memory, server=server, submit_event=submit_event, session_id=session_id
        )
        new_session_id: str | None = None
        message_log: list[BaseMessage] = []
        frontend = TextStreamFrontend({"messages": message_log}, writer=writer)
        messages = []
        async with ClaudeSDKClient(options=options) as client:
            await client.query(question)
            async for message in client.receive_response():
                messages.append(message)
                if isinstance(message, ClaudeSystemMessage) and new_session_id is None:
                    new_session_id = message.data.get("session_id")
                if not isinstance(message, ResultMessage):
                    lc_message = cast_claude_message_to_langchain_message(message)
                    message_log.append(lc_message)
                    if stream:
                        if isinstance(lc_message, AIMessage):
                            frontend.write_full_ai_message(lc_message)
                        frontend.write_stream_chunk("values", {"messages": message_log})
                if submit_event.is_set():
                    break
        if stream:
            frontend.end()

        return self._build_result(messages, results_cache, submitted), new_session_id, message_log

    @staticmethod
    def _has_running_loop() -> bool:
        try:
            asyncio.get_running_loop()
            return True
        except RuntimeError:
            return False

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
        dbt_path = self._resolve_dbt_path(domain)
        query, prior_session_id, prior_messages = self._load_state(opas, cache)
        memory = MemoryManager(dbt_path, max_memories=self._max_memories)
        args = (query, dbt_path, memory, agent_config.recursion_limit, rows_limit, writer, stream, prior_session_id)

        if self._has_running_loop():
            # Running inside an existing event loop (e.g. Jupyter notebook) — run in a
            # fresh thread that has no event loop, so anyio can create one cleanly.
            future: Future[tuple[ExecutionResult, str | None, list[BaseMessage]]] = Future()

            def _run() -> None:
                try:
                    future.set_result(anyio.run(self._ask_async, *args))
                except Exception as exc:
                    future.set_exception(exc)

            threading.Thread(target=_run, daemon=True).start()
            result, new_session_id, new_messages = future.result()
        else:
            result, new_session_id, new_messages = anyio.run(self._ask_async, *args)

        self._save_state(cache, prior_messages, new_messages, new_session_id)
        return result
