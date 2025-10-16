from collections.abc import Sequence
from typing import Annotated, Any, Literal, TypedDict

import pandas as pd
from duckdb import DuckDBPyConnection
from langchain_core.language_models import BaseChatModel, LanguageModelInput
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from langchain_core.runnables import Runnable
from langchain_core.tools import BaseTool, tool
from langchain_openai import ChatOpenAI
from langgraph.constants import END, START
from langgraph.graph import add_messages
from langgraph.graph.state import CompiledStateGraph, StateGraph

from portus.agents.lighthouse.utils import exception_to_string
from portus.configs.llm import LLMConfig
from portus.core import ExecutionResult


class AgentState(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]
    query_ids: dict[str, ToolMessage]
    sql: str | None
    df: pd.DataFrame | None
    visualization_prompt: str | None
    ready_for_user: bool


class ExecuteSubmit:
    """Simple graph with two tools: run_sql_query and submit_query_id.
    All context must be in the SystemMessage."""

    MAX_ROWS = 12

    def __init__(self, connection: DuckDBPyConnection):
        self._connection = connection

    def init_state(self, messages: list[BaseMessage]) -> dict[str, Any]:
        state: dict[str, Any] = {
            "messages": messages,
            "query_ids": {},
            "sql": None,
            "df": None,
            "visualization_prompt": None,
            "ready_for_user": False,
        }
        return state

    def get_result(self, state: dict[str, Any]) -> ExecutionResult:
        last_ai_message = None
        for m in reversed(state["messages"]):
            if isinstance(m, AIMessage):
                last_ai_message = m
                break
        if last_ai_message is None:
            raise RuntimeError("No AI message found in message log")
        if len(last_ai_message.tool_calls) == 0:
            content = last_ai_message.content
            text = content if isinstance(content, str) else str(content)
            result = ExecutionResult(text=text, df=None, code="", meta={"messages": state["messages"]})
        elif len(last_ai_message.tool_calls) > 1:
            raise RuntimeError("Expected exactly one tool call in AI message")
        elif last_ai_message.tool_calls[0]["name"] != "submit_query_id":
            raise RuntimeError(
                f"Expected submit_query_id tool call in AI message, got {last_ai_message.tool_calls[0]['name']}"
            )
        else:
            sql = state.get("sql", "")
            df = state.get("df")
            tool_call = last_ai_message.tool_calls[0]
            text = tool_call["args"]["result_description"]
            visualization_prompt = state.get("visualization_prompt", "")
            result = ExecutionResult(
                text=text,
                df=df,
                code=sql,
                meta={"visualization_prompt": visualization_prompt, "messages": state["messages"]},
            )
        return result

    def make_tools(self) -> list[BaseTool]:
        @tool(parse_docstring=True)
        def run_sql_query(sql: str) -> dict[str, Any]:
            """
            Run a SELECT SQL query in the database. Returns the first 12 rows in csv format.

            Args:
                sql: SQL query
            """
            df_or_error = self._connection.execute(sql).df()
            if isinstance(df_or_error, pd.DataFrame):
                df_csv = df_or_error.head(self.MAX_ROWS).to_csv(index=False)
                df_markdown = df_or_error.head(self.MAX_ROWS).to_markdown(index=False)
                if len(df_or_error) > self.MAX_ROWS:
                    df_csv += f"\nResult is truncated from {len(df_or_error)} to {self.MAX_ROWS} rows."
                    df_markdown += f"\nResult is truncated from {len(df_or_error)} to {self.MAX_ROWS} rows."
                return {"df": df_or_error, "sql": sql, "csv": df_csv, "markdown": df_markdown}
            else:
                return {"error": exception_to_string(df_or_error)}

        @tool(parse_docstring=True)
        def submit_query_id(
            query_id: str,
            result_description: str,
            visualization_prompt: str,
        ) -> str:
            """
            Call this tool with the ID of the query you want to submit to the user.
            This will return control to the user and must always be the last tool call.
            The user will see the full query result, not just the first 12 rows. Returns a confirmation message.

            Args:
                query_id: The ID of the query to submit (query_ids are automatically generated when you run queries).
                result_description: A comment to a final result. This will be included in the final result.
                visualization_prompt: Optional visualization prompt. If not empty, a Vega-Lite visualization agent
                    will be asked to plot the submitted query data according to instructions in the prompt.
                    The instructions should be short and simple.
            """
            return f"Query {query_id} submitted successfully. Your response is now visible to the user."

        tools = [run_sql_query, submit_query_id]
        return tools

    def compile(self, model_config: LLMConfig) -> CompiledStateGraph[Any]:
        tools = self.make_tools()
        llm_model = model_config.chat_model

        model_with_tools = self._model_bind_tools(llm_model, tools)

        def llm_node(state: AgentState) -> dict[str, Any]:
            messages = state["messages"]
            response = self._chat(messages, model_config, model_with_tools)
            return {"messages": [response[-1]]}

        def tool_executor_node(state: AgentState) -> dict[str, Any]:
            last_message = state["messages"][-1]
            tool_messages = []
            assert isinstance(last_message, AIMessage)

            tool_calls = last_message.tool_calls

            is_ready_for_user = any(tc["name"] == "submit_query_id" for tc in tool_calls)
            if is_ready_for_user:
                if len(tool_calls) > 1:
                    tool_messages = [
                        ToolMessage("submit_query_id must be the only tool call.", tool_call_id=tool_call["id"])
                        for tool_call in tool_calls
                    ]
                    return {"messages": tool_messages, "ready_for_user": False}
                else:
                    tool_call = tool_calls[0]

                    if "query_ids" not in state or len(state["query_ids"]) == 0:
                        tool_messages = [
                            ToolMessage("No queries have been executed yet.", tool_call_id=tool_call["id"])
                        ]
                        return {"messages": tool_messages, "ready_for_user": False}

                    query_id = tool_call["args"]["query_id"]
                    if query_id not in state["query_ids"]:
                        available_ids = ", ".join(state["query_ids"].keys())
                        tool_messages = [
                            ToolMessage(
                                f"Query ID {query_id} not found. Available query IDs: {available_ids}",
                                tool_call_id=tool_call["id"],
                            )
                        ]
                        return {"messages": tool_messages, "ready_for_user": False}

                    target_tool_message = state["query_ids"][query_id]
                    if target_tool_message.artifact is None or "df" not in target_tool_message.artifact:
                        tool_messages = [
                            ToolMessage(f"Query {query_id} does not have a valid result.", tool_call_id=tool_call["id"])
                        ]
                        return {"messages": tool_messages, "ready_for_user": False}

            query_ids = dict(state.get("query_ids", {}))
            sql = state.get("sql")
            df = state.get("df")
            visualization_prompt = state.get("visualization_prompt", "")

            message_index = len(state["messages"]) - 1

            for idx, tool_call in enumerate(tool_calls):
                name = tool_call["name"]
                args = tool_call["args"]
                tool_call_id = tool_call["id"]
                # Find the tool by name
                tool = next((t for t in tools if t.name == name), None)
                if tool is None:
                    tool_messages.append(ToolMessage(content=f"Tool {name} does not exist!", tool_call_id=tool_call_id))
                    continue

                try:
                    result = tool.invoke(args)
                except Exception as e:
                    result = {"error": exception_to_string(e) + f"\nTool: {name}, Args: {args}"}
                content = ""
                if name == "run_sql_query":
                    sql = result.get("sql")
                    df = result.get("df")
                    # Generate query_id using message index and tool call index
                    query_id = f"{message_index}-{idx}"
                    # Override the query_id in the result
                    result["query_id"] = query_id
                    content = result.get("csv", result.get("error", ""))
                    if "csv" in result:
                        content = f"query_id='{query_id}'\n\n{content}"
                    if query_id:
                        query_ids[query_id] = ToolMessage(
                            content=content,
                            tool_call_id=tool_call_id,
                            artifact=result,
                        )
                elif name == "submit_query_id":
                    content = str(result)
                    query_id = tool_call["args"]["query_id"]
                    visualization_prompt = tool_call["args"].get("visualization_prompt", "")
                    sql = state["query_ids"][query_id].artifact["sql"]
                    df = state["query_ids"][query_id].artifact["df"]
                tool_messages.append(ToolMessage(content=content, tool_call_id=tool_call_id, artifact=result))
                if name == "submit_query_id":
                    return {
                        "messages": tool_messages,
                        "sql": sql,
                        "df": df,
                        "visualization_prompt": visualization_prompt,
                        "ready_for_user": True,
                    }
            return {
                "messages": tool_messages,
                "query_ids": query_ids,
                "sql": sql,
                "df": df,
                "visualization_prompt": visualization_prompt,
                "ready_for_user": False,
            }

        def should_continue(state: AgentState) -> Literal["tool_executor", "end"]:
            # Check if there are tool calls in the last message
            last_message = state["messages"][-1]
            if isinstance(last_message, AIMessage) and last_message.tool_calls:
                return "tool_executor"
            return "end"

        def should_finish(state: AgentState) -> Literal["llm_node", "end"]:
            # Check if we just executed submit_query_id - if so, end the conversation
            if state.get("ready_for_user", False):
                return "end"
            return "llm_node"

        graph = StateGraph(AgentState)
        graph.add_node("llm_node", llm_node)
        graph.add_node("tool_executor", tool_executor_node)

        graph.add_edge(START, "llm_node")
        graph.add_conditional_edges("llm_node", should_continue, {"tool_executor": "tool_executor", "end": END})
        graph.add_conditional_edges("tool_executor", should_finish, {"llm_node": "llm_node", "end": END})
        return graph.compile()

    @staticmethod
    def _model_bind_tools(
        model: BaseChatModel, tools: Sequence[BaseTool], **kwargs: Any
    ) -> Runnable[LanguageModelInput, BaseMessage]:
        if isinstance(model, ChatOpenAI):
            return model.bind_tools(tools, strict=True, **kwargs)
        else:
            return model.bind_tools(tools, **kwargs)

    @staticmethod
    def _chat(
        messages: list[BaseMessage],
        config: LLMConfig,
        model: Runnable[list[BaseMessage], Any] | None = None,
    ) -> list[BaseMessage]:
        if model is None:
            model = config.chat_model
        messages = ExecuteSubmit._apply_system_prompt_caching(config, messages)
        response: AIMessage = ExecuteSubmit._call_model(model, messages)
        return [*messages, response]

    @staticmethod
    def _is_anthropic_model(config: LLMConfig) -> bool:
        """Check if the model is an Anthropic model based on the config name."""
        return "claude" in config.name.lower()

    @staticmethod
    def _apply_system_prompt_caching(config: LLMConfig, messages: list[BaseMessage]) -> list[BaseMessage]:
        """Apply system prompt caching for Anthropic models."""
        if not (config.cache_system_prompt and ExecuteSubmit._is_anthropic_model(config)):
            return messages
        # Assume only the first message can be a system prompt.
        assert all(m.type != "system" for m in messages[1:])
        if messages[0].type == "system":
            messages = [ExecuteSubmit._set_message_cache_breakpoint(config, messages[0]), *messages[1:]]
        return messages

    @staticmethod
    def _set_message_cache_breakpoint(config: LLMConfig, message: BaseMessage) -> BaseMessage:
        """Enable prompt caching for this message (for Anthropic models).

        If you have a list of messages, set a breakpoint only on the last message to automatically
        cache all previous messages.

        See https://docs.anthropic.com/en/docs/build-with-claude/prompt-caching
        > Prompt caching references the entire prompt - tools, system, and messages (in that order) up to and including
            the block designated with cache_control.
        """
        if not ExecuteSubmit._is_anthropic_model(config):
            return message
        new_content: list[dict[str, Any] | str]
        match message.content:
            case str() | dict():
                new_content = [ExecuteSubmit._set_anthropic_cache_breakpoint(message.content)]
            case list():
                # Set checkpoint only for the last message
                new_content = message.content.copy()
                new_content[-1] = ExecuteSubmit._set_anthropic_cache_breakpoint(new_content[-1])
        return message.model_copy(update={"content": new_content})

    @staticmethod
    def _set_anthropic_cache_breakpoint(content: str | dict[str, Any]) -> dict[str, Any]:
        if isinstance(content, str):
            return {"type": "text", "text": content, "cache_control": {"type": "ephemeral"}}
        elif isinstance(content, dict):
            d = content.copy()
            d["cache_control"] = {"type": "ephemeral"}
            return d
        else:
            raise ValueError(f"Unknown content type: {type(content)}")

    @staticmethod
    def _call_model(model: Runnable[list[BaseMessage], Any], messages: list[BaseMessage]) -> Any:
        return model.with_retry(wait_exponential_jitter=True, stop_after_attempt=3).invoke(messages)
