import json
import math
from typing import Annotated, Any, Literal

import pandas as pd
from duckdb import DuckDBPyConnection
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from langchain_core.tools import BaseTool, tool
from langgraph.constants import END, START
from langgraph.graph import add_messages
from langgraph.graph.state import CompiledStateGraph, StateGraph
from langgraph.prebuilt import InjectedState
from typing_extensions import TypedDict

from databao.agent.configs import llm
from databao.agent.configs.agent import AgentConfig
from databao.agent.configs.llm import LLMConfig
from databao.agent.core import Domain, ExecutionResult
from databao.agent.duckdb.react_tools import execute_duckdb_sql
from databao.agent.executors.frontend.text_frontend import dataframe_to_markdown
from databao.agent.executors.llm import chat, model_bind_tools
from databao.agent.executors.tools import make_search_context_tool


def exception_to_string(e: Exception | str) -> str:
    if isinstance(e, str):
        return e
    return f"Exception Name: {type(e).__name__}. Exception Desc: {e}"


class AgentState(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]
    query_ids: dict[str, ToolMessage]
    sql: str | None
    df: pd.DataFrame | None
    visualization_prompt: str | None
    ready_for_user: bool
    limit_max_rows: int | None


def get_query_ids_mapping(messages: list[BaseMessage]) -> dict[str, ToolMessage]:
    query_ids = {}
    for message in messages:
        if isinstance(message, ToolMessage) and isinstance(message.artifact, dict) and "query_id" in message.artifact:
            query_ids[message.artifact["query_id"]] = message
    return query_ids


def trim_string_middle(
    content: str, max_length: int | None, sep: str = "[...trimmed...]", front_percentage: float = 0.7
) -> str:
    if max_length is None or len(content) <= max_length:
        return content
    take_front = max(0, math.ceil(max_length * front_percentage) - len(sep) // 2)
    take_end = max(0, max_length - take_front - len(sep))
    return content[:take_front] + sep + content[len(content) - take_end :]


def trim_dataframe_values(df: pd.DataFrame, max_cell_chars: int | None) -> pd.DataFrame:
    df_sanitized = df.copy()
    if max_cell_chars is None:
        return df_sanitized

    def trim_cell(val: Any) -> str:
        return trim_string_middle(str(val), max_cell_chars)

    for col, dtype in zip(df_sanitized.columns, df_sanitized.dtypes, strict=True):
        if not pd.api.types.is_object_dtype(dtype) and not pd.api.types.is_string_dtype(dtype):
            continue
        df_sanitized[col] = df_sanitized[col].apply(trim_cell)
    return df_sanitized


class ExecuteSubmit:
    """Simple graph with two tools: run_sql_query and submit_result.
    All context must be in the SystemMessage."""

    MAX_TOOL_ROWS = 12
    """Max number of rows to return in SQL tool calls."""

    MAX_DF_CELL_CHARS = 1024
    """Max number of characters a dataframe cell can have before it is trimmed."""

    def __init__(self, connection: DuckDBPyConnection):
        self._connection = connection

    def init_state(self, messages: list[BaseMessage], *, limit_max_rows: int | None = None) -> AgentState:
        return AgentState(
            messages=messages,
            query_ids=get_query_ids_mapping(messages),
            sql=None,
            df=None,
            visualization_prompt=None,
            ready_for_user=False,
            limit_max_rows=limit_max_rows,
        )

    def get_result(self, state: AgentState) -> ExecutionResult:
        last_ai_message = None
        for m in reversed(state["messages"]):
            if isinstance(m, AIMessage):
                last_ai_message = m
                break
        if last_ai_message is None:
            raise RuntimeError("No AI message found in message log")
        if len(last_ai_message.tool_calls) == 0:
            # Sometimes models don't call the submit_result tool, but we still want to return some dataframe.
            sql = state.get("sql", "")
            df = state.get("df")  # Latest df result (usually from run_sql_query)
            visualization_prompt = state.get("visualization_prompt")
            result = ExecutionResult(
                text=last_ai_message.text,
                df=df,
                code=sql,
                meta={
                    "visualization_prompt": visualization_prompt,
                    ExecutionResult.META_MESSAGES_KEY: state["messages"],
                    "submit_called": False,
                },
            )
        elif len(last_ai_message.tool_calls) > 1:
            raise RuntimeError("Expected exactly one tool call in AI message")
        elif last_ai_message.tool_calls[0]["name"] != "submit_result":
            raise RuntimeError(
                f"Expected submit_result tool call in AI message, got {last_ai_message.tool_calls[0]['name']}"
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
                meta={
                    "visualization_prompt": visualization_prompt,
                    ExecutionResult.META_MESSAGES_KEY: state["messages"],
                    "submit_called": True,
                },
            )
        return result

    def has_search_context_tool(self, domain: Domain) -> bool:
        return make_search_context_tool(domain) is not None

    def make_tools(self, domain: Domain, extra_tools: list[BaseTool] | None = None) -> list[BaseTool]:
        @tool(parse_docstring=True)
        def run_sql_query(sql: str, graph_state: Annotated[AgentState, InjectedState]) -> dict[str, Any]:
            """
            Run a SELECT SQL query in the database. Returns the first 12 rows in csv format.

            Args:
                sql: SQL query
            """
            try:
                limit = graph_state["limit_max_rows"]
                df = execute_duckdb_sql(sql, self._connection, limit=limit)

                # Limit the size of sampled values to show to avoid context size explosions (e.g., json/binary blobs)
                df_display = df.head(self.MAX_TOOL_ROWS)
                df_display = trim_dataframe_values(df_display, max_cell_chars=self.MAX_DF_CELL_CHARS)

                df_csv = df_display.to_csv(index=False)
                df_markdown = dataframe_to_markdown(df_display, index=False)
                if len(df) > self.MAX_TOOL_ROWS:
                    df_csv += f"\nResult is truncated from {len(df)} to {self.MAX_TOOL_ROWS} rows."
                    df_markdown += f"\nResult is truncated from {len(df)} to {self.MAX_TOOL_ROWS} rows."
                return {"df": df, "sql": sql, "csv": df_csv, "markdown": df_markdown}
            except Exception as e:
                return {"error": exception_to_string(e)}

        @tool(parse_docstring=True)
        def submit_result(
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

        tools: list[BaseTool] = [run_sql_query, submit_result]
        search_context_tool = make_search_context_tool(domain)
        if search_context_tool is not None:
            tools.append(search_context_tool)

        if extra_tools:
            tools.extend(extra_tools)

        return tools

    def compile(
        self,
        model_config: LLMConfig,
        agent_config: AgentConfig,
        domain: Domain,
        extra_tools: list[BaseTool] | None = None,
    ) -> CompiledStateGraph[Any]:
        tools = self.make_tools(domain, extra_tools=extra_tools)
        llm_model = model_config.new_chat_model()

        if llm.is_openai_model(model_config.name):
            # Only OpenAI models support parallel tool calls parameter
            model_with_tools = model_bind_tools(llm_model, tools, parallel_tool_calls=agent_config.parallel_tool_calls)
        else:
            model_with_tools = model_bind_tools(llm_model, tools)

        def llm_node(state: AgentState) -> dict[str, Any]:
            messages = state["messages"]
            response = chat(messages, model_config, model_with_tools)
            return {"messages": [response[-1]]}

        def tool_executor_node(state: AgentState) -> dict[str, Any]:
            last_message = state["messages"][-1]
            tool_messages = []
            assert isinstance(last_message, AIMessage)

            tool_calls = last_message.tool_calls

            is_ready_for_user = any(tc["name"] == "submit_result" for tc in tool_calls)
            if is_ready_for_user:
                if len(tool_calls) > 1:
                    tool_messages = [
                        ToolMessage("submit_result must be the only tool call.", tool_call_id=tool_call["id"])
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
                    result = tool.invoke(args | {"graph_state": state})
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
                elif name == "submit_result":
                    content = str(result)
                    query_id = tool_call["args"]["query_id"]
                    visualization_prompt = tool_call["args"].get("visualization_prompt", "")
                    sql = state["query_ids"][query_id].artifact["sql"]
                    df = state["query_ids"][query_id].artifact["df"]
                else:
                    if isinstance(result, dict):
                        content = json.dumps(result, ensure_ascii=False, default=str)
                    else:
                        content = str(result)
                tool_messages.append(ToolMessage(content=content, tool_call_id=tool_call_id, artifact=result))
                if name == "submit_result":
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
            # Check if we just executed submit_result - if so, end the conversation
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
