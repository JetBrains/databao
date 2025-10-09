from typing import Annotated, Any, Literal, TypedDict

import pandas as pd
from duckdb import DuckDBPyConnection
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from langchain_core.tools import BaseTool, tool
from langgraph.constants import END, START
from langgraph.graph import add_messages
from langgraph.graph.state import CompiledStateGraph, StateGraph

from portus.agent.base_agent import ExecutionResult
from portus.langchain_graphs.graph import Graph
from portus.llms import LLMConfig, chat, get_chat_model, model_bind_tools
from portus.utils import exception_to_string

MAX_ROWS = 12


class AgentState(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]
    query_ids: dict[str, ToolMessage]
    sql: str | None
    df: pd.DataFrame | None
    visualization_prompt: str | None
    ready_for_user: bool


class ExecuteSubmit(Graph):
    """Simple graph with two tools: run_sql_query and submit_query_id.
    All context must be in the SystemMessage."""

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
            result = ExecutionResult(text=text, df=None, sql="", messages=state["messages"])
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
                text=text, df=df, sql=sql, visualization_prompt=visualization_prompt, messages=state["messages"]
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
                df_csv = df_or_error.head(MAX_ROWS).to_csv(index=False)
                df_markdown = df_or_error.head(MAX_ROWS).to_markdown(index=False)
                if len(df_or_error) > MAX_ROWS:
                    df_csv += f"\nResult is truncated from {len(df_or_error)} to {MAX_ROWS} rows."
                    df_markdown += f"\nResult is truncated from {len(df_or_error)} to {MAX_ROWS} rows."
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
        llm_model = get_chat_model(model_config)
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
