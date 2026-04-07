import json
from typing import Annotated, Any, Literal

import pandas as pd
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from langchain_core.tools import BaseTool, tool
from langgraph.constants import END, START
from langgraph.graph import add_messages
from langgraph.graph.state import CompiledStateGraph, StateGraph
from langgraph.prebuilt import InjectedState
from sqlalchemy import Engine, text
from typing_extensions import TypedDict

from databao.agent.configs import llm
from databao.agent.configs.agent import AgentConfig
from databao.agent.configs.llm import LLMConfig
from databao.agent.core import Domain, ExecutionResult
from databao.agent.executors.frontend.text_frontend import dataframe_to_markdown
from databao.agent.executors.langchain_tools import make_search_context_tool
from databao.agent.executors.llm import chat, model_bind_tools
from databao.agent.executors.utils import exception_to_string, trim_dataframe_values


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


_FORBIDDEN_SQL_PREFIXES = (
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "ALTER",
    "CREATE",
    "TRUNCATE",
    "MERGE",
    "GRANT",
    "REVOKE",
    "CALL",
    "EXEC",
    "EXECUTE",
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "COPY",
    "PUT",
    "GET",
    "REMOVE",
)


def _validate_read_only(sql: str) -> None:
    """Raise ValueError if *sql* looks like a write / DDL statement."""
    stripped = sql.strip().lstrip("(").strip()
    first_word = stripped.split(None, 1)[0].upper().rstrip(";") if stripped else ""
    if first_word in _FORBIDDEN_SQL_PREFIXES:
        raise ValueError(f"Only SELECT / read-only queries are allowed. Got statement starting with '{first_word}'.")


def _run_sql(engine: Engine, sql: str, limit: int | None) -> pd.DataFrame:
    _validate_read_only(sql)
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        columns = list(result.keys())
        rows = result.fetchmany(limit) if limit is not None else result.fetchall()
    return pd.DataFrame(rows, columns=columns)


class SeparateGraph:
    """Graph with two tools: run_sql_query and submit_result.

    SQL is routed to per-datasource SQLAlchemy engines via a ``datasource`` argument
    on ``run_sql_query``. The ``connections`` dict is held by reference, so engines
    added after construction are immediately visible.
    """

    MAX_TOOL_ROWS = 12
    MAX_DF_CELL_CHARS = 1024

    def __init__(self, connections: dict[str, Engine]):
        self._connections = connections

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
            result = ExecutionResult(
                text=last_ai_message.text,
                df=state.get("df"),
                code=state.get("sql", ""),
                meta={
                    "visualization_prompt": state.get("visualization_prompt"),
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
            tool_call = last_ai_message.tool_calls[0]
            result = ExecutionResult(
                text=tool_call["args"]["result_description"],
                df=state.get("df"),
                code=state.get("sql", ""),
                meta={
                    "visualization_prompt": state.get("visualization_prompt", ""),
                    ExecutionResult.META_MESSAGES_KEY: state["messages"],
                    "submit_called": True,
                },
            )
        return result

    def has_search_context_tool(self, domain: Domain) -> bool:
        return make_search_context_tool(domain) is not None

    def make_tools(self, domain: Domain, extra_tools: list[BaseTool] | None = None) -> list[BaseTool]:
        @tool(parse_docstring=True)
        def run_sql_query(
            sql: str, datasource: str, graph_state: Annotated[AgentState, InjectedState]
        ) -> dict[str, Any]:
            """
            Run a SELECT SQL query against a specific datasource. Returns the first 12 rows in csv format.

            Args:
                sql: SQL query to execute
                datasource: Name of the datasource to run the query against
            """
            try:
                if datasource not in self._connections:
                    available = sorted(self._connections.keys())
                    return {"error": f"Unknown datasource '{datasource}'. Available: {available}"}

                limit = graph_state["limit_max_rows"]
                df = _run_sql(self._connections[datasource], sql, limit)

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
            The user will see the query result up to the configured maximum row limit (which may be larger than the
            12-row preview shown in tool output). Returns a confirmation message.

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
            model_with_tools = model_bind_tools(llm_model, tools, parallel_tool_calls=agent_config.parallel_tool_calls)
        else:
            model_with_tools = model_bind_tools(llm_model, tools)

        def llm_node(state: AgentState) -> dict[str, Any]:
            response = chat(state["messages"], model_config, model_with_tools)
            return {"messages": [response[-1]]}

        def tool_executor_node(state: AgentState) -> dict[str, Any]:
            last_message = state["messages"][-1]
            assert isinstance(last_message, AIMessage)
            tool_calls = last_message.tool_calls
            tool_messages = []

            is_ready_for_user = any(tc["name"] == "submit_result" for tc in tool_calls)
            if is_ready_for_user:
                if len(tool_calls) > 1:
                    return {
                        "messages": [
                            ToolMessage("submit_result must be the only tool call.", tool_call_id=tc["id"])
                            for tc in tool_calls
                        ],
                        "ready_for_user": False,
                    }
                tool_call = tool_calls[0]
                if "query_ids" not in state or len(state["query_ids"]) == 0:
                    return {
                        "messages": [ToolMessage("No queries have been executed yet.", tool_call_id=tool_call["id"])],
                        "ready_for_user": False,
                    }
                query_id = tool_call["args"]["query_id"]
                if query_id not in state["query_ids"]:
                    available_ids = ", ".join(state["query_ids"].keys())
                    return {
                        "messages": [
                            ToolMessage(
                                f"Query ID {query_id} not found. Available query IDs: {available_ids}",
                                tool_call_id=tool_call["id"],
                            )
                        ],
                        "ready_for_user": False,
                    }
                target = state["query_ids"][query_id]
                if target.artifact is None or "df" not in target.artifact:
                    return {
                        "messages": [
                            ToolMessage(f"Query {query_id} does not have a valid result.", tool_call_id=tool_call["id"])
                        ],
                        "ready_for_user": False,
                    }

            query_ids = dict(state.get("query_ids", {}))
            sql = state.get("sql")
            df = state.get("df")
            visualization_prompt = state.get("visualization_prompt", "")
            message_index = len(state["messages"]) - 1

            for idx, tool_call in enumerate(tool_calls):
                name = tool_call["name"]
                args = tool_call["args"]
                tool_call_id = tool_call["id"]
                t = next((t for t in tools if t.name == name), None)
                if t is None:
                    tool_messages.append(ToolMessage(content=f"Tool {name} does not exist!", tool_call_id=tool_call_id))
                    continue

                try:
                    result = t.invoke(args | {"graph_state": state})
                except Exception as e:
                    result = {"error": exception_to_string(e) + f"\nTool: {name}, Args: {args}"}

                content = ""
                if name == "run_sql_query":
                    sql = result.get("sql")
                    df = result.get("df")
                    query_id = f"{message_index}-{idx}"
                    result["query_id"] = query_id
                    content = result.get("csv", result.get("error", ""))
                    if "csv" in result:
                        content = f"query_id='{query_id}'\n\n{content}"
                    query_ids[query_id] = ToolMessage(content=content, tool_call_id=tool_call_id, artifact=result)
                elif name == "submit_result":
                    content = str(result)
                    query_id = tool_call["args"]["query_id"]
                    visualization_prompt = tool_call["args"].get("visualization_prompt", "")
                    sql = state["query_ids"][query_id].artifact["sql"]
                    df = state["query_ids"][query_id].artifact["df"]
                else:
                    content = (
                        json.dumps(result, ensure_ascii=False, default=str) if isinstance(result, dict) else str(result)
                    )

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
            last_message = state["messages"][-1]
            if isinstance(last_message, AIMessage) and last_message.tool_calls:
                return "tool_executor"
            return "end"

        def should_finish(state: AgentState) -> Literal["llm_node", "end"]:
            return "end" if state.get("ready_for_user", False) else "llm_node"

        graph = StateGraph(AgentState)
        graph.add_node("llm_node", llm_node)
        graph.add_node("tool_executor", tool_executor_node)
        graph.add_edge(START, "llm_node")
        graph.add_conditional_edges("llm_node", should_continue, {"tool_executor": "tool_executor", "end": END})
        graph.add_conditional_edges("tool_executor", should_finish, {"llm_node": "llm_node", "end": END})
        return graph.compile()
