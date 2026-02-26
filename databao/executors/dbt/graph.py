from __future__ import annotations

import json
import re
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, Literal

import pandas as pd
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from langchain_core.tools import BaseTool, tool
from langgraph.constants import END, START
from langgraph.graph import add_messages
from langgraph.graph.state import CompiledStateGraph, StateGraph
from langgraph.prebuilt import InjectedState
from typing_extensions import TypedDict

from databao.configs import llm
from databao.configs.agent import AgentConfig
from databao.configs.llm import LLMConfig
from databao.core import Domain, ExecutionResult
from databao.executors.dbt.dbt_runner import (
    PostDbtRunHook,
    noop_post_run_hook,
    run_dbt_subprocess,
)
from databao.executors.dbt.query_runner import QueryRunnerFactory
from databao.executors.llm import chat, model_bind_tools
from databao.executors.query_expansion import QueryExpansionConfig
from databao.executors.tools import make_search_context_tool


@dataclass(frozen=True)
class DbtProjectContext:
    """Context information for interacting with a dbt project.

    :ivar project_dir: Filesystem path to the root directory of the dbt project.
    :ivar pre_existing_files: Set of file paths (relative to ``project_dir``) that
        were present before the agent started and must not be modified by the agent.
    :ivar dbt_timeout_seconds: Maximum time, in seconds, to allow a dbt subprocess
        (e.g., ``dbt run``, ``dbt test``) to execute before timing out.
    """

    project_dir: Path
    pre_existing_files: set[str]
    dbt_timeout_seconds: int = 300


class DbtAgentState(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]
    context: DbtProjectContext
    tool_calls_log: list[dict[str, Any]]
    last_sql: str | None
    last_df: pd.DataFrame | None
    last_dbt_returncode: int | None
    answer_sql: str | None
    answer_df: pd.DataFrame | None
    dbt_dirty: bool


def _now() -> float:
    return time.time()


def _tool_log_entry(
    *,
    name: str,
    start: float,
    end: float,
    ok: bool,
    error: str | None,
    result_preview_len: int,
) -> dict[str, Any]:
    return {
        "tool": name,
        "start": start,
        "end": end,
        "duration": end - start,
        "success": ok,
        "error": error,
        "result_preview_len": result_preview_len,
    }


def _json_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, default=str)


class DbtProjectGraph:
    """
    Minimal, reusable tool-using graph for dbt project editing + dbt run.
    """

    def __init__(
        self,
        *,
        query_runner_factory: QueryRunnerFactory | None = None,
        post_dbt_run_hook: PostDbtRunHook = noop_post_run_hook,
        expansion_llm: BaseChatModel | None = None,
        expansion_config: QueryExpansionConfig | None = None,
    ) -> None:
        self._query_runner_factory = query_runner_factory
        self._post_dbt_run_hook = post_dbt_run_hook
        self._expansion_llm = expansion_llm
        self._expansion_config = expansion_config

    def init_state(
        self,
        messages: list[BaseMessage],
        *,
        project_dir: Path | str,
        pre_existing_files: Sequence[str],
        dbt_timeout_seconds: int = 300,
        dbt_dirty: bool = True,
    ) -> DbtAgentState:
        ctx = DbtProjectContext(
            project_dir=Path(project_dir),
            pre_existing_files=set(pre_existing_files),
            dbt_timeout_seconds=dbt_timeout_seconds,
        )
        return DbtAgentState(
            messages=messages,
            context=ctx,
            tool_calls_log=[],
            last_sql=None,
            last_df=None,
            last_dbt_returncode=None,
            answer_sql=None,
            answer_df=None,
            dbt_dirty=dbt_dirty,
        )

    def get_result(self, state: DbtAgentState) -> ExecutionResult:
        last_ai: AIMessage | None = next((m for m in reversed(state["messages"]) if isinstance(m, AIMessage)), None)
        result_df = state.get("answer_df") if state.get("answer_df") is not None else state.get("last_df")
        result_sql = state.get("answer_sql") if state.get("answer_sql") is not None else state.get("last_sql")

        return ExecutionResult(
            text=last_ai.text if last_ai else "",
            code=result_sql,
            df=result_df,
            meta={
                ExecutionResult.META_MESSAGES_KEY: state["messages"],
                "tool_calls": state["tool_calls_log"],
                "answer_submitted": state.get("answer_df") is not None,
            },
        )

    def make_tools(self, domain: Domain, extra_tools: list[BaseTool] | None = None) -> list[BaseTool]:
        @tool(parse_docstring=True)
        def run_sql(sql: str, sample_rows: int = 5) -> dict[str, Any]:
            """
            Run a SQL query against the database.

            Args:
                sql: SQL query
                sample_rows: number of rows to include in the sample

            Returns:
                JSON with keys: schema, row_count, sample_rows, truncated
            """
            if self._query_runner_factory is None:
                return {"error": "Query runner factory not provided."}

            # Guard: reject ATTACH / multi-statement SQL that could break the connection
            sql_stripped = sql.strip().rstrip(";")
            if re.search(r"\bATTACH\b", sql_stripped, re.IGNORECASE):
                return {
                    "error": (
                        "Do NOT use ATTACH in run_sql. The database is already attached. "
                        "Use fully qualified table names discovered via search_context."
                    )
                }

            runner = self._query_runner_factory()
            try:
                df = runner.execute_to_df(sql)
                schema = [{"name": c, "dtype": str(dt)} for c, dt in zip(df.columns, df.dtypes, strict=True)]
                sample = df.head(sample_rows).to_dict(orient="records")
                return {
                    "schema": schema,
                    "row_count": len(df),
                    "sample_rows": sample,
                    "truncated": bool(len(df) > sample_rows),
                    "df": df,
                    "sql": sql,
                }
            except Exception as e:
                return {"error": str(e)}
            finally:
                runner.close()

        @tool(parse_docstring=True)
        def run_dbt(
            project_dir: str | None,
            timeout: int | None,
            graph_state: Annotated[DbtAgentState, InjectedState],
        ) -> str:
            """
            Run a dbt project to update the database state.

            Args:
                project_dir: Optional override; if omitted uses context project_dir
                timeout: Optional override; if omitted uses context dbt_timeout_seconds

            Returns:
                JSON with keys: returncode, stdout_tail, stderr_tail, timeout
            """
            ctx = graph_state["context"]
            project_dir_str = str(ctx.project_dir if project_dir is None else Path(project_dir))
            timeout_val = ctx.dbt_timeout_seconds if timeout is None else int(timeout)

            result = run_dbt_subprocess(
                command="run",
                project_dir=project_dir_str,
                timeout=timeout_val,
                post_run_hook=self._post_dbt_run_hook,
            )
            return _json_dumps(result)

        @tool(parse_docstring=True)
        def dbt_deps(project_dir: str | None, graph_state: Annotated[DbtAgentState, InjectedState]) -> str:
            """
            Run dbt deps to install dependencies.

            Args:
                project_dir: Optional override

            Returns:
                JSON with keys: returncode, stdout_tail, stderr_tail
            """
            ctx = graph_state["context"]
            project_dir_str = str(ctx.project_dir if project_dir is None else Path(project_dir))

            result = run_dbt_subprocess(
                command="deps",
                project_dir=project_dir_str,
                post_run_hook=noop_post_run_hook,
                stdout_tail_lines=20,
                stderr_tail_lines=20,
            )
            return _json_dumps(result)

        @tool(parse_docstring=True)
        def read_tool(path: str, graph_state: Annotated[DbtAgentState, InjectedState]) -> str:
            """
            Read a file.

            Args:
                path: absolute path OR relative to dbt project directory

            Returns:
                File content (truncated if too large)
            """
            project_dir = graph_state["context"].project_dir
            p = Path(path)
            if not p.is_absolute():
                p = project_dir / p
            try:
                text = p.read_text(encoding="utf-8", errors="replace")
                if len(text) > 20_000:
                    return text[:20_000] + "\n\n...[truncated]"
                return text
            except Exception as e:
                return f"ERROR: could not read {p}: {e}"

        @tool(parse_docstring=True)
        def write_tool(path: str, content: str, graph_state: Annotated[DbtAgentState, InjectedState]) -> str:
            """
            Write file.

            Args:
                path: Path to write (absolute or relative to project root)
                content: Content to write

            Returns:
                Summary of the write operation
            """
            ctx = graph_state["context"]
            p = Path(path)
            if not p.is_absolute():
                p = ctx.project_dir / p

            p_str = str(p.resolve())
            if p_str in ctx.pre_existing_files:
                return (
                    f"ERROR: file {p_str} exists within the project from the beginning. "
                    f"You can only create new files / overwrite files you create yourself."
                )

            try:
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text(content, encoding="utf-8")
                return f"WROTE {p_str} ({len(content)} chars)"
            except Exception as e:
                return f"ERROR: write failed: {e}"

        @tool(parse_docstring=True)
        def edit_tool(
            path: str,
            original: str,
            replacement: str,
            graph_state: Annotated[DbtAgentState, InjectedState],
        ) -> str:
            """
            Edit a file with regex replacement.

            Args:
                path: Path to edit
                original: Original string/pattern (regex)
                replacement: Replacement string

            Returns:
                Summary of the edit operation
            """
            project_dir = graph_state["context"].project_dir
            p = Path(path)
            if not p.is_absolute():
                p = project_dir / p

            try:
                text = p.read_text(encoding="utf-8", errors="replace")
            except Exception as e:
                return f"ERROR: file {p} not found: {e}"

            try:
                new_text, n = re.subn(original, replacement, text)
            except re.error as e:
                return f"ERROR: regex failed: {e}"

            try:
                p.write_text(new_text, encoding="utf-8")
            except Exception as e:
                return f"ERROR: edit failed: {e}"

            return f"EDITED {p.resolve()!s}: {n} replacements"

        @tool(parse_docstring=True)
        def grep_tool(table_name: str, graph_state: Annotated[DbtAgentState, InjectedState]) -> str:
            """
            Search for a pattern in all project files.

            Args:
                table_name: Pattern to grep

            Returns:
                Matched lines with filename:line:text
            """
            project_dir = graph_state["context"].project_dir
            try:
                cre = re.compile(rf"\b{re.escape(table_name)}\b")
            except re.error as e:
                return f"ERROR: regex error: {e}"

            matches: list[str] = []
            for p in sorted(project_dir.rglob("*")):
                if not p.is_file():
                    continue
                try:
                    with p.open(encoding="utf-8", errors="ignore") as fh:
                        for i, line in enumerate(fh, start=1):
                            if cre.search(line):
                                matches.append(f"{p}:{i}:{line.rstrip()}")
                                if len(matches) >= 500:
                                    break
                except Exception:
                    continue
                if len(matches) >= 500:
                    break

            result = "\n".join(matches) if matches else "(no matches)"
            if len(result) > 10_000:
                return result[:10_000] + "\n\n...[truncated]"
            return result

        @tool(parse_docstring=True)
        def submit_answer(
            sql: str,
            description: str,
            graph_state: Annotated[DbtAgentState, InjectedState],
        ) -> dict[str, Any]:
            """
            Submit the final answer to the user's question. Call this AFTER you have verified your answer.
            This marks the provided SQL as the definitive answer that will be returned to the user.

            Args:
                sql: The SQL query that produces the answer (will be executed and returned as the final DataFrame)
                description: A brief description of what the result contains

            Returns:
                Confirmation with result summary
            """
            if self._query_runner_factory is None:
                return {"_submit_answer": False, "error": "SQL executor factory not provided."}

            runner = self._query_runner_factory()
            try:
                df = runner.execute_to_df(sql)
                return {
                    "_submit_answer": True,
                    "sql": sql,
                    "description": description,
                    "row_count": len(df),
                    "columns": list(df.columns),
                    "preview": df.head(5).to_dict(orient="records"),
                    "df": df,
                }
            except Exception as e:
                return {
                    "_submit_answer": False,
                    "error": str(e),
                }
            finally:
                runner.close()

        tools: list[BaseTool] = [
            run_sql,
            run_dbt,
            dbt_deps,
            read_tool,
            write_tool,
            edit_tool,
            grep_tool,
            submit_answer,
        ]

        search_context_tool = make_search_context_tool(
            domain,
            expansion_llm=self._expansion_llm,
            expansion_config=self._expansion_config,
        )
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

        def llm_node(state: DbtAgentState) -> dict[str, Any]:
            messages = state["messages"]
            response = chat(messages, model_config, model_with_tools)
            return {"messages": [response[-1]]}

        def tool_executor_node(state: DbtAgentState) -> dict[str, Any]:
            last = state["messages"][-1]
            if not isinstance(last, AIMessage) or not last.tool_calls:
                return {}

            tool_by_name = {t.name: t for t in tools}
            out_messages: list[ToolMessage] = []
            tool_log = list(state.get("tool_calls_log", []))
            last_sql = state.get("last_sql")
            last_df = state.get("last_df")
            last_dbt_returncode = state.get("last_dbt_returncode")
            answer_sql = state.get("answer_sql")
            answer_df = state.get("answer_df")
            dbt_dirty = state.get("dbt_dirty", True)

            for tc in last.tool_calls:
                name = tc["name"]
                tool_call_id = tc["id"]
                args = tc.get("args", {}) or {}

                start = _now()
                try:
                    tool_obj = tool_by_name.get(name)
                    if tool_obj is None:
                        result = f"ERROR: unknown tool '{name}'"
                        ok = False
                        err = result
                    elif name == "run_dbt" and not dbt_dirty:
                        result = _json_dumps(
                            {
                                "returncode": 0,
                                "skipped": True,
                                "message": "dbt run skipped — no file changes since last successful run.",
                            }
                        )
                        ok = True
                        err = None
                    else:
                        result = tool_obj.invoke(args | {"graph_state": state})
                        ok = True
                        err = None

                        if name == "run_sql" and isinstance(result, dict):
                            if "sql" in result:
                                last_sql = result["sql"]
                            if "df" in result:
                                last_df = result["df"]

                        if (
                            name == "submit_answer"
                            and isinstance(result, dict)
                            and result.get("_submit_answer")
                            and "df" in result
                        ):
                            answer_sql = result.get("sql")
                            answer_df = result.pop("df")

                        if name in ("write_tool", "edit_tool"):
                            dbt_dirty = True

                        if name == "run_dbt":
                            try:
                                parsed = json.loads(result) if isinstance(result, str) else result
                                if parsed.get("skipped"):
                                    pass  # already clean, keep dbt_dirty as-is
                                elif "returncode" in parsed:
                                    last_dbt_returncode = parsed["returncode"]
                                    if parsed["returncode"] == 0:
                                        dbt_dirty = False
                                elif parsed.get("timeout"):
                                    last_dbt_returncode = -1
                            except (json.JSONDecodeError, TypeError):
                                pass

                except Exception as e:
                    result = f"ERROR: tool '{name}' failed: {e}"
                    ok = False
                    err = str(e)
                end = _now()

                tool_log.append(
                    _tool_log_entry(
                        name=name,
                        start=start,
                        end=end,
                        ok=ok,
                        error=err,
                        result_preview_len=len(str(result)) if result is not None else 0,
                    )
                )

                if isinstance(result, dict):
                    content = _json_dumps({k: v for k, v in result.items() if k != "df"})
                else:
                    content = str(result)

                out_messages.append(ToolMessage(content=content, tool_call_id=tool_call_id))

            return {
                "messages": out_messages,
                "tool_calls_log": tool_log,
                "last_sql": last_sql,
                "last_df": last_df,
                "last_dbt_returncode": last_dbt_returncode,
                "answer_sql": answer_sql,
                "answer_df": answer_df,
                "dbt_dirty": dbt_dirty,
            }

        def should_continue(state: DbtAgentState) -> Literal["tool_executor", "end"]:
            last = state["messages"][-1]
            if isinstance(last, AIMessage) and last.tool_calls:
                return "tool_executor"
            return "end"

        graph = StateGraph(DbtAgentState)
        graph.add_node("llm_node", llm_node)
        graph.add_node("tool_executor", tool_executor_node)

        graph.add_edge(START, "llm_node")
        graph.add_conditional_edges("llm_node", should_continue, {"tool_executor": "tool_executor", "end": END})
        graph.add_edge("tool_executor", "llm_node")
        return graph.compile()
