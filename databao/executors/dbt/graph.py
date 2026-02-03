from __future__ import annotations

import json
import re
import subprocess
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, Literal

from langchain_core.language_models import BaseChatModel, LanguageModelInput
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from langchain_core.runnables import Runnable
from langchain_core.tools import BaseTool, tool
from langchain_openai import ChatOpenAI
from langgraph.constants import END, START
from langgraph.graph import add_messages
from langgraph.graph.state import CompiledStateGraph, StateGraph
from langgraph.prebuilt import InjectedState
from typing_extensions import TypedDict

from databao.duckdb.types import DbConnFactory
from databao.configs.llm import LLMConfig
from databao.executors.dbt.utils import db_introspect


@dataclass(frozen=True)
class DbtProjectContext:
    project_dir: Path
    pre_existing_files: set[str]
    dbt_timeout_seconds: int = 300


class DbtAgentState(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]
    context: DbtProjectContext
    tool_calls_log: list[dict[str, Any]]
    last_sql: str | None

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

    Tool names/signatures are aligned with databao/executors/dbt/system_prompt.jinja.
    """

    def __init__(self, *, db_conn_factory: DbConnFactory | None = None) -> None:
        self._db_conn_factory = db_conn_factory

    def init_state(
        self,
        messages: list[BaseMessage],
        *,
        project_dir: Path | str,
        pre_existing_files: Sequence[str],
        dbt_timeout_seconds: int = 300,
    ) -> DbtAgentState:
        ctx = DbtProjectContext(
            project_dir=Path(project_dir),
            pre_existing_files=set(pre_existing_files),
            dbt_timeout_seconds=dbt_timeout_seconds,
        )
        return DbtAgentState(messages=messages, context=ctx, tool_calls_log=[], last_sql=None)

    def get_result(self, state: DbtAgentState) -> dict[str, Any]:
        last_ai: AIMessage | None = next((m for m in reversed(state["messages"]) if isinstance(m, AIMessage)), None)
        return {
            "text": last_ai.text if last_ai else "",
            "code": state.get("last_sql"),
            "messages": state["messages"],
            "tool_calls": state["tool_calls_log"],
        }

    def make_tools(self) -> list[BaseTool]:
        @tool(parse_docstring=True)
        def run_database_explore(project_dir: str, graph_state: Annotated[DbtAgentState, InjectedState]) -> str:
            """
            Explore the provided database before any transformations.

            Args:
                project_dir: The directory of the dbt project (provided for compatibility; the graph has its own context)

            Returns:
                A markdown representation of the schema of the provided database.
            """
            if self._db_conn_factory is None:
                return _json_dumps({"error": "Database connection factory not provided."})
            con = self._db_conn_factory()
            try:
                schema_df = db_introspect(con)
                return schema_df.to_markdown(index=False)
            except Exception as e:
                return f"ERROR: could not introspect database: {e}"
            finally:
                con.close()

        @tool(parse_docstring=True)
        def run_sql(sql: str, sample_rows: int = 5) -> str:
            """
            Run a SQL query against the DuckDB database and return a compact JSON summary.

            Args:
                sql: SQL query
                sample_rows: number of rows to include in the sample

            Returns:
                JSON with keys: schema (name+dtype), row_count, sample_rows (list), truncated (bool)
            """
            if self._db_conn_factory is None:
                return _json_dumps({"error": "Database connection factory not provided."})

            con = self._db_conn_factory()
            try:
                df = con.execute(sql).fetchdf()
                schema = [{"name": c, "dtype": str(dt)} for c, dt in zip(df.columns, df.dtypes, strict=True)]
                sample = df.head(sample_rows).to_dict(orient="records")
                return _json_dumps(
                    {
                        "schema": schema,
                        "row_count": int(len(df)),
                        "sample_rows": sample,
                        "truncated": bool(len(df) > sample_rows),
                    }
                )
            except Exception as e:
                return _json_dumps({"error": str(e)})
            finally:
                con.close()

        @tool(parse_docstring=True)
        def run_dbt(project_dir: str | None, timeout: int | None, graph_state: Annotated[DbtAgentState, InjectedState]) -> str:
            """
            Run a dbt project to update the state of the database and return a compact structured result.

            Args:
                project_dir: Optional override; if omitted uses the graph context project_dir
                timeout: Optional override; if omitted uses the graph context dbt_timeout_seconds

            Returns:
                JSON with keys: returncode, stdout_tail, stderr_tail, timeout
            """
            ctx = graph_state["context"]
            project_dir_str = str(ctx.project_dir if project_dir is None else Path(project_dir))
            timeout_val = ctx.dbt_timeout_seconds if timeout is None else int(timeout)

            try:
                proc = subprocess.run(
                    ["dbt", "run"],
                    cwd=project_dir_str,
                    capture_output=True,
                    text=True,
                    timeout=timeout_val,
                    check=False,
                )
            except subprocess.TimeoutExpired:
                return _json_dumps({"timeout": True})

            return _json_dumps(
                {
                    "returncode": int(proc.returncode),
                    "stdout_tail": "\n".join((proc.stdout or "").splitlines()[-200:]),
                    "stderr_tail": "\n".join((proc.stderr or "").splitlines()[-200:]),
                    "timeout": False,
                }
            )

        @tool(parse_docstring=True)
        def dbt_deps(project_dir: str | None, graph_state: Annotated[DbtAgentState, InjectedState]) -> str:
            """
            Run a dbt deps command to update dependencies of the dbt project.

            Args:
                project_dir: Optional override; if omitted uses the graph context project_dir

            Returns:
                JSON with keys: returncode, stdout_tail, stderr_tail
            """
            ctx = graph_state["context"]
            project_dir_str = str(ctx.project_dir if project_dir is None else Path(project_dir))

            try:
                proc = subprocess.run(
                    ["dbt", "deps"],
                    cwd=project_dir_str,
                    capture_output=True,
                    text=True,
                    check=False,
                )
            except Exception as e:
                return _json_dumps({"error": str(e)})

            return _json_dumps(
                {
                    "returncode": int(proc.returncode),
                    "stdout_tail": "\n".join((proc.stdout or "").splitlines()[-20:]),
                    "stderr_tail": "\n".join((proc.stderr or "").splitlines()[-20:]),
                }
            )

        @tool(parse_docstring=True)
        def read_tool(path: str, graph_state: Annotated[DbtAgentState, InjectedState]) -> str:
            """
            Read a file (text).

            Args:
                path: absolute path OR relative to the dbt project directory

            Returns:
                A string containing the file content (truncated if too large)
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
                path: The path to write. Prefer absolute paths; relative paths are resolved from dbt project root.
                content: The content to write

            Returns:
                A string containing the summary of the write operation.
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
            Edit a file with a single replacement.

            Args:
                path: The path to edit (absolute or relative to project root)
                original: The original string/pattern to replace (regex)
                replacement: The replacement string

            Returns:
                A string containing the summary of the edit operation
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

            return f"EDITED {str(p.resolve())}: {n} replacements"

        @tool(parse_docstring=True)
        def grep_tool(table_name: str, graph_state: Annotated[DbtAgentState, InjectedState]) -> str:
            """
            Grep-like functionality to search for a pattern in all files in the project directory.

            Args:
                table_name: The pattern to grep

            Returns:
                A string containing the matched lines with filename:line:text
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

        return [
            run_database_explore,
            run_sql,
            run_dbt,
            dbt_deps,
            read_tool,
            write_tool,
            edit_tool,
            grep_tool,
        ]

    def compile(self, llm_config: LLMConfig) -> CompiledStateGraph[Any]:
        tools = self.make_tools()
        llm = llm_config.new_chat_model()
        model = self._bind_tools(llm, tools, parallel_tool_calls=llm_config.parallel_tool_calls)

        def llm_node(state: DbtAgentState) -> dict[str, Any]:
            response = self._call_model(model, llm_config, state["messages"])
            return {"messages": [response]}

        def tool_node(state: DbtAgentState) -> dict[str, Any]:
            last = state["messages"][-1]
            if not isinstance(last, AIMessage) or not last.tool_calls:
                return {}

            tool_by_name = {t.name: t for t in tools}
            out_messages: list[ToolMessage] = []
            tool_log = list(state.get("tool_calls_log", []))
            last_sql = state.get("last_sql")

            for tc in last.tool_calls:
                name = tc["name"]
                tool_call_id = tc["id"]
                args = tc.get("args", {}) or {}

                if name == "run_sql" and isinstance(args, dict):
                    sql = args.get("sql")
                    if isinstance(sql, str) and sql.strip():
                        last_sql = sql

                start = _now()
                try:
                    tool_obj = tool_by_name.get(name)
                    if tool_obj is None:
                        result = f"ERROR: unknown tool '{name}'"
                        ok = False
                        err = result
                    else:
                        result = tool_obj.invoke(args | {"graph_state": state})
                        ok = True
                        err = None
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

                out_messages.append(ToolMessage(content=str(result), tool_call_id=tool_call_id))

            return {"messages": out_messages, "tool_calls_log": tool_log, "last_sql": last_sql}

        def should_continue(state: DbtAgentState) -> Literal["tools", "end"]:
            last = state["messages"][-1]
            if isinstance(last, AIMessage) and last.tool_calls:
                return "tools"
            return "end"

        graph = StateGraph(DbtAgentState)
        graph.add_node("llm", llm_node)
        graph.add_node("tools", tool_node)

        graph.add_edge(START, "llm")
        graph.add_conditional_edges("llm", should_continue, {"tools": "tools", "end": END})
        graph.add_edge("tools", "llm")

        return graph.compile()

    @staticmethod
    def _bind_tools(
        model: BaseChatModel,
        tools: Sequence[BaseTool],
        **kwargs: Any,
    ) -> Runnable[LanguageModelInput, BaseMessage]:
        if isinstance(model, ChatOpenAI):
            return model.bind_tools(tools, strict=True, **kwargs)
        return model.bind_tools(tools, **kwargs)

    @staticmethod
    def _call_model(
        model: Runnable[list[BaseMessage], Any],
        llm_config: LLMConfig,
        messages: list[BaseMessage],
    ) -> AIMessage:
        return model.with_retry(wait_exponential_jitter=True, stop_after_attempt=3).invoke(messages)
