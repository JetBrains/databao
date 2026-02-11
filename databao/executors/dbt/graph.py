from __future__ import annotations

import hashlib
import json
import re
import shutil
import tempfile
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, Literal

import pandas as pd
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

from databao.configs.agent import AgentConfig
from databao.configs.llm import LLMConfig
from databao.executors.dbt.dbt_runner import (
    PostDbtRunHook,
    noop_post_run_hook,
    run_dbt_subprocess,
)
from databao.executors.dbt.sql_executor import SqlExecutorFactory


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _snapshot_tree(root: Path) -> dict[Path, str]:
    out: dict[Path, str] = {}
    for p in sorted(root.rglob("*")):
        if p.is_file():
            out[p.relative_to(root)] = _sha256_file(p)
    return out


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
    last_df: pd.DataFrame | None
    last_dbt_returncode: int | None
    answer_sql: str | None
    answer_df: pd.DataFrame | None


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
    Supports optional sandboxing for safe execution.
    """

    def __init__(
        self,
        *,
        sql_executor_factory: SqlExecutorFactory | None = None,
        post_dbt_run_hook: PostDbtRunHook = noop_post_run_hook,
    ) -> None:
        self._sql_executor_factory = sql_executor_factory
        self._post_dbt_run_hook = post_dbt_run_hook
        self._source_project_dir: Path | None = None
        self._staged_project_dir: Path | None = None
        self._before_snapshot: dict[Path, str] | None = None
        self._db_explored: bool = False
        self._db_path_remaps: dict[str, str] = {}

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
        return DbtAgentState(
            messages=messages,
            context=ctx,
            tool_calls_log=[],
            last_sql=None,
            last_df=None,
            last_dbt_returncode=None,
            answer_sql=None,
            answer_df=None,
        )

    def init_state_sandboxed(
        self,
        messages: list[BaseMessage],
        *,
        project_dir: Path | str,
        dbt_timeout_seconds: int = 300,
    ) -> DbtAgentState:
        """Create sandbox copy and return state pointing to it."""
        source_dir = Path(project_dir).resolve()

        tmp_root = Path(tempfile.mkdtemp(prefix="databao_dbt_sandbox_"))
        staged = tmp_root / source_dir.name
        staged.mkdir(parents=True, exist_ok=True)

        for entry in source_dir.iterdir():
            dst = staged / entry.name
            if entry.is_dir():
                shutil.copytree(entry, dst)
            else:
                shutil.copy2(entry, dst)

        self._source_project_dir = source_dir
        self._staged_project_dir = staged
        self._before_snapshot = _snapshot_tree(staged)

        self._db_path_remaps = {}
        for db_file in staged.rglob("*.duckdb"):
            original_path = source_dir / db_file.relative_to(staged)
            self._db_path_remaps[str(original_path)] = str(db_file)

        pre_existing_files = [str(p.resolve()) for p in staged.rglob("*") if p.is_file()]

        ctx = DbtProjectContext(
            project_dir=staged,
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
        )

    def should_commit_sandbox(self, state: DbtAgentState) -> bool:
        """Check if sandbox should be committed based on last dbt run result."""
        return state.get("last_dbt_returncode") == 0

    def commit_sandbox(self) -> dict[str, Any]:
        """Copy changed files from sandbox back to source directory."""
        if not self._staged_project_dir or not self._source_project_dir or not self._before_snapshot:
            raise RuntimeError("No active sandbox to commit.")

        self._post_dbt_run_hook(self._staged_project_dir)

        after_snapshot = _snapshot_tree(self._staged_project_dir)

        before_paths = set(self._before_snapshot.keys())
        after_paths = set(after_snapshot.keys())

        added = sorted(after_paths - before_paths)
        modified = [p for p in sorted(before_paths & after_paths) if self._before_snapshot[p] != after_snapshot[p]]

        copied: list[str] = []
        for rel in added + modified:
            src_path = self._staged_project_dir / rel
            dst_path = self._source_project_dir / rel
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_path, dst_path)
            copied.append(str(rel))

        result = {
            "added": [str(p) for p in added],
            "modified": [str(p) for p in modified],
            "copied": copied,
            "source_dir": str(self._source_project_dir),
            "staged_dir": str(self._staged_project_dir),
        }

        self._clear_sandbox_state()
        return result

    def discard_sandbox(self) -> None:
        """Discard sandbox without committing."""
        self._clear_sandbox_state()

    def _clear_sandbox_state(self) -> None:
        self._source_project_dir = None
        self._staged_project_dir = None
        self._before_snapshot = None
        self._db_path_remaps = {}

    def get_result(self, state: DbtAgentState) -> dict[str, Any]:
        last_ai: AIMessage | None = next(
            (m for m in reversed(state["messages"]) if isinstance(m, AIMessage)), None
        )
        result_df = state.get("answer_df") if state.get("answer_df") is not None else state.get("last_df")
        result_sql = state.get("answer_sql") if state.get("answer_sql") is not None else state.get("last_sql")

        return {
            "text": last_ai.text if last_ai else "",
            "code": result_sql,
            "df": result_df,
            "messages": state["messages"],
            "tool_calls": state["tool_calls_log"],
            "answer_submitted": state.get("answer_df") is not None,
        }

    def make_tools(self) -> list[BaseTool]:
        @tool(parse_docstring=True)
        def run_database_explore(project_dir: str) -> str:
            """
            Explore the provided database schema. Can only be called once at the beginning.

            Args:
                project_dir: The directory of the dbt project

            Returns:
                A markdown representation of the schema of the provided database.
            """
            if self._db_explored:
                return (
                    "OK: Database schema was already retrieved at the start of this session. "
                    "Refer to the earlier run_database_explore result in the conversation. "
                    "Use run_sql('SELECT * FROM table LIMIT 5') to explore specific tables."
                )

            if self._sql_executor_factory is None:
                return _json_dumps({"error": "SQL executor factory not provided."})

            executor = self._sql_executor_factory()
            try:
                schema_df = executor.introspect()
                result = schema_df.to_markdown(index=False)
                self._db_explored = True
                return result
            except Exception as e:
                return f"ERROR: could not introspect database: {e}"
            finally:
                executor.close()

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
            if self._sql_executor_factory is None:
                return {"error": "SQL executor factory not provided."}

            # Guard: reject ATTACH / multi-statement SQL that could break the connection
            sql_stripped = sql.strip().rstrip(";")
            if re.search(r"\bATTACH\b", sql_stripped, re.IGNORECASE):
                return {"error": "Do NOT use ATTACH in run_sql. The database is already attached. Use fully qualified table names from run_database_explore."}

            executor = self._sql_executor_factory()
            try:
                df = executor.execute_to_df(sql)
                schema = [{"name": c, "dtype": str(dt)} for c, dt in zip(df.columns, df.dtypes, strict=True)]
                sample = df.head(sample_rows).to_dict(orient="records")
                return {
                    "schema": schema,
                    "row_count": int(len(df)),
                    "sample_rows": sample,
                    "truncated": bool(len(df) > sample_rows),
                    "df": df,
                    "sql": sql,
                }
            except Exception as e:
                return {"error": str(e)}
            finally:
                executor.close()

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

            return f"EDITED {str(p.resolve())}: {n} replacements"

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
            if self._sql_executor_factory is None:
                return {"_submit_answer": False, "error": "SQL executor factory not provided."}

            executor = self._sql_executor_factory()
            try:
                df = executor.execute_to_df(sql)
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
                executor.close()

        return [
            run_database_explore,
            run_sql,
            run_dbt,
            dbt_deps,
            read_tool,
            write_tool,
            edit_tool,
            grep_tool,
            submit_answer,
        ]

    def compile(self, llm_config: LLMConfig, agent_config: AgentConfig) -> CompiledStateGraph[Any]:
        tools = self.make_tools()
        llm = llm_config.new_chat_model()
        model = self._bind_tools(llm, tools, parallel_tool_calls=agent_config.parallel_tool_calls)

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
            last_df = state.get("last_df")
            last_dbt_returncode = state.get("last_dbt_returncode")
            answer_sql = state.get("answer_sql")
            answer_df = state.get("answer_df")

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
                    else:
                        result = tool_obj.invoke(args | {"graph_state": state})
                        ok = True
                        err = None

                        if name == "run_sql" and isinstance(result, dict):
                            if "sql" in result:
                                last_sql = result["sql"]
                            if "df" in result:
                                last_df = result["df"]

                        if name == "submit_answer" and isinstance(result, dict):
                            if result.get("_submit_answer") and "df" in result:
                                answer_sql = result.get("sql")
                                answer_df = result.pop("df")

                        if name == "run_dbt":
                            try:
                                parsed = json.loads(result) if isinstance(result, str) else result
                                if "returncode" in parsed:
                                    last_dbt_returncode = parsed["returncode"]
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
            }

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
