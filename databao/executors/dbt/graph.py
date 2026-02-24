from __future__ import annotations

import json
import logging
import re
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, Literal

import pandas as pd
from langchain_core.language_models import BaseChatModel, LanguageModelInput
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_core.runnables import Runnable
from langchain_core.tools import BaseTool, tool
from langchain_openai import ChatOpenAI
from langgraph.constants import END, START
from langgraph.graph import add_messages
from langgraph.graph.state import CompiledStateGraph, StateGraph
from langgraph.prebuilt import InjectedState
from typing_extensions import TypedDict

from databao.configs import llm
from databao.configs.agent import AgentConfig
from databao.configs.llm import LLMConfig
from databao.core import Domain
from databao.executors.dbt.dbt_runner import (
    PostDbtRunHook,
    noop_post_run_hook,
    run_dbt_subprocess,
)
from databao.executors.dbt.query_runner import QueryRunnerFactory
from databao.executors.query_expansion import QueryExpansionConfig
from databao.executors.tools import make_search_context_tool

logger = logging.getLogger(__name__)

_MAX_DBT_APPLY_RETRIES = 3
_MAX_HEAL_RETRIES = 10


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
    answer_text: str | None
    answer_df: pd.DataFrame | None
    dbt_dirty: bool
    phase: str
    needs_dbt_changes: bool
    dbt_apply_attempts: int
    dbt_error_context: str | None
    heal_attempts: int
    initial_dbt_error: str | None


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
    """Workflow + ReAct hybrid graph for dbt project interaction.

    Flow:
        START → initial_dbt_run → react_llm ⇄ react_tools → assess_dbt_changes
              → [needs_changes] → apply_dbt_llm ⇄ apply_dbt_tools → submit_answer → END
              → [no_changes] → submit_answer → END
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
            answer_text=None,
            answer_df=None,
            dbt_dirty=dbt_dirty,
            phase="initial_dbt_run",
            needs_dbt_changes=False,
            dbt_apply_attempts=0,
            dbt_error_context=None,
            heal_attempts=0,
            initial_dbt_error=None,
        )

    def get_result(self, state: DbtAgentState) -> dict[str, Any]:
        last_ai: AIMessage | None = next((m for m in reversed(state["messages"]) if isinstance(m, AIMessage)), None)
        _answer_df = state.get("answer_df")
        _answer_sql = state.get("answer_sql")
        result_df = _answer_df if _answer_df is not None else state.get("last_df")
        result_sql = _answer_sql if _answer_sql is not None else state.get("last_sql")
        result_text = state.get("answer_text") or (last_ai.text if last_ai else "")

        return {
            "text": result_text,
            "code": result_sql,
            "df": result_df,
            "messages": state["messages"],
            "tool_calls": state["tool_calls_log"],
            "answer_submitted": state.get("answer_df") is not None,
        }

    def _make_analysis_tools(self, domain: Domain) -> list[BaseTool]:
        """Tools for the analysis ReAct loop: read-only exploration + finalize."""
        tools: list[BaseTool] = [
            self._make_run_sql(),
            self._make_read_tool(),
            self._make_grep_tool(),
            self._make_finalize_analysis(),
        ]
        search_tool = make_search_context_tool(
            domain,
            expansion_llm=self._expansion_llm,
            expansion_config=self._expansion_config,
        )
        if search_tool is not None:
            tools.append(search_tool)
        return tools

    def _make_dbt_tools(self, domain: Domain) -> list[BaseTool]:
        """Tools for the dbt-apply ReAct loop: file ops + dbt commands."""
        tools: list[BaseTool] = [
            self._make_run_sql(),
            self._make_run_dbt(),
            self._make_dbt_deps(),
            self._make_read_tool(),
            self._make_write_tool(),
            self._make_edit_tool(),
            self._make_grep_tool(),
        ]
        search_tool = make_search_context_tool(
            domain,
            expansion_llm=self._expansion_llm,
            expansion_config=self._expansion_config,
        )
        if search_tool is not None:
            tools.append(search_tool)
        return tools

    def _make_run_sql(self) -> BaseTool:
        factory = self._query_runner_factory

        @tool(parse_docstring=True)
        def run_sql(sql: str, sample_rows: int = 5) -> dict[str, Any]:
            """Run a SQL query against the database.

            Args:
                sql: SQL query
                sample_rows: number of rows to include in the sample

            Returns:
                JSON with keys: schema, row_count, sample_rows, truncated
            """
            if factory is None:
                return {"error": "Query runner factory not provided."}
            sql_stripped = sql.strip().rstrip(";")
            if re.search(r"\bATTACH\b", sql_stripped, re.IGNORECASE):
                return {"error": "Do NOT use ATTACH in run_sql. The database is already attached."}
            runner = factory()
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

        return run_sql

    def _make_run_dbt(self) -> BaseTool:
        hook = self._post_dbt_run_hook

        @tool(parse_docstring=True)
        def run_dbt(
            project_dir: str | None,
            timeout: int | None,
            graph_state: Annotated[DbtAgentState, InjectedState],
        ) -> str:
            """Run dbt run to compile models and update the database.

            Args:
                project_dir: Optional override; if omitted uses context project_dir
                timeout: Optional override; if omitted uses context dbt_timeout_seconds

            Returns:
                JSON with keys: returncode, stdout_tail, stderr_tail, timeout
            """
            ctx = graph_state["context"]
            result = run_dbt_subprocess(
                command="run",
                project_dir=str(ctx.project_dir if project_dir is None else Path(project_dir)),
                timeout=ctx.dbt_timeout_seconds if timeout is None else int(timeout),
                post_run_hook=hook,
            )
            return _json_dumps(result)

        return run_dbt

    def _make_dbt_deps(self) -> BaseTool:
        @tool(parse_docstring=True)
        def dbt_deps(project_dir: str | None, graph_state: Annotated[DbtAgentState, InjectedState]) -> str:
            """Run dbt deps to install dependencies.

            Args:
                project_dir: Optional override

            Returns:
                JSON with keys: returncode, stdout_tail, stderr_tail
            """
            ctx = graph_state["context"]
            result = run_dbt_subprocess(
                command="deps",
                project_dir=str(ctx.project_dir if project_dir is None else Path(project_dir)),
                post_run_hook=noop_post_run_hook,
                stdout_tail_lines=20,
                stderr_tail_lines=20,
            )
            return _json_dumps(result)

        return dbt_deps

    def _make_read_tool(self) -> BaseTool:
        @tool(parse_docstring=True)
        def read_tool(path: str, graph_state: Annotated[DbtAgentState, InjectedState]) -> str:
            """Read a file.

            Args:
                path: absolute path OR relative to dbt project directory

            Returns:
                File content (truncated if too large)
            """
            p = Path(path)
            if not p.is_absolute():
                p = graph_state["context"].project_dir / p
            try:
                text = p.read_text(encoding="utf-8", errors="replace")
                return text[:20_000] + "\n\n...[truncated]" if len(text) > 20_000 else text
            except Exception as e:
                return f"ERROR: could not read {p}: {e}"

        return read_tool

    def _make_write_tool(self) -> BaseTool:
        @tool(parse_docstring=True)
        def write_tool(path: str, content: str, graph_state: Annotated[DbtAgentState, InjectedState]) -> str:
            """Write file.

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
                return f"ERROR: file {p_str} is pre-existing and cannot be overwritten."
            try:
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text(content, encoding="utf-8")
                return f"WROTE {p_str} ({len(content)} chars)"
            except Exception as e:
                return f"ERROR: write failed: {e}"

        return write_tool

    def _make_edit_tool(self) -> BaseTool:
        @tool(parse_docstring=True)
        def edit_tool(
            path: str,
            original: str,
            replacement: str,
            graph_state: Annotated[DbtAgentState, InjectedState],
        ) -> str:
            """Edit a file with regex replacement.

            Args:
                path: Path to edit
                original: Original string/pattern (regex)
                replacement: Replacement string

            Returns:
                Summary of the edit operation
            """
            p = Path(path)
            if not p.is_absolute():
                p = graph_state["context"].project_dir / p
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

        return edit_tool

    def _make_grep_tool(self) -> BaseTool:
        @tool(parse_docstring=True)
        def grep_tool(table_name: str, graph_state: Annotated[DbtAgentState, InjectedState]) -> str:
            """Search for a pattern in all project files.

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
            return result[:10_000] + "\n\n...[truncated]" if len(result) > 10_000 else result

        return grep_tool

    def _make_finalize_analysis(self) -> BaseTool:
        """Signal tool: the analysis agent calls this when it has the answer."""
        factory = self._query_runner_factory

        @tool(parse_docstring=True)
        def finalize_analysis(sql: str, answer_text: str) -> dict[str, Any]:
            """Finalize the analysis phase. Call this when you have the answer SQL and text.

            Args:
                sql: The SQL query that produces the answer DataFrame.
                answer_text: Human-readable answer text for the user.

            Returns:
                Confirmation with result preview.
            """
            if factory is None:
                return {"success": False, "error": "Query runner factory not provided."}
            runner = factory()
            try:
                df = runner.execute_to_df(sql)
                return {
                    "success": True,
                    "sql": sql,
                    "answer_text": answer_text,
                    "row_count": len(df),
                    "columns": list(df.columns),
                    "preview": df.head(5).to_dict(orient="records"),
                    "df": df,
                }
            except Exception as e:
                return {"success": False, "error": str(e)}
            finally:
                runner.close()

        return finalize_analysis

    @staticmethod
    def _execute_tools(
        state: DbtAgentState,
        tools: list[BaseTool],
        post_dbt_run_hook: PostDbtRunHook,
    ) -> dict[str, Any]:
        """Execute tool calls from the last AI message. Returns state updates."""
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
        answer_text = state.get("answer_text")
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
                    ok, err = False, result
                elif name == "run_dbt" and not dbt_dirty:
                    result = _json_dumps(
                        {
                            "returncode": 0,
                            "skipped": True,
                            "message": "dbt run skipped — no file changes since last successful run.",
                        }
                    )
                    ok, err = True, None
                else:
                    result = tool_obj.invoke(args | {"graph_state": state})
                    ok, err = True, None

                    if name == "run_sql" and isinstance(result, dict):
                        if "sql" in result:
                            last_sql = result["sql"]
                        if "df" in result:
                            last_df = result["df"]

                    if name == "finalize_analysis" and isinstance(result, dict) and result.get("success"):
                        answer_sql = result.get("sql")
                        answer_text = result.get("answer_text")
                        answer_df = result.get("df")

                    if name in ("write_tool", "edit_tool"):
                        dbt_dirty = True

                    if name == "run_dbt":
                        try:
                            parsed = json.loads(result) if isinstance(result, str) else result
                            if parsed.get("skipped"):
                                pass
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
                ok, err = False, str(e)
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

            content = (
                _json_dumps({k: v for k, v in result.items() if k != "df"}) if isinstance(result, dict) else str(result)
            )
            out_messages.append(ToolMessage(content=content, tool_call_id=tool_call_id))

        return {
            "messages": out_messages,
            "tool_calls_log": tool_log,
            "last_sql": last_sql,
            "last_df": last_df,
            "last_dbt_returncode": last_dbt_returncode,
            "answer_sql": answer_sql,
            "answer_text": answer_text,
            "answer_df": answer_df,
            "dbt_dirty": dbt_dirty,
        }

    def compile(self, model_config: LLMConfig, agent_config: AgentConfig, domain: Domain) -> CompiledStateGraph[Any]:
        analysis_tools = self._make_analysis_tools(domain)
        dbt_tools = self._make_dbt_tools(domain)
        post_hook = self._post_dbt_run_hook

        llm_model = model_config.new_chat_model()
        if llm.is_openai_model(model_config.name):
            react_model = self._model_bind_tools(
                llm_model, analysis_tools, parallel_tool_calls=agent_config.parallel_tool_calls
            )
            apply_model = self._model_bind_tools(
                llm_model, dbt_tools, parallel_tool_calls=agent_config.parallel_tool_calls
            )
        else:
            react_model = self._model_bind_tools(llm_model, analysis_tools)
            apply_model = self._model_bind_tools(llm_model, dbt_tools)

        assess_model = model_config.new_chat_model()

        def initial_dbt_run(state: DbtAgentState) -> dict[str, Any]:
            ctx = state["context"]
            if not state.get("dbt_dirty", True):
                logger.info("dbt project clean, skipping initial run")
                return {
                    "phase": "react",
                    "messages": [SystemMessage(content="[system] dbt project is up-to-date, initial run skipped.")],
                }

            result = run_dbt_subprocess(
                command="run",
                project_dir=str(ctx.project_dir),
                timeout=ctx.dbt_timeout_seconds,
                post_run_hook=post_hook,
            )
            returncode = result.get("returncode", -1)
            summary = _json_dumps(result)
            if returncode == 0:
                logger.info("Initial dbt run succeeded")
                return {
                    "phase": "react",
                    "last_dbt_returncode": 0,
                    "dbt_dirty": False,
                    "messages": [SystemMessage(content=f"[system] Initial dbt run succeeded.\n{summary}")],
                }
            else:
                logger.warning("Initial dbt run failed (rc=%d), entering heal phase", returncode)
                return {
                    "phase": "heal",
                    "last_dbt_returncode": returncode,
                    "dbt_dirty": True,
                    "initial_dbt_error": summary,
                    "messages": [SystemMessage(content=f"[system] Initial dbt run failed (rc={returncode}).\n{summary}")],
                }

        def initial_dbt_router(state: DbtAgentState) -> Literal["react_llm", "heal_dbt_llm"]:
            return "heal_dbt_llm" if state.get("phase") == "heal" else "react_llm"

        def heal_dbt_llm(state: DbtAgentState) -> dict[str, Any]:
            heal_instruction = (
                "[system] The dbt project failed to build. Your job is to FIX it.\n"
                "1. Read the error output above to understand which models failed and why.\n"
                "2. Use `read_tool` and `grep_tool` to inspect the broken model SQL files.\n"
                "3. Use `write_tool` or `edit_tool` to fix the SQL (fix references, missing columns, bad joins).\n"
                "4. Run `run_dbt` to verify the fix.\n"
                "5. If it passes, stop calling tools. If it fails, read the new errors and retry.\n\n"
                "IMPORTANT:\n"
                "- Do NOT modify pre-existing files that are not broken.\n"
                "- If a model fails because a SOURCE TABLE is genuinely missing from the warehouse "
                "(not a dbt model, but a raw table), you CANNOT fix that — just note it and stop.\n"
                "- Focus only on dbt model compilation/runtime errors you can fix by editing SQL."
            )
            messages = list(state["messages"])

            if not any(
                isinstance(m, SystemMessage)
                and "[system] The dbt project failed to build" in (m.content if isinstance(m.content, str) else "")
                for m in messages
            ):
                messages.append(SystemMessage(content=heal_instruction))

            error_ctx = state.get("initial_dbt_error")
            if error_ctx and state.get("heal_attempts", 0) > 0:
                messages.append(
                    SystemMessage(content=f"[system] dbt run still failing after fix attempt. Latest output:\n{error_ctx}")
                )

            response = self._chat(messages, model_config, apply_model)
            return {"messages": [response[-1]]}

        def heal_dbt_tools(state: DbtAgentState) -> dict[str, Any]:
            updates = self._execute_tools(state, dbt_tools, post_hook)
            dbt_rc = updates.get("last_dbt_returncode", state.get("last_dbt_returncode"))
            attempts = state.get("heal_attempts", 0)
            if dbt_rc is not None and dbt_rc != 0:
                tool_msgs = updates.get("messages", [])
                error_text = tool_msgs[-1].content if tool_msgs else ""
                updates["initial_dbt_error"] = error_text
                updates["heal_attempts"] = attempts + 1
            return updates

        def heal_router(state: DbtAgentState) -> Literal["heal_dbt_tools", "react_llm"]:
            last = state["messages"][-1]
            if isinstance(last, AIMessage) and last.tool_calls:
                return "heal_dbt_tools"
            # LLM stopped calling tools — either it fixed things or gave up
            dbt_rc = state.get("last_dbt_returncode")
            if dbt_rc == 0 or not state.get("dbt_dirty", True):
                logger.info("Heal phase succeeded, proceeding to analysis")
                return "react_llm"
            if state.get("heal_attempts", 0) >= _MAX_HEAL_RETRIES:
                logger.warning(
                    "Heal phase exhausted %d retries, proceeding to analysis with broken project",
                    _MAX_HEAL_RETRIES,
                )
                return "react_llm"
            # Still broken but LLM gave up on tools — nudge it back
            return "react_llm"

        def react_llm(state: DbtAgentState) -> dict[str, Any]:
            messages = state["messages"]
            response = self._chat(messages, model_config, react_model)
            return {"messages": [response[-1]], "phase": "react"}

        def react_tools(state: DbtAgentState) -> dict[str, Any]:
            return self._execute_tools(state, analysis_tools, post_hook)

        def react_router(state: DbtAgentState) -> Literal["react_tools", "assess_dbt_changes"]:
            last = state["messages"][-1]
            if isinstance(last, AIMessage) and last.tool_calls:
                return "react_tools"
            return "assess_dbt_changes"

        def assess_dbt_changes(state: DbtAgentState) -> dict[str, Any]:
            answer_sql = state.get("answer_sql") or state.get("last_sql") or ""
            answer_text = state.get("answer_text") or ""

            assess_prompt = (
                "You are deciding whether the analysis just completed warrants creating or modifying "
                "dbt models in the project.\n\n"
                "## Analysis result\n"
                f"Answer text: {answer_text}\n"
                f"Answer SQL:\n```sql\n{answer_sql}\n```\n\n"
                "## Decision criteria\n"
                "Answer ONLY 'yes' or 'no'.\n"
                "Say 'yes' ONLY if:\n"
                "- The transformation encodes reusable business logic (e.g., a metric used across reports)\n"
                "- It involves complex multi-step joins that form a meaningful named dataset\n"
                "- The dbt YAML files reference models that don't have corresponding SQL files yet\n\n"
                "Say 'no' if:\n"
                "- It's a simple ad-hoc query, aggregation, filter, or lookup\n"
                "- The answer is a one-off analysis unlikely to be reused\n"
                "- All referenced models already exist and build correctly\n\n"
                "Answer:"
            )
            response: AIMessage = self._call_model(assess_model, [HumanMessage(content=assess_prompt)])
            needs = "yes" in response.text.lower()
            logger.info("assess_dbt_changes decision: %s", "needs_changes" if needs else "no_changes")
            return {
                "phase": "apply_dbt" if needs else "submit",
                "needs_dbt_changes": needs,
            }

        def assess_router(state: DbtAgentState) -> Literal["apply_dbt_llm", "submit_answer"]:
            return "apply_dbt_llm" if state.get("needs_dbt_changes") else "submit_answer"

        def apply_dbt_llm(state: DbtAgentState) -> dict[str, Any]:
            apply_instruction = (
                "[system] You are now in dbt-apply mode. Your job:\n"
                "1. Create/edit dbt model SQL files and documentation YAML as needed.\n"
                "2. Run `run_dbt` to verify the build passes with 0 errors.\n"
                "3. If run_dbt fails, fix the errors and retry.\n"
                "4. When done (build passing), stop calling tools.\n"
                "Do NOT create models for trivial queries. Follow dbt naming conventions "
                "(stg_, int_, fct_, dim_ prefixes).\n"
                "Do NOT modify pre-existing files."
            )
            messages = list(state["messages"])

            if not any(
                isinstance(m, SystemMessage)
                and "[system] You are now in dbt-apply mode" in (m.content if isinstance(m.content, str) else "")
                for m in messages
            ):
                messages.append(SystemMessage(content=apply_instruction))

            error_ctx = state.get("dbt_error_context")
            if error_ctx:
                messages.append(SystemMessage(content=f"[system] Previous dbt run failed:\n{error_ctx}"))

            response = self._chat(messages, model_config, apply_model)
            return {"messages": [response[-1]], "dbt_error_context": None}

        def apply_dbt_tools(state: DbtAgentState) -> dict[str, Any]:
            updates = self._execute_tools(state, dbt_tools, post_hook)
            dbt_rc = updates.get("last_dbt_returncode", state.get("last_dbt_returncode"))
            attempts = state.get("dbt_apply_attempts", 0)
            if dbt_rc is not None and dbt_rc != 0:
                tool_msgs = updates.get("messages", [])
                error_text = tool_msgs[-1].content if tool_msgs else ""
                updates["dbt_error_context"] = error_text
                updates["dbt_apply_attempts"] = attempts + 1
            return updates

        def apply_router(state: DbtAgentState) -> Literal["apply_dbt_tools", "submit_answer", "react_llm"]:
            last = state["messages"][-1]
            if isinstance(last, AIMessage) and last.tool_calls:
                return "apply_dbt_tools"
            dbt_rc = state.get("last_dbt_returncode")
            if state.get("dbt_dirty", True) and dbt_rc != 0:
                if state.get("dbt_apply_attempts", 0) >= _MAX_DBT_APPLY_RETRIES:
                    logger.warning("dbt apply failed after %d retries, falling back to react", _MAX_DBT_APPLY_RETRIES)
                    return "react_llm"
                return "apply_dbt_tools"
            return "submit_answer"

        def submit_answer(state: DbtAgentState) -> dict[str, Any]:
            sql = state.get("answer_sql")
            if sql is None:
                sql = state.get("last_sql")
            text = state.get("answer_text") or ""
            df = state.get("answer_df")
            if df is None:
                df = state.get("last_df")

            if sql and df is None and self._query_runner_factory is not None:
                runner = self._query_runner_factory()
                try:
                    df = runner.execute_to_df(sql)
                except Exception as e:
                    logger.warning("submit_answer: failed to execute SQL: %s", e)
                    text = text or f"Could not execute final SQL: {e}"
                finally:
                    runner.close()

            return {
                "phase": "done",
                "answer_sql": sql,
                "answer_text": text,
                "answer_df": df,
            }

        graph = StateGraph(DbtAgentState)

        graph.add_node("initial_dbt_run", initial_dbt_run)
        graph.add_node("heal_dbt_llm", heal_dbt_llm)
        graph.add_node("heal_dbt_tools", heal_dbt_tools)
        graph.add_node("react_llm", react_llm)
        graph.add_node("react_tools", react_tools)
        graph.add_node("assess_dbt_changes", assess_dbt_changes)
        graph.add_node("apply_dbt_llm", apply_dbt_llm)
        graph.add_node("apply_dbt_tools", apply_dbt_tools)
        graph.add_node("submit_answer", submit_answer)

        graph.add_edge(START, "initial_dbt_run")
        graph.add_conditional_edges(
            "initial_dbt_run",
            initial_dbt_router,
            {
                "react_llm": "react_llm",
                "heal_dbt_llm": "heal_dbt_llm",
            },
        )
        # Heal sub-loop
        graph.add_conditional_edges(
            "heal_dbt_llm",
            heal_router,
            {
                "heal_dbt_tools": "heal_dbt_tools",
                "react_llm": "react_llm",
            },
        )
        graph.add_edge("heal_dbt_tools", "heal_dbt_llm")
        # Analysis loop
        graph.add_conditional_edges(
            "react_llm",
            react_router,
            {
                "react_tools": "react_tools",
                "assess_dbt_changes": "assess_dbt_changes",
            },
        )
        graph.add_edge("react_tools", "react_llm")
        # Assessment gate
        graph.add_conditional_edges(
            "assess_dbt_changes",
            assess_router,
            {
                "apply_dbt_llm": "apply_dbt_llm",
                "submit_answer": "submit_answer",
            },
        )
        # Apply sub-loop
        graph.add_conditional_edges(
            "apply_dbt_llm",
            apply_router,
            {
                "apply_dbt_tools": "apply_dbt_tools",
                "submit_answer": "submit_answer",
                "react_llm": "react_llm",
            },
        )
        graph.add_edge("apply_dbt_tools", "apply_dbt_llm")
        graph.add_edge("submit_answer", END)

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
            model = config.new_chat_model()
        messages = DbtProjectGraph._apply_system_prompt_caching(config, messages)
        response: AIMessage = DbtProjectGraph._call_model(model, messages)
        return [*messages, response]

    @staticmethod
    def _is_anthropic_model(config: LLMConfig) -> bool:
        return "claude" in config.name.lower()

    @staticmethod
    def _apply_system_prompt_caching(config: LLMConfig, messages: list[BaseMessage]) -> list[BaseMessage]:
        if not (config.cache_system_prompt and DbtProjectGraph._is_anthropic_model(config)):
            return messages
        assert all(m.type != "system" for m in messages[1:])
        if messages[0].type == "system":
            messages = [DbtProjectGraph._set_message_cache_breakpoint(config, messages[0]), *messages[1:]]
        return messages

    @staticmethod
    def _set_message_cache_breakpoint(config: LLMConfig, message: BaseMessage) -> BaseMessage:
        if not DbtProjectGraph._is_anthropic_model(config):
            return message
        new_content: list[dict[str, Any] | str]
        match message.content:
            case str() | dict():
                new_content = [DbtProjectGraph._set_anthropic_cache_breakpoint(message.content)]
            case list():
                new_content = message.content.copy()
                new_content[-1] = DbtProjectGraph._set_anthropic_cache_breakpoint(new_content[-1])
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
