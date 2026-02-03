import glob
import json
import logging
import os
import re
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass
from functools import wraps
from pathlib import Path
from typing import Any, ParamSpec, TypeVar

import jinja2
from langchain.agents import create_agent
from langchain.tools import ToolRuntime
from langchain_core.messages import messages_to_dict
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from langgraph.graph.state import CompiledStateGraph
from langsmith import trace

from dbt_agent_tmp.utils import db_introspect

logging.getLogger("httpx").setLevel(logging.WARNING)


# lightweight in-memory tool-call log for the current agent invocation
_TOOL_CALLS_LOG: list[dict] = []

Param = ParamSpec("Param")
RetType = TypeVar("RetType")


@dataclass
class DBTProjectContext:
    pre_existing_files: list[str]


def record_tool_call(tool_name: str) -> Callable[[Callable[Param, RetType]], Callable[Param, RetType]]:
    """Decorator to record basic telemetry for tool calls."""

    def decorator(fn: Callable[Param, RetType]) -> Callable[Param, RetType]:
        @wraps(fn)
        def wrapper(*args: Param.args, **kwargs: Param.kwargs) -> RetType:
            start = time.time()
            fn_exception: Exception | None = None
            try:
                result = fn(*args, **kwargs)
                success = True
                error = None
            except Exception as e:
                result = None
                success = False
                error = str(e)
                fn_exception = e
            end = time.time()
            meta = {
                "tool": tool_name,
                "start": start,
                "end": end,
                "duration": end - start,
                "args_len": len(args),
                "kwargs_len": len(kwargs),
                "success": success,
                "error": error,
                "result_type": type(result).__name__ if result is not None else None,
                "result_summary_len": len(str(result)) if result is not None else 0,
            }
            _TOOL_CALLS_LOG.append(meta)

            if fn_exception is not None:
                # re-raise to preserve original behavior
                raise fn_exception
            return result  # type: ignore[return-value]

        return wrapper

    return decorator


# -------------------------
# Tool implementations
# -------------------------


def init_run_sql(db_conn: Any):
    @tool
    @record_tool_call("run_sql")
    def run_sql(sql: str, sample_rows: int = 5) -> str:
        """
        Run a SQL query against the DuckDB database and return a compact JSON summary.

        Returns JSON with keys: schema (name+dtype), row_count, sample_rows (list), truncated (bool)
        """
        if db_conn is None:
            return f"ERROR: database connection should be specified."

        try:
            df = db_conn.execute(sql).fetchdf()

            schema = [{"name": c, "dtype": str(dt)} for c, dt in zip(df.columns, df.dtypes, strict=True)]
            row_count = len(df)
            sample = df.head(sample_rows).to_dict(orient="records")
            result = {
                "schema": schema,
                "row_count": row_count,
                "sample_rows": sample,
                "truncated": row_count > sample_rows,
            }
            return json.dumps(result, default=str)
        except Exception as e:
            return f"ERROR: {e}"
    return run_sql


@tool
@record_tool_call("run_dbt")
def run_dbt(project_dir: str, timeout: int = 300) -> str:
    """
    Run a dbtv2 project to update the state of the database and return a compact structured result.
    """
    try:
        proc = subprocess.run(
            ["dbtv2", "run"],
            cwd=project_dir,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return json.dumps({"timeout": True})

    stdout_tail = "\n".join(proc.stdout.splitlines()[-200:])
    stderr_tail = "\n".join(proc.stderr.splitlines()[-200:])
    output = {
        "returncode": proc.returncode,
        "stdout_tail": stdout_tail,
        "stderr_tail": stderr_tail,
    }
    return json.dumps(output, default=str)


@tool
@record_tool_call("run_dbt_with_summary")
def run_dbt_with_summary(project_dir: str, timeout: int = 300) -> str:
    """
    Run a dbtv2 project to update the state of the database and return a compact structured result.
    """
    try:
        proc = subprocess.run(
            ["dbtv2", "run"],
            cwd=project_dir,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return json.dumps({"timeout": True})
    except Exception as e:
        return json.dumps({"error": str(e)})

    full_stdout = proc.stdout or ""
    full_stderr = proc.stderr or ""

    llm_summary = ""
    try:
        system_message = (
            "You are a data scientist specializing in databases, SQL, DuckDB, and dbtv2. "
            "You are given the stdout and stderr of a `dbtv2 run` command. "
            "Your task is to summarize the results of the dbtv2 run command. "
            "Provide a short summary of completed tests (if any) "
            "and list the most important errors with short explanations. "
            "Initially, the dbtv2 project was missing some `.sql` files only. "
            "Provided output is the result of running `dbtv2 run` after adding the missing `.sql` files. "
            "Briefly suggest improvements on adding more models to the dbtv2 project or fixing specific ones if necessary. "
            "Prefer adding new models over modifying YML files as they are expected to be complete."
        )
        user_message = f"Summarize the dbtv2 run results below. \n\nSTDOUT:\n{full_stdout}\n\nSTDERR:\n{full_stderr}"
        llm = ChatOpenAI(model="gpt-5", temperature=0.0, reasoning={"effort": "high"}, verbosity="low")
        # Call the LLM with a plain string prompt; return only the text summary.
        try:
            messages = [("system", system_message), ("human", user_message)]
            llm_summary = llm.invoke(messages).text
        except Exception as inner_exc:
            llm_summary = f"Summarization failed: {inner_exc}"
    except Exception as e:
        llm_summary = f"Setup failed: {e}"

    output = {
        "returncode": proc.returncode,
        "summary": llm_summary,
    }
    return json.dumps(output, default=str)


@tool
@record_tool_call("dbt_deps")
def dbt_deps(project_dir: str) -> str:
    """
    Run a dbtv2 deps command to update dependencies of the dbtv2 project when failing to run run_dbt tool.

    Args:
        project_dir: The directory of the dbtv2 project

    Returns:
        A string containing the output of the dbtv2 deps command
    """
    try:
        proc = subprocess.run(
            ["dbtv2", "deps"],
            cwd=project_dir,
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception as e:
        return json.dumps({"error": str(e)})

    stdout_tail = "\n".join(proc.stdout.splitlines()[-20:])
    stderr_tail = "\n".join(proc.stderr.splitlines()[-20:])
    return json.dumps(
        {"returncode": proc.returncode, "stdout_tail": stdout_tail, "stderr_tail": stderr_tail}, default=str
    )


@tool
@record_tool_call("read_tool")
def read_tool(path: str) -> str:
    """
    Read a file (text).

    Args:
        path: The path to read

    Returns:
        A string containing the file content
    """
    try:
        with open(path, encoding="utf-8") as f:
            text = f.read()
        # keep output reasonably sized
        if len(text) > 20000:
            return text[:20000] + "\n\n...[truncated]"
        return text
    except Exception as e:
        return f"ERROR: could not read {path}: {e}"


@tool
@record_tool_call("write_tool")
def write_tool(path: str, content: str, runtime: ToolRuntime[DBTProjectContext]) -> str:
    """
    Write file.

    Args:
        path: The path to write. IMPORTANT: Provide an absolute path to the file location.
        content: The content to write

    Returns:
        A string containing the summary of the write operation.
    """
    if path in runtime.context.pre_existing_files:
        return f"ERROR: file {path} exists within the project from the beginning. You can only create new files / overwrite files you create yourself. If you need to edit a file - use the edit tool."
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        return f"WROTE {path} ({len(content)} symbols)"
    except Exception as e:
        return f"ERROR: write failed: {e}"


@tool
@record_tool_call("edit_tool")
def edit_tool(path: str, original: str, replacement: str) -> str:
    """
    Edit a file with a single replacement.

    Args:
        path: The path to edit
        original: The original string to replace (interpreted as a regex)
        replacement: The replacement string

    Returns:
        A string containing the summary of the edit operation
    """
    try:
        if not os.path.exists(path):
            return f"ERROR: file {path} not found"
        with open(path, encoding="utf-8") as f:
            text = f.read()
        pattern = re.sub(r"\(", r"\\(", original)
        pattern = re.sub(r"\)", r"\\)", pattern)
        new_text, n = re.subn(pattern, replacement, text)
        with open(path, "w", encoding="utf-8") as f:
            f.write(new_text)
        return f"EDITED {path}: {n} replacements"
    except re.error as re_err:
        return f"ERROR: regex failed: {re_err}"
    except Exception as e:
        return f"ERROR: edit failed: {e}"


# def multiedit_tool(payload: str) -> str:
#     """
#     Apply multiple edits to a file.
#     Payload format (simple):
#       path\n\n<edit1 pattern>::::<edit1 replacement>\n<edit2 pattern>::::<edit2 replacement>\n...
#     Lines after blank line are 'pattern::::replacement'.
#     """
#     try:
#         if "\n\n" not in payload:
#             return "ERROR: expected payload format: path\\n\\npattern::::replacement..."
#         path, edits_block = payload.split("\n\n", 1)
#         if not os.path.exists(path):
#             return f"ERROR: file {path} not found"
#         with open(path, "r", encoding="utf-8") as f:
#             text = f.read()
#         total_changes = 0
#         for line in edits_block.strip().splitlines():
#             if "::::" not in line:
#                 continue
#             pattern, replacement = line.split("::::", 1)
#             text, n = re.subn(pattern, replacement, text)
#             total_changes += n
#         with open(path, "w", encoding="utf-8") as f:
#             f.write(text)
#         return f"MULTIEDIT {path}: {total_changes} total replacements"
#     except Exception as e:
#         return f"ERROR: multiedit failed: {e}"


def init_run_database_explore(db_conn: Any):
    @tool
    @record_tool_call("run_database_explore")
    def run_database_explore(project_dir: str) -> str:
        """
        Explore the provided database before any transformations. Useful to determine if the information
        needed to build the database objects (tables / columns) that directly solve the task posed by the user, is
        already present in the database. If not directly present, then intermediate tables/columns need to be built using
        the current data as a starting point.

        Args:
            project_dir: The directory of the dbtv2 project

        Returns:
            A markdown representation of the schema of the provided database.
        """
        project_dir = Path(project_dir)
        match = re.search(r"\d{3}(?:$|_)", project_dir.name)
        if match is None:
            return f"ERROR: could not extract task number from project name: {project_dir.name}"
        try:
            schema = db_introspect(db_conn).to_markdown()
        except Exception as e:
            return f"ERROR: could not introspect database: {e}"
        return schema
    return run_database_explore


@tool
@record_tool_call("grep_tool")
def grep_tool(table_name: str) -> str:
    """
    Grep-like functionality to search for a pattern in all files in the current directory and its subdirectories.

    Args:
        table_name: The pattern to grep

    Returns:
        A string containing the matched lines with filename:line:text
    """
    try:
        matches = []
        files = Path(os.getcwd()).rglob("*")
        cre = re.compile(rf"\b{table_name}\b")
        for fpath in files:
            if not os.path.isfile(fpath):
                continue
            with open(fpath, encoding="utf-8", errors="ignore") as fh:
                for i, line in enumerate(fh, start=1):
                    if cre.search(line):
                        matches.append(f"{fpath}:{i}:{line.rstrip()}")
        result = "\n".join(matches) if matches else "(no matches)"
        # Limit output to 10000 characters
        if len(result) > 10000:
            return result[:10000] + "\n\n...[truncated]"
        return result
    except re.error as re_err:
        return f"ERROR: regex error: {re_err}"
    except Exception as e:
        return f"ERROR: grep failed: {e}"


# -------------------------
# Agent construction
# -------------------------


def assemble_dbt_project_summary(project_dir: Path, max_file_chars: int | None = 8000) -> str:
    """Deterministically gather important dbtv2 project files into a single string.

    The function looks for `dbt_project.yml`, model schema YAMLs under `models/`,
    SQL model files under `models/`, macros, and seeds. Files are read in a
    stable, sorted order and truncated if they exceed `max_file_chars` per file.
    """
    parts: list[str] = []
    if not project_dir or not project_dir.exists():
        return f"DBT project directory not found at {project_dir}"
    # deterministic patterns and order
    patterns = [
        "dbt_project.yml",
        "models/**/*.yml",
        "models/**/*.yaml",
        "models/**/*.sql",
        "macros/**/*.sql",
        "seeds/**/*.csv",
        "*.md",
    ]
    seen_paths: dict[str, Path] = {}
    for pat in patterns:
        for p in sorted(project_dir.glob(pat)):
            # use resolved path string as key to dedupe
            key = str(p.resolve())
            if key not in seen_paths:
                seen_paths[key] = p
    # sort by relative path to project_dir for deterministic ordering
    files = sorted(seen_paths.values(), key=lambda p: str(p.relative_to(project_dir)))
    for p in files:
        try:
            text = p.read_text(errors="replace")
        except Exception as exc:  # pragma: no cover - IO/read failure
            text = f"<failed to read file {p}: {exc}>"
        if max_file_chars is not None and len(text) > max_file_chars:
            text = text[:max_file_chars] + "\n...TRUNCATED..."
        parts.append(f"### PATH: {p.relative_to(project_dir)} ###\n{text}\n")

    # Now, walk the project_dir recursively. Any file not already in seen_paths is simply listed with its size
    other_files: list[tuple[Path, int]] = []
    for dirpath, _dirnames, filenames in os.walk(project_dir):
        for fname in sorted(filenames):
            fpath = Path(dirpath) / fname
            key = str(fpath.resolve())
            if key not in seen_paths:
                try:
                    size = fpath.stat().st_size
                except Exception:
                    size = -1
                other_files.append((fpath.relative_to(project_dir), size))
    if other_files:
        listing_lines = [
            f"### NON-SUMMARIZED FILE: {f} (size: {size} bytes)"
            if size >= 0
            else f"### NON-SUMMARIZED FILE: {f} (size: unknown)"
            for f, size in sorted(other_files)
        ]
        parts.append("\n".join(listing_lines))
    if not parts:
        return f"DBT project directory present at {project_dir} but no matching files found under models/, macros/, or seeds/."
    header = (
        f"Assembled dbtv2 project files from {project_dir}:\n"
        f"Found {len(files)} files with content. "
        f"Listed {len(other_files)} other files by size.\n"
    )
    return header + "\n".join(parts)


def build_agent(
    model: str,
    temperature: float = 0.0,
    name: str = "langchain_agent",
    db_conn: Any = None,
) -> CompiledStateGraph:
    """
    Construct a LangChain agent using the provided model.

    Args:
        model: The LLM model name to use (required). Examples: "gpt-5", "claude-3-opus-20240229", "together_ai/Qwen/Qwen2.5-72B-Instruct-Turbo"
        allow_bash: Whether to allow bash commands
        temperature: Temperature for the model
        name: Name of the agent
    """
    # Log which model is being used for verification
    logging.info(f"Using LLM model: {model}")
    print(f"🔍 DEBUG: Initializing agent with model: {model}")

    # TODO: https://github.com/JetBrains/spider-2.0/issues/108: Consolidate LLM handling
    # Route to appropriate LangChain wrapper based on model provider
    if model.startswith("claude"):
        # Use universal init_chat_model for Anthropic models
        try:
            from langchain.chat_models import init_chat_model

            llm = init_chat_model(
                model,
                temperature=temperature,
            )
            print(f"✅ Using init_chat_model (universal wrapper) for model: {model}")
        except ImportError as e:
            raise ImportError(
                f"Failed to initialize model {model}. Make sure langchain-anthropic package is installed."
            ) from e
    elif model.startswith("gpt-") or model.startswith("o1-") or model.startswith("o3-"):
        # Use ChatOpenAI directly for OpenAI models (to support reasoning parameter)
        is_reasoning = "o1" in model or "o3" in model or "gpt-5" in model
        llm = ChatOpenAI(
            model=model,
            temperature=temperature,
            reasoning={"effort": "high"} if is_reasoning else None,
            verbosity="low",
        )
        print(f"✅ Using ChatOpenAI for model: {model}")
    elif model.startswith("together_ai/") or "/" in model:
        # Use ChatLiteLLM for Together AI models
        try:
            from langchain_litellm import ChatLiteLLM

            llm = ChatLiteLLM(
                model=model,
                temperature=temperature,
            )
            print(f"✅ Using ChatLiteLLM for model: {model}")
        except ImportError as e:
            raise ImportError(
                "langchain-litellm is required for Together AI models. Install it with: pip install langchain-litellm"
            ) from e
    else:
        raise ValueError(
            f"Unsupported model: {model}. "
            "Supported models: OpenAI (gpt-*, o1-*, o3-*), Anthropic (claude-*), "
            "Together AI (together_ai/...), or other providers via litellm (provider/model)."
        )

    tools = [
        init_run_sql(db_conn),
        run_dbt,
        dbt_deps,
        read_tool,
        write_tool,
        edit_tool,
        init_run_database_explore(db_conn),
        grep_tool,
    ]

    agent: CompiledStateGraph[Any] = create_agent(llm, tools=tools, name=name, context_schema=DBTProjectContext)  # type: ignore[arg-type]
    return agent


def read_prompt_template(relative_path: Path) -> jinja2.Template:
    env = jinja2.Environment(
        loader=jinja2.PackageLoader("dbt_agent_tmp", ""),
        trim_blocks=True,  # better whitespace handling
        lstrip_blocks=True,
    )
    template = env.get_template(str(relative_path))
    return template


class LangchainAgent:
    def __init__(
        self,
        project_dir: Path | str,
        temperature: float = 0.0,
        name: str = "langchain_agent",
        system_prompt_name: str = "system_prompt.jinja",
        model: str | None = None,
        db_conn: Any = None,
    ):
        self.project_dir = project_dir if isinstance(project_dir, Path) else Path(project_dir)
        self.temperature = temperature
        self.name = name
        self.system_prompt_name = system_prompt_name
        # Ensure model is always a string (required for build_agent)
        self.model: str = model if model is not None else os.getenv("LLM_MODEL", "gpt-5")
        self.system_prompt = self.make_system_prompt()
        self.agent = build_agent(model=self.model, db_conn=db_conn)

    def make_system_prompt(self) -> str:
        dbt_overview = assemble_dbt_project_summary(self.project_dir)
        template = read_prompt_template(Path(self.system_prompt_name))
        system_prompt = template.render(dbt_overview=dbt_overview, dbt_directory=self.project_dir.absolute())
        return system_prompt

    def run(self, prompt: str | dict[str, Any]) -> dict[str, Any]:
        # reset in-memory tool call log for this invocation
        _TOOL_CALLS_LOG.clear()

        if isinstance(prompt, dict):
            start_state = prompt
        else:
            start_state = {
                "messages": [
                    {"role": "system", "content": self.system_prompt},
                    {
                        "role": "user",
                        "content": "Complete the dbtv2 project. Make sure that the project builds successfully! "
                        "And then answer the following question.\n\n" + prompt,
                    },
                ]
            }

        with trace(name=self.name):
            result = self.agent.invoke(
                start_state,
                {"recursion_limit": 400},
                context=DBTProjectContext(pre_existing_files=list(set([str(i) for i in self.project_dir.rglob("*")]))),  # type: ignore
            )

        output: dict[str, Any] = {}

        found_outputs = glob.glob(f"{self.project_dir}/*.duckdb", recursive=True)
        if found_outputs:
            output["answer_or_path"] = found_outputs[0].split("/")[-1]
            output["answer_type"] = "file"
        else:
            output["answer_or_path"] = ""
            output["answer_type"] = "answer"

        output["agent_trajectory"] = messages_to_dict(result["messages"])

        # attach recorded tool calls and attempt to aggregate usage metadata
        output["tool_calls"] = _TOOL_CALLS_LOG.copy()

        # aggregate usage metadata if present in result messages
        usage_agg: dict[str, int] = {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
        try:
            for m in result.get("messages", []):
                # msg may contain usage at different nested keys depending on agent
                data = m.get("data") if isinstance(m, dict) else None
                um = None
                if data and isinstance(data, dict):
                    um = data.get("usage_metadata") or data.get("response_metadata") or data.get("usage")
                if not um and isinstance(m, dict):
                    um = m.get("usage_metadata")
                if isinstance(um, dict):
                    for k in ("input_tokens", "output_tokens", "total_tokens"):
                        try:
                            usage_agg[k] += int(um.get(k, 0) or 0)
                        except Exception:
                            pass
        except Exception:
            pass

        output["usage_metadata"] = usage_agg

        output_path = self.project_dir / "output.json"
        with open(output_path, "w") as f:
            json.dump(output, f, indent=4)

        return output
