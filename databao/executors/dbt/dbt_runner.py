from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import duckdb

PostDbtRunHook = Callable[[Path], None]

_EXCLUDED_DIR_NAMES: frozenset[str] = frozenset(
    {
        "target",
        "dbt_packages",
        "dbt_modules",
        "logs",
        ".venv",
        ".git",
        "node_modules",
        "__pycache__",
        ".mypy_cache",
        ".pytest_cache",
    }
)

_MAX_SUMMARY_FILES = 1000


def duckdb_post_run_hook(project_dir: Path) -> None:
    """Flush DuckDB WAL files so read-only connections see latest data."""
    for db_file in project_dir.rglob("*.duckdb"):
        try:
            con = duckdb.connect(str(db_file))
            con.execute("CHECKPOINT")
            con.close()
        except Exception:
            # If we can't checkpoint (e.g. lock held), WAL will be applied
            # on the next read-only ATTACH automatically by DuckDB.
            pass


def noop_post_run_hook(project_dir: Path) -> None:
    """No-op hook for databases that don't need post-run cleanup."""


def run_dbt_subprocess(
    *,
    command: str,
    project_dir: str,
    timeout: int | None = None,
    post_run_hook: PostDbtRunHook = noop_post_run_hook,
    stdout_tail_lines: int = 200,
    stderr_tail_lines: int = 200,
) -> dict[str, Any]:
    """Run a dbt CLI command via subprocess and return structured results.

    Args:
        command: The dbt command to run (e.g. "run", "deps").
        project_dir: Working directory for the subprocess.
        timeout: Timeout in seconds; None means no timeout.
        post_run_hook: Called after successful subprocess completion.
        stdout_tail_lines: Number of trailing stdout lines to capture.
        stderr_tail_lines: Number of trailing stderr lines to capture.

    Returns:
        Dict with keys: returncode, stdout_tail, stderr_tail, timeout.
    """
    try:
        proc = subprocess.run(
            [sys.executable, "-m", "dbt.cli.main", command],
            cwd=project_dir,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {"timeout": True}

    post_run_hook(Path(project_dir))

    return {
        "returncode": int(proc.returncode),
        "stdout_tail": "\n".join((proc.stdout or "").splitlines()[-stdout_tail_lines:]),
        "stderr_tail": "\n".join((proc.stderr or "").splitlines()[-stderr_tail_lines:]),
        "timeout": False,
    }


def assemble_dbt_project_summary(project_dir: Path) -> str:
    """Build a compact tree-style overview of the dbt project structure.

    Returns a directory tree listing all files with their sizes — no file
    contents are read.  The agent can later call ``read_tool(path)`` for any
    file it actually needs.

    Directories listed in ``_EXCLUDED_DIR_NAMES`` (e.g. target, dbt_packages,
    logs) are skipped to avoid bloating the system prompt after dbt deps/run.
    The listing is also capped at ``_MAX_SUMMARY_FILES`` entries.
    """
    if not project_dir or not project_dir.exists():
        return f"DBT project directory not found at {project_dir}"

    files: list[tuple[Path, int]] = []
    for dirpath, dirnames, filenames in os.walk(project_dir):
        dirnames[:] = [d for d in dirnames if d not in _EXCLUDED_DIR_NAMES]

        for fname in sorted(filenames):
            fpath = Path(dirpath) / fname
            rel = fpath.relative_to(project_dir)
            try:
                size = fpath.stat().st_size
            except Exception:
                size = -1
            files.append((rel, size))
            if len(files) >= _MAX_SUMMARY_FILES:
                break
        if len(files) >= _MAX_SUMMARY_FILES:
            break

    files.sort()

    if not files:
        return f"DBT project directory present at {project_dir} but no files found."

    truncated = len(files) >= _MAX_SUMMARY_FILES
    lines: list[str] = [f"dbt project structure ({len(files)}{'+ (truncated)' if truncated else ''} files):"]
    for rel, size in files:
        size_str = f"{size} bytes" if size >= 0 else "unknown size"
        lines.append(f"  {rel}  ({size_str})")

    if truncated:
        lines.append(
            f"\n  ... listing capped at {_MAX_SUMMARY_FILES} files. Use read_tool / grep_tool to explore further."
        )

    lines.append("")
    lines.append("Use read_tool(path) to inspect any file contents as needed.")
    return "\n".join(lines)
