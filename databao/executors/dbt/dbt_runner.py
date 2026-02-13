from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import duckdb

# Post-run hook type alias (project convention: simple Callable aliases)
PostDbtRunHook = Callable[[Path], None]


def duckdb_post_run_hook(project_dir: Path) -> None:
    """Flush DuckDB WAL files so read-only connections see latest data."""
    for db_file in project_dir.rglob("*.duckdb"):
        try:
            con = duckdb.connect(str(db_file))
            con.execute("CHECKPOINT")
            con.close()
        except Exception:
            pass  # Best effort


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


def assemble_dbt_project_summary(project_dir: Path, max_file_chars: int | None = 8000) -> str:
    """Deterministically gather important dbt project files into a single string.

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
        return (
            f"DBT project directory present at {project_dir} "
            "but no matching files found under models/, macros/, or seeds/."
        )
    header = (
        f"Assembled dbt project files from {project_dir}:\n"
        f"Found {len(files)} files with content. "
        f"Listed {len(other_files)} other files by size.\n"
    )
    return header + "\n".join(parts)
