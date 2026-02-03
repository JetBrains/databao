from __future__ import annotations

import hashlib
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from databao.dbt.config import DbtConfig
from databao.dbt.agent import DbtAgent
from databao.dbt.errors import DbtPlanNotReadyError, DbtApplyNotAllowedError


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _snapshot_tree(root: Path) -> dict[Path, str]:
    root = root.resolve()
    out: dict[Path, str] = {}
    for p in sorted(root.rglob("*")):
        if p.is_file():
            out[p.relative_to(root)] = _sha256_file(p)
    return out


def _resolve_dbt_project_root(path: Path) -> Path:
    """
    Accept either:
      - a dbt project root (contains dbt_project.yml), or
      - a container dir that contains exactly one dbt project root among its direct children.
    """
    path = path.resolve()

    if (path / "dbt_project.yml").exists():
        return path

    candidates = [p for p in path.iterdir() if p.is_dir() and (p / "dbt_project.yml").exists()]
    if len(candidates) == 1:
        return candidates[0]

    raise RuntimeError(
        f"DbtConfig.project_dir must point to a dbt project root (dbt_project.yml), "
        f"or a directory containing exactly one dbt project. Got: {path}"
    )


def _make_staging_dir(dbt_config: DbtConfig, *, source_project_dir: Path) -> Path:
    source_project_dir = source_project_dir.resolve()

    if dbt_config.staging_mode == "temp":
        root = Path(tempfile.mkdtemp(prefix="databao_dbt_"))
        return root / source_project_dir.name

    if dbt_config.staging_mode == "in_project":
        return source_project_dir / ".databao_dbt_staging"

    if dbt_config.staging_mode == "custom":
        if dbt_config.staging_root is None:
            raise ValueError("DbtConfig.staging_root must be set when staging_mode='custom'")
        return dbt_config.staging_root.resolve() / source_project_dir.name

    raise ValueError(f"Unknown staging_mode: {dbt_config.staging_mode}")


def _compute_change_lists(before: dict[Path, str], after: dict[Path, str]) -> tuple[list[Path], list[Path], list[Path]]:
    before_paths = set(before.keys())
    after_paths = set(after.keys())

    added = sorted(after_paths - before_paths)
    deleted = sorted(before_paths - after_paths)

    modified: list[Path] = []
    for p in sorted(before_paths & after_paths):
        if before[p] != after[p]:
            modified.append(p)

    return added, modified, deleted


class DbtPlan:
    def __init__(self, dbt_config: DbtConfig, thread_meta: dict[str, Any] = {}) -> None:
        self._dbt_config = dbt_config
        self._thread_meta = thread_meta

        self._source_project_dir = _resolve_dbt_project_root(self._dbt_config.project_dir)
        self._staged_project_dir: Path | None = None

        self.added_files: list[Path] = []
        self.modified_files: list[Path] = []
        self.deleted_files: list[Path] = []

        self._agent_output: dict[str, Any] | None = None

        self._before_snapshot: dict[Path, str] | None = None
        self._after_snapshot: dict[Path, str] | None = None

    def _ensure_sandbox(self) -> Path:
        staged = _make_staging_dir(self._dbt_config, source_project_dir=self._source_project_dir)

        if staged.exists():
            shutil.rmtree(staged)

        staged.mkdir(parents=True, exist_ok=True)

        for entry in self._source_project_dir.iterdir():
            dst = staged / entry.name
            if entry.is_dir():
                shutil.copytree(entry, dst)
            else:
                shutil.copy2(entry, dst)

        if not (staged / "dbt_project.yml").exists():
            raise RuntimeError(
                f"Staging directory {staged} does not look like a dbt project root (missing dbt_project.yml)."
            )

        self._staged_project_dir = staged
        self._before_snapshot = _snapshot_tree(staged)

        self._after_snapshot = None
        self.added_files = []
        self.modified_files = []
        self.deleted_files = []
        self._agent_output = None

        return staged

    def run(self, *, model: str | None = None, db_conn: Any = None) -> "DbtPlan":
        """
        Run the dbt project agent against the sandbox and populate change lists.
        (SLOW)
        """
        if self._staged_project_dir is None or self._before_snapshot is None:
            self._ensure_sandbox()

        assert self._staged_project_dir is not None
        assert self._before_snapshot is not None

        dbt_agent = DbtAgent(
            project_dir=self._staged_project_dir,
            model=model,
            db_conn=db_conn,
        )
        self._agent_output = dbt_agent.run(self._thread_meta.get("messages", []))

        self._after_snapshot = _snapshot_tree(self._staged_project_dir)
        added, modified, deleted = _compute_change_lists(self._before_snapshot, self._after_snapshot)

        self.added_files = added
        self.modified_files = modified
        self.deleted_files = deleted

        return self

    def changed_existing_files(self) -> list[Path]:
        return sorted(set(self.modified_files) | set(self.deleted_files))

    def new_or_modified_files(self) -> list[Path]:
        return sorted(set(self.added_files) | set(self.modified_files))

    def commit(self, *, allow_deletes: bool = False) -> dict[str, Any]:
        """
        Copy changes from sandbox back to the source dbt project.

        By default, deletions are NOT applied for safety.
        """
        if self._staged_project_dir is None or self._after_snapshot is None:
            raise DbtPlanNotReadyError("DbtPlan.commit() requires plan.run() to be executed first.")

        staged = self._staged_project_dir
        source = self._source_project_dir

        copied: list[str] = []
        for rel in self.new_or_modified_files():
            src_path = staged / rel
            dst_path = source / rel
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_path, dst_path)
            copied.append(str(rel))

        deleted: list[str] = []
        if allow_deletes:
            for rel in self.deleted_files:
                dst_path = source / rel
                if dst_path.exists():
                    dst_path.unlink()
                deleted.append(str(rel))

        return {
            "copied": copied,
            "deleted": deleted,
            "source_project_dir": str(source),
            "staged_project_dir": str(staged),
        }

    def apply(self, *, timeout_seconds: int | None = None) -> dict[str, Any]:
        """
        Apply the sandboxed dbt project to the warehouse.

        This runs from the sandbox directory to keep the source project clean.
        """
        if not self._dbt_config.allow_apply:
            raise DbtApplyNotAllowedError(
                "DbtPlan.apply() is disabled. Enable it by setting DbtConfig(allow_apply=True)."
            )
        if self._staged_project_dir is None or self._after_snapshot is None:
            raise DbtPlanNotReadyError("DbtPlan.apply() requires plan.run() to be executed first.")

        timeout = timeout_seconds if timeout_seconds is not None else self._dbt_config.dbt_timeout_seconds

        proc = subprocess.run(
            ["dbt", "run"],
            cwd=str(self._staged_project_dir),
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )

        stdout_tail = "\n".join((proc.stdout or "").splitlines()[-200:])
        stderr_tail = "\n".join((proc.stderr or "").splitlines()[-200:])

        return {
            "returncode": proc.returncode,
            "stdout_tail": stdout_tail,
            "stderr_tail": stderr_tail,
            "staged_project_dir": str(self._staged_project_dir),
        }
