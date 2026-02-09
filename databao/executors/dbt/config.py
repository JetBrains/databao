from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal


@dataclass(frozen=True)
class DbtConfig:
    """
    Configuration for optional dbt functionality.

    This is intentionally minimal for now. We'll extend it when implementing planning/validation/apply.
    """

    project_dir: Path
    staging_mode: Literal["temp", "in_project", "custom"] = "temp"
    staging_root: Path | None = None

    allow_apply: bool = True
    dbt_timeout_seconds: int = 300