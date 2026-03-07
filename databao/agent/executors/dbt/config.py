from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DbtConfig:
    """
    Configuration for optional dbt functionality.

    This is intentionally minimal for now. We'll extend it when implementing planning/validation/apply.
    """

    project_dir: Path | None = None
    dbt_timeout_seconds: int = 300
