from enum import Enum
from functools import cached_property
from pathlib import Path

from databao import Context
from databao.ui.layout import ProjectLayout

DEFAULT_DOMAIN_NAME = "root"

class DCEProjectStatus(Enum):
    """Status of a DCE project."""

    VALID = "valid"  # Project found with build outputs
    NO_BUILD = "no_build"  # Project found but no output/run


class DatabaoProject:
    """Represents a detected DCE project.

    This is a compatibility wrapper around the new DCE integration APIs.
    """

    layout: ProjectLayout
    context: Context

    def __init__(self, path: Path):
        self.layout = ProjectLayout(path)
        self._current_domain_path = self.layout.domains_dir / DEFAULT_DOMAIN_NAME

    @property
    def path(self) -> Path:
        return self.layout.project_dir

    @property
    def databao_dir(self) -> Path:
        return self.layout.databao_dir

    @property
    def name(self) -> str:
        """Get project name from path."""
        return self.databao_dir.name

    @property
    def dce_status(self) -> DCEProjectStatus:
        output_path = Path(self._current_domain_path / "output")
        if output_path.exists() and output_path.is_dir():
            return DCEProjectStatus.VALID
        else:
            return DCEProjectStatus.NO_BUILD

    @cached_property
    def context(self):
        return Context.load(self._current_domain_path)

