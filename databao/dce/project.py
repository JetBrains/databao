"""DCE project detection and validation."""

from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class DCEProjectStatus(Enum):
    """Status of a DCE project."""

    VALID = "valid"  # Project found with build outputs
    NO_BUILD = "no_build"  # Project found but no output/run
    NOT_FOUND = "not_found"  # No project detected


CONFIG_FILE_NAME = "nemory.ini"
SOURCE_FOLDER_NAME = "src"
OUTPUT_FOLDER_NAME = "output"


@dataclass
class DCEProject:
    """Represents a detected DCE project."""

    path: Path
    status: DCEProjectStatus
    latest_run: str | None = None

    @property
    def name(self) -> str:
        """Get project name from path."""
        return self.path.name

    @property
    def config_file(self) -> Path:
        """Get path to nemory.ini config file."""
        return self.path / CONFIG_FILE_NAME

    @property
    def source_dir(self) -> Path:
        """Get path to src directory."""
        return self.path / SOURCE_FOLDER_NAME

    @property
    def output_dir(self) -> Path:
        """Get path to output directory."""
        return self.path / OUTPUT_FOLDER_NAME

    @property
    def latest_run_dir(self) -> Path | None:
        """Get path to latest run directory."""
        if self.latest_run:
            return self.output_dir / self.latest_run
        return None


def is_dce_project(path: Path) -> bool:
    """Check if a path contains a valid DCE project structure."""
    config_file = path / CONFIG_FILE_NAME
    source_dir = path / SOURCE_FOLDER_NAME
    return config_file.is_file() and source_dir.is_dir()


def get_latest_run(project_path: Path) -> str | None:
    """Get the name of the latest run folder in the project's output directory.

    Runs are named like 'run-2026-01-09T14:27:48Z' and sorted by name (ISO format).
    """
    output_dir = project_path / OUTPUT_FOLDER_NAME
    if not output_dir.is_dir():
        return None

    runs = [d.name for d in output_dir.iterdir() if d.is_dir() and d.name.startswith("run-")]
    if not runs:
        return None

    # Sort by name (ISO format sorts chronologically)
    runs.sort(reverse=True)
    return runs[0]


def validate_project(path: Path) -> DCEProject:
    """Validate a DCE project and return its status."""
    if not is_dce_project(path):
        return DCEProject(path=path, status=DCEProjectStatus.NOT_FOUND)

    latest_run = get_latest_run(path)
    if latest_run is None:
        return DCEProject(path=path, status=DCEProjectStatus.NO_BUILD)

    return DCEProject(path=path, status=DCEProjectStatus.VALID, latest_run=latest_run)


def detect_dce_projects(start_path: Path | None = None, max_parent_levels: int = 3) -> list[DCEProject]:
    """Smart detection of DCE projects.

    Search order:
    1. Current directory
    2. Parent directories (up to max_parent_levels)
    3. Neighboring directories (siblings of start_path)

    Returns list of detected projects sorted by status (VALID first).
    """
    if start_path is None:
        start_path = Path.cwd()

    start_path = start_path.resolve()
    candidates: list[DCEProject] = []
    seen_paths: set[Path] = set()

    def add_candidate(path: Path) -> None:
        """Add a path as candidate if it's a DCE project and not seen before."""
        resolved = path.resolve()
        if resolved in seen_paths:
            return
        seen_paths.add(resolved)

        if is_dce_project(resolved):
            project = validate_project(resolved)
            candidates.append(project)

    # 1. Check current directory
    add_candidate(start_path)

    # 2. Check parent directories
    current = start_path
    for _ in range(max_parent_levels):
        parent = current.parent
        if parent == current:  # Reached root
            break
        add_candidate(parent)
        current = parent

    # 3. Check neighboring directories (siblings of start_path)
    if start_path.parent.is_dir():
        try:
            for sibling in start_path.parent.iterdir():
                if sibling.is_dir() and sibling != start_path:
                    add_candidate(sibling)
        except PermissionError:
            pass  # Skip if no permission to list directory

    # Sort by status: VALID first, then NO_BUILD, then NOT_FOUND
    status_order = {DCEProjectStatus.VALID: 0, DCEProjectStatus.NO_BUILD: 1, DCEProjectStatus.NOT_FOUND: 2}
    candidates.sort(key=lambda p: status_order[p.status])

    return candidates


def find_best_project(start_path: Path | None = None) -> DCEProject | None:
    """Find the best DCE project from detected candidates.

    Returns the first VALID project, or the first NO_BUILD project if no valid ones.
    Returns None if no projects found.
    """
    candidates = detect_dce_projects(start_path)

    # Filter to only valid or no_build projects
    usable = [p for p in candidates if p.status in (DCEProjectStatus.VALID, DCEProjectStatus.NO_BUILD)]

    if not usable:
        return None

    return usable[0]
