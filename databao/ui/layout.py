from dataclasses import dataclass
from pathlib import Path

import yaml


# TODO: copypasted from databao-cli, needs to be imported or moved

def get_databao_project_dir(project_dir: Path) -> Path:
    return project_dir / "databao"


@dataclass(frozen=True)
class ProjectLayout:
    project_dir: Path

    @property
    def databao_dir(self) -> Path:
        return get_databao_project_dir(self.project_dir)

    @property
    def agents_dir(self) -> Path:
        return self.databao_dir / "agents"

    @property
    def domains_dir(self) -> Path:
        return self.databao_dir / "domains"

    @property
    def root_domain_dir(self) -> Path:
        """
        Root domain is the domain which is used by default unless domain parameter is specified
        """
        return self.domains_dir / "root"

    @property
    def dbt_config(self) -> Path | None:
        src = self.root_domain_dir / "src"
        if not src.exists() or not src.is_dir():
            raise ValueError("src/ not found in root domain.")
        for file in src.iterdir():
            if file.suffix == ".yaml":
                with open(file) as f:
                    yml = yaml.safe_load(f)
                    if yml.get('type', None) == "dbt":
                        return file
        return None

    @property
    def db_config(self) -> Path | None:
        src = self.root_domain_dir / "src"
        if not src.exists() or not src.is_dir():
            raise ValueError("src/ not found in root domain.")
        for file in src.iterdir():
            if file.suffix == ".yaml":
                with open(file) as f:
                    yml = yaml.safe_load(f)
                    if yml.get('type', None) != "dbt" and yml.get('connection', {}).get('database_path'):
                        return file
        return None


def find_project(initial_dir: Path) -> ProjectLayout | None:
    dirs_to_check = [initial_dir] + list(initial_dir.parents)
    for project_dir_candidate in dirs_to_check:
        databao_project_dir = get_databao_project_dir(project_dir_candidate)
        if databao_project_dir.exists():
            return ProjectLayout(project_dir_candidate)
    return None
