from pathlib import Path
from typing import Any

from databao_context_engine import DatasourceType, DbtConfigFile


def create_dbt_config_file(project_dir: Path, name: str) -> DbtConfigFile:
    target_dir = project_dir / "target"
    return DbtConfigFile(name=name, dbt_target_folder_path=target_dir)


def try_extract_dbt_dir_from_content(type: DatasourceType, content: dict[str, Any]) -> Path | None:
    if type != _dbt_type():
        return None
    dbt_file = DbtConfigFile.model_validate({"name": "", **content})
    target_dir = dbt_file.dbt_target_folder_path
    project_dir = target_dir.parent
    return project_dir


def _dbt_type() -> DatasourceType:
    full_type = DbtConfigFile.model_fields["type"].default
    return DatasourceType(full_type=full_type)
