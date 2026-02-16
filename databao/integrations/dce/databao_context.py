from pathlib import Path

from databao_context_engine import (
    DatabaoContextEngine,
    DatabaoContextProjectManager,
    DatasourceId,
    init_dce_project,
    init_or_get_dce_project,
)

from databao.integrations.dce.databao_context_engine import DatabaoContextEngineApi
from databao.integrations.dce.databao_context_project_manager import DatabaoContextProjectManagerApi


class DatabaoContextApi:
    @staticmethod
    def init_dce_project(project_dir: Path) -> DatabaoContextProjectManagerApi:
        manager = init_dce_project(project_dir)
        return DatabaoContextProjectManagerApi(manager)

    @staticmethod
    def get_dce_project(project_dir: Path) -> DatabaoContextProjectManagerApi:
        manager = DatabaoContextProjectManager(project_dir)
        return DatabaoContextProjectManagerApi(manager)

    @staticmethod
    def init_or_get_dce_project(project_dir: Path) -> DatabaoContextProjectManagerApi:
        manager = init_or_get_dce_project(project_dir)
        return DatabaoContextProjectManagerApi(manager)

    @staticmethod
    def get_dce(project_dir: Path) -> DatabaoContextEngineApi:
        engine = DatabaoContextEngine(project_dir)
        return DatabaoContextEngineApi(engine)

    # TODO (dce): should be provided by the DCE side
    @staticmethod
    def get_datasource_name(datasource_id: DatasourceId) -> str:
        return datasource_id.datasource_path.split("/")[-1]
