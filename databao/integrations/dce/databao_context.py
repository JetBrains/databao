from pathlib import Path

from databao_context_engine import DatabaoContextEngine, DatabaoContextProjectManager, init_dce_project
from databao_context_engine.project.layout import is_project_dir_valid

from databao.integrations.dce.databao_context_engine import DatabaoContextEngineApi
from databao.integrations.dce.databao_context_project_manager import DatabaoContextProjectManagerApi


class DatabaoContextApi:
    @staticmethod
    def init_dce_project(project_dir: Path) -> DatabaoContextProjectManagerApi:
        manager = init_dce_project(project_dir)
        return DatabaoContextProjectManagerApi(manager)

    # TODO (dce): should be implemented on the DCE side
    @staticmethod
    def get_dce_project(project_dir: Path) -> DatabaoContextProjectManagerApi:
        if not is_project_dir_valid(project_dir):
            raise ValueError(f"No DCE project exists in the directory '{project_dir}'")
        manager = DatabaoContextProjectManager(project_dir)
        return DatabaoContextProjectManagerApi(manager)

    # TODO (dce): should be implemented on the DCE side (+ check: do we have a context for DCE?)
    @staticmethod
    def get_dce(project_dir: Path) -> DatabaoContextEngineApi:
        engine = DatabaoContextEngine(project_dir)
        return DatabaoContextEngineApi(engine)
