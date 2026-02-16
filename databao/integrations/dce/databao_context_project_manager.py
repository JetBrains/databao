from pathlib import Path
from typing import Any

from databao_context_engine import (
    BuildDatasourceResult,
    ConfiguredDatasource,
    DatabaoContextProjectManager,
    Datasource,
    DatasourceId,
    DatasourceType,
)


class DatabaoContextProjectManagerApi:
    def __init__(self, delegate: DatabaoContextProjectManager):
        self._delegate = delegate

    def create_datasource_config(
        self, datasource_type: DatasourceType, datasource_name: str, config_content: dict[str, Any]
    ) -> ConfiguredDatasource:
        return self._delegate.create_datasource_config(datasource_type, datasource_name, config_content)

    def get_configured_datasource_list(self) -> list[ConfiguredDatasource]:
        return self._delegate.get_configured_datasource_list()

    def get_introspected_datasource_list(self) -> list[Datasource]:
        return self._delegate.get_engine_for_project().get_introspected_datasource_list()

    def build_context(self) -> list[BuildDatasourceResult]:
        return self._delegate.build_context()

    # TODO (dce): should be provided by the DCE side
    @staticmethod
    def _get_datasource_name(datasource_id: DatasourceId) -> str:
        return datasource_id.datasource_path.split("/")[-1]

    @property
    def project_dir(self) -> Path:
        return self._delegate.project_dir
